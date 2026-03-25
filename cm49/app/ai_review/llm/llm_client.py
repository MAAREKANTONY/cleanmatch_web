from __future__ import annotations

import json
import logging
import re
from typing import Any

import requests

from .cost_estimator import CostEstimator
from .guardrails import LLMGuardrails
from .config import describe_provider_runtime, get_llm_api_key, resolve_llm_runtime_config

logger = logging.getLogger(__name__)


class GuardedLLMClient:
    """Safe wrapper around multiple JSON-only LLM providers.

    Supported providers:
    - openai_compatible_json
    - gemini_generate_content
    - anthropic_messages_json

    If the provider is disabled or unavailable, returns a deterministic guarded mock.
    """

    def __init__(self, estimator: CostEstimator, guardrails: LLMGuardrails, config: dict | None = None):
        self.estimator = estimator
        self.guardrails = guardrails
        self.config = resolve_llm_runtime_config(config or {})
        self.enabled = bool(self.config.get('enabled', False))
        self.provider = str(self.config.get('provider', 'openai_compatible_json') or 'openai_compatible_json')
        self.model = str(self.config.get('model') or '')
        self.base_url = str(self.config.get('base_url') or '').strip()
        self.timeout_seconds = int(self.config.get('timeout_seconds', 30) or 30)
        self.max_completion_tokens = int(self.config.get('max_completion_tokens', 300) or 300)
        self.temperature = float(self.config.get('temperature', 0.1) or 0.1)
        self.system_prompt = str(self.config.get('system_prompt', '') or '')
        self.api_key = get_llm_api_key(self.config)
        self.api_key_env = str(self.config.get('api_key_env', 'OPENAI_API_KEY') or 'OPENAI_API_KEY')
        self.anthropic_version = str(self.config.get('anthropic_version', '2023-06-01') or '2023-06-01')

    def live_call_diagnostics(self) -> dict[str, Any]:
        provider_snapshot = describe_provider_runtime(self.provider)
        missing: list[str] = []
        if not self.enabled:
            missing.append('llm_disabled')
        if not self.provider:
            missing.append('provider_missing')
        if not provider_snapshot.get('provider_exists'):
            missing.append('provider_not_found_in_catalog')
        if not self.model:
            missing.append('model_missing')
        expected_base_url = str(provider_snapshot.get('base_url') or '').strip()
        expected_api_key_env = str(provider_snapshot.get('api_key_env') or '').strip()
        if not self.api_key:
            missing.append('api_key_missing')
        if not self.base_url:
            missing.append('base_url_missing')
        if expected_base_url and self.base_url and expected_base_url != self.base_url:
            missing.append('base_url_provider_mismatch')
        if expected_api_key_env and self.api_key_env and expected_api_key_env != self.api_key_env:
            missing.append('api_key_env_provider_mismatch')
        if missing:
            fallback_reason = 'mock_llm_not_enabled:' + ','.join(missing)
        else:
            fallback_reason = 'provider_ready'
        return {
            'enabled': bool(self.enabled),
            'provider': self.provider,
            'model': self.model,
            'base_url': self.base_url,
            'api_key_env': self.api_key_env,
            'api_key_present': bool(self.api_key),
            'api_key_length': len(self.api_key or ''),
            'expected_base_url': str(provider_snapshot.get('base_url') or ''),
            'expected_api_key_env': str(provider_snapshot.get('api_key_env') or ''),
            'provider_catalog': provider_snapshot,
            'missing_requirements': missing,
            'fallback_reason': fallback_reason,
            'live_call_ready': not missing,
            'timeout_seconds': self.timeout_seconds,
            'max_completion_tokens': self.max_completion_tokens,
            'temperature': self.temperature,
        }

    def call(self, row_key: str, capability: str, context: dict[str, Any], prompt_text: str) -> dict[str, Any]:
        estimated_cost, prompt_tokens, completion_tokens = self.estimator.estimate(prompt_text, self.max_completion_tokens)
        decision = self.guardrails.check(row_key=row_key, capability=capability, context=context, estimated_cost_eur=estimated_cost, provider=self.provider, model=self.model, prompt_text=prompt_text)
        if not decision.allowed:
            if decision.reason == 'cache_hit' and decision.cached_response is not None:
                cached = dict(decision.cached_response)
                if cached.get('status') == 'guardrails_ready':
                    cached['status'] = 'fallback_mock'
                cached.setdefault('status', 'cache_hit')
                cached.setdefault('cache_hit', True)
                cached.setdefault('estimated_cost_eur', 0.0)
                cached.setdefault('actual_cost_eur', 0.0)
                cached.setdefault('result_source', 'cache')
                cached.setdefault('llm_configured', bool(self.enabled))
                cached.setdefault('llm_live_call_ready', bool(self.live_call_diagnostics().get('live_call_ready', False)))
                cached.setdefault('live_call_attempted', False)
                return cached
            return {
                'status': 'skipped',
                'reason': decision.reason,
                'cache_hit': False,
                'estimated_cost_eur': round(estimated_cost, 6),
                'actual_cost_eur': 0.0,
                'prompt_tokens': prompt_tokens,
                'completion_tokens': completion_tokens,
                'provider': self.provider,
                'model': self.model,
                'result_source': 'none',
                'llm_configured': bool(self.enabled),
                'llm_live_call_ready': False,
                'live_call_attempted': False,
            }

        diagnostics = self.live_call_diagnostics()
        live_call = diagnostics.get('live_call_ready', False)
        logger.warning('LLM runtime diagnostics: %s', json.dumps(diagnostics, ensure_ascii=False))
        if live_call:
            payload = self._call_live_provider(prompt_text, capability, estimated_cost, prompt_tokens, completion_tokens)
            payload.setdefault('result_source', 'live')
            payload.setdefault('llm_configured', bool(self.enabled))
            payload.setdefault('llm_live_call_ready', True)
            payload.setdefault('live_call_attempted', True)
        else:
            payload = self._mock_payload(capability, context, estimated_cost, prompt_tokens, completion_tokens, diagnostics=diagnostics)
            payload.setdefault('result_source', 'mock')
            payload.setdefault('llm_configured', bool(self.enabled))
            payload.setdefault('llm_live_call_ready', False)
            payload.setdefault('live_call_attempted', False)

        actual_cost = float(payload.get('actual_cost_eur', estimated_cost) or 0.0)
        if payload.get('status') == 'live_success' and live_call:
            self.guardrails.budget_manager.register_cost(row_key=row_key, cost_eur=actual_cost)
        cache_payload = dict(payload)
        try:
            cache_payload['result_json'] = json.loads(json.dumps(payload.get('result_json', {}), ensure_ascii=False))
        except Exception:
            cache_payload['result_json'] = {}
        self.guardrails.cache_manager.set(capability, context, self.guardrails.prompt_version, cache_payload, provider=self.provider, model=self.model, prompt_text=prompt_text)
        return payload

    def _call_live_provider(self, prompt_text: str, capability: str, estimated_cost: float, prompt_tokens: int, completion_tokens: int) -> dict[str, Any]:
        try:
            if self.provider == 'gemini_generate_content':
                return self._call_gemini(prompt_text, estimated_cost, prompt_tokens, completion_tokens)
            if self.provider == 'anthropic_messages_json':
                return self._call_anthropic(prompt_text, estimated_cost, prompt_tokens, completion_tokens)
            return self._call_openai_compatible(prompt_text, estimated_cost, prompt_tokens, completion_tokens)
        except Exception as exc:
            fallback = self._mock_payload(capability, {'website_meta_description': prompt_text}, estimated_cost, prompt_tokens, completion_tokens)
            fallback.update({
                'status': 'fallback_after_error',
                'reason': f'provider_error:{exc.__class__.__name__}',
                'provider': self.provider,
                'model': self.model,
            })
            return fallback

    def _call_openai_compatible(self, prompt_text: str, estimated_cost: float, prompt_tokens: int, completion_tokens: int) -> dict[str, Any]:
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
        }
        body = {
            'model': self.model,
            'temperature': self.temperature,
            'max_tokens': self.max_completion_tokens,
            'response_format': {'type': 'json_object'},
            'messages': [
                {'role': 'system', 'content': self.system_prompt},
                {'role': 'user', 'content': prompt_text},
            ],
        }
        resp = requests.post(self.base_url, headers=headers, json=body, timeout=self.timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        content = (((data.get('choices') or [{}])[0].get('message') or {}).get('content') or '').strip()
        parsed = self._safe_json(content)
        parse_ok = 'invalid_json_response' not in list(parsed.get('evidence') or [])
        usage = data.get('usage') or {}
        total_prompt_tokens = int(usage.get('prompt_tokens') or prompt_tokens)
        total_completion_tokens = int(usage.get('completion_tokens') or completion_tokens)
        actual_cost, _, _ = self.estimator.estimate_from_token_counts(total_prompt_tokens, total_completion_tokens)
        return {
            'status': 'live_success' if parse_ok else 'live_invalid_json',
            'reason': 'provider_success' if parse_ok else 'provider_success_invalid_json',
            'cache_hit': False,
            'estimated_cost_eur': round(estimated_cost, 6),
            'actual_cost_eur': round(actual_cost, 6),
            'prompt_tokens': total_prompt_tokens,
            'completion_tokens': total_completion_tokens,
            'provider': self.provider,
            'model': self.model,
            'result_json': parsed,
            'raw_content': content[:8000],
            'json_parse_success': parse_ok,
        }

    def _call_gemini(self, prompt_text: str, estimated_cost: float, prompt_tokens: int, completion_tokens: int) -> dict[str, Any]:
        endpoint = f"{self.base_url.rstrip('/')}/models/{self.model}:generateContent"
        headers = {
            'x-goog-api-key': self.api_key,
            'Content-Type': 'application/json',
        }
        body = {
            'system_instruction': {'parts': [{'text': self.system_prompt}]},
            'contents': [
                {
                    'role': 'user',
                    'parts': [{'text': prompt_text}],
                }
            ],
            'generationConfig': {
                'temperature': self.temperature,
                'maxOutputTokens': self.max_completion_tokens,
                'responseMimeType': 'application/json',
            },
        }
        resp = requests.post(endpoint, headers=headers, json=body, timeout=self.timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        candidates = data.get('candidates') or []
        parts = (((candidates[0] if candidates else {}).get('content') or {}).get('parts') or [])
        content = ''
        for part in parts:
            text = (part or {}).get('text')
            if text:
                content += str(text)
        parsed = self._safe_json(content.strip())
        parse_ok = 'invalid_json_response' not in list(parsed.get('evidence') or [])
        usage = data.get('usageMetadata') or {}
        total_prompt_tokens = int(usage.get('promptTokenCount') or prompt_tokens)
        total_completion_tokens = int(usage.get('candidatesTokenCount') or completion_tokens)
        actual_cost, _, _ = self.estimator.estimate_from_token_counts(total_prompt_tokens, total_completion_tokens)
        return {
            'status': 'live_success' if parse_ok else 'live_invalid_json',
            'reason': 'provider_success' if parse_ok else 'provider_success_invalid_json',
            'cache_hit': False,
            'estimated_cost_eur': round(estimated_cost, 6),
            'actual_cost_eur': round(actual_cost, 6),
            'prompt_tokens': total_prompt_tokens,
            'completion_tokens': total_completion_tokens,
            'provider': self.provider,
            'model': self.model,
            'result_json': parsed,
            'raw_content': content[:8000],
            'json_parse_success': parse_ok,
        }

    def _call_anthropic(self, prompt_text: str, estimated_cost: float, prompt_tokens: int, completion_tokens: int) -> dict[str, Any]:
        headers = {
            'x-api-key': self.api_key,
            'anthropic-version': self.anthropic_version,
            'content-type': 'application/json',
        }
        body = {
            'model': self.model,
            'system': self.system_prompt,
            'max_tokens': self.max_completion_tokens,
            'temperature': self.temperature,
            'messages': [
                {'role': 'user', 'content': prompt_text},
            ],
        }
        resp = requests.post(self.base_url, headers=headers, json=body, timeout=self.timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        content_blocks = data.get('content') or []
        text_parts = []
        for block in content_blocks:
            if (block or {}).get('type') == 'text' and (block or {}).get('text'):
                text_parts.append(str(block.get('text')))
        content = ''.join(text_parts).strip()
        parsed = self._safe_json(content)
        parse_ok = 'invalid_json_response' not in list(parsed.get('evidence') or [])
        usage = data.get('usage') or {}
        total_prompt_tokens = int(usage.get('input_tokens') or prompt_tokens)
        total_completion_tokens = int(usage.get('output_tokens') or completion_tokens)
        actual_cost, _, _ = self.estimator.estimate_from_token_counts(total_prompt_tokens, total_completion_tokens)
        return {
            'status': 'live_success' if parse_ok else 'live_invalid_json',
            'reason': 'provider_success' if parse_ok else 'provider_success_invalid_json',
            'cache_hit': False,
            'estimated_cost_eur': round(estimated_cost, 6),
            'actual_cost_eur': round(actual_cost, 6),
            'prompt_tokens': total_prompt_tokens,
            'completion_tokens': total_completion_tokens,
            'provider': self.provider,
            'model': self.model,
            'result_json': parsed,
            'raw_content': content[:8000],
            'json_parse_success': parse_ok,
        }

    def _mock_payload(self, capability: str, context: dict[str, Any], estimated_cost: float, prompt_tokens: int, completion_tokens: int, diagnostics: dict[str, Any] | None = None) -> dict[str, Any]:
        diagnostics = diagnostics or self.live_call_diagnostics()
        missing_items = diagnostics.get('missing_requirements', []) or []
        fallback_reason = diagnostics.get('fallback_reason', 'mock_llm_not_enabled')
        result = {
            'service_mode': self._guess_service_mode(context),
            'cuisine_type': self._guess_cuisine(context),
            'segment_type0': 'horeca',
            'segment_type1': 'fast_food' if self._guess_service_mode(context) == 'fast_food' else 'table_service',
            'segment_type2': '',
            'segment_type3': '',
            'confidence': 0.55,
            'evidence': ['llm_fallback_mock', fallback_reason],
            'requires_human_review': True,
            'reasoning_short': self._build_mock_reasoning_short(diagnostics),
            'llm_debug': diagnostics,
        }
        return {
            'status': 'fallback_mock',
            'reason': fallback_reason,
            'cache_hit': False,
            'estimated_cost_eur': round(estimated_cost, 6),
            'actual_cost_eur': 0.0,
            'prompt_tokens': prompt_tokens,
            'completion_tokens': completion_tokens,
            'provider': self.provider,
            'model': self.model,
            'llm_debug': diagnostics,
            'result_json': result,
            'raw_content': json.dumps(result, ensure_ascii=False),
            'result_source': 'mock',
            'llm_configured': bool(diagnostics.get('enabled', False)),
            'llm_live_call_ready': bool(diagnostics.get('live_call_ready', False)),
            'live_call_attempted': False,
        }


    @staticmethod
    def _build_mock_reasoning_short(diagnostics: dict[str, Any]) -> str:
        fallback_reason = str(diagnostics.get('fallback_reason') or 'row_live_call_not_ready')
        missing_items = diagnostics.get('missing_requirements', []) or []
        missing_text = ', '.join(missing_items) if missing_items else 'none'
        if diagnostics.get('enabled'):
            return (
                'LLM provider was configured for the job, but this row used deterministic fallback because '
                f'a live row-level LLM call was not ready ({fallback_reason}). Missing or mismatched: {missing_text}.'
            )
        return (
            'This row used deterministic fallback because live LLM was disabled or not fully configured '
            f'for the job ({fallback_reason}). Missing or mismatched: {missing_text}.'
        )

    @classmethod
    def _safe_json(cls, content: str) -> dict[str, Any]:
        normalized = cls._normalize_json_candidate(content)
        for candidate in [normalized, cls._extract_first_json_object(normalized)]:
            if not candidate:
                continue
            try:
                data = json.loads(candidate)
                if isinstance(data, dict):
                    return data
            except Exception:
                continue

        recovered = cls._recover_partial_json_fields(normalized)
        if recovered:
            return recovered

        return {
            'service_mode': '',
            'cuisine_type': '',
            'segment_type0': '',
            'segment_type1': '',
            'segment_type2': '',
            'segment_type3': '',
            'confidence': 0.0,
            'evidence': ['invalid_json_response'],
            'requires_human_review': True,
            'reasoning_short': 'Provider response was not valid JSON.',
        }


    @classmethod
    def _recover_partial_json_fields(cls, content: str) -> dict[str, Any]:
        text = cls._normalize_json_candidate(content)
        if not text or '{' not in text:
            return {}

        recovered: dict[str, Any] = {
            'service_mode': '',
            'cuisine_type': '',
            'segment_type0': '',
            'segment_type1': '',
            'segment_type2': '',
            'segment_type3': '',
            'confidence': 0.0,
            'evidence': [],
            'requires_human_review': True,
            'reasoning_short': '',
        }

        def parse_string_value(key: str) -> str | None:
            null_match = re.search(rf'"{re.escape(key)}"\s*:\s*null', text)
            if null_match:
                return ''
            quoted_match = re.search(rf'"{re.escape(key)}"\s*:\s*"((?:\\.|[^"\\])*)"', text, flags=re.DOTALL)
            if quoted_match:
                try:
                    return json.loads('"' + quoted_match.group(1) + '"')
                except Exception:
                    return quoted_match.group(1)
            if key == 'reasoning_short':
                start_match = re.search(rf'"{re.escape(key)}"\s*:\s*"', text)
                if start_match:
                    tail = text[start_match.end():]
                    tail = re.sub(r'```+$', '', tail).strip()
                    return cls._truncate_partial_text(tail)
            if key == 'evidence':
                start_match = re.search(rf'"{re.escape(key)}"\s*:\s*"', text)
                if start_match:
                    tail = text[start_match.end():]
                    next_key = re.search(r'",\s*"[A-Za-z0-9_]+"\s*:', tail, flags=re.DOTALL)
                    candidate = tail[:next_key.start()] if next_key else tail
                    return cls._truncate_partial_text(candidate)
            return None

        for key in ['service_mode', 'cuisine_type', 'segment_type0', 'segment_type1', 'segment_type2', 'segment_type3']:
            value = parse_string_value(key)
            if value is not None:
                recovered[key] = value

        confidence_match = re.search(r'"confidence"\s*:\s*(-?\d+(?:\.\d+)?)', text)
        if confidence_match:
            try:
                recovered['confidence'] = float(confidence_match.group(1))
            except Exception:
                pass

        review_match = re.search(r'"requires_human_review"\s*:\s*(true|false)', text, flags=re.IGNORECASE)
        if review_match:
            recovered['requires_human_review'] = review_match.group(1).lower() == 'true'

        evidence_array_match = re.search(r'"evidence"\s*:\s*(\[[\s\S]*?\])', text)
        if evidence_array_match:
            try:
                recovered['evidence'] = json.loads(evidence_array_match.group(1))
            except Exception:
                pass
        elif not recovered['evidence']:
            evidence_text = parse_string_value('evidence')
            if evidence_text:
                recovered['evidence'] = [evidence_text]

        reasoning = parse_string_value('reasoning_short')
        if reasoning:
            recovered['reasoning_short'] = reasoning

        structured_keys = [recovered.get('service_mode'), recovered.get('cuisine_type'), recovered.get('segment_type0'), recovered.get('segment_type1'), recovered.get('segment_type2'), recovered.get('segment_type3')]
        if not any(str(v).strip() for v in structured_keys):
            return {}

        evidence_list = recovered.get('evidence') or []
        if isinstance(evidence_list, list):
            evidence_list = [str(item).strip() for item in evidence_list if str(item).strip()]
        else:
            evidence_list = [str(evidence_list).strip()] if str(evidence_list).strip() else []
        evidence_list.append('partial_json_recovered')
        if cls._looks_truncated(text):
            evidence_list.append('provider_response_truncated')
        recovered['evidence'] = evidence_list
        if not recovered.get('reasoning_short'):
            recovered['reasoning_short'] = 'Partial structured JSON was recovered from a truncated provider response.'
        return recovered

    @staticmethod
    def _truncate_partial_text(value: str, max_len: int = 280) -> str:
        text = str(value or '').replace('\n', ' ').replace('\r', ' ').strip(' \"`')
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) > max_len:
            text = text[:max_len].rstrip()
        return text

    @staticmethod
    def _looks_truncated(text: str) -> bool:
        stripped = str(text or '').strip()
        if not stripped:
            return False
        if stripped.endswith('}') or stripped.endswith(']'):
            return False
        open_braces = stripped.count('{') + stripped.count('[')
        close_braces = stripped.count('}') + stripped.count(']')
        return open_braces > close_braces or stripped.endswith(':') or stripped.endswith(',')

    @staticmethod
    def _normalize_json_candidate(content: str) -> str:
        text = str(content or '').strip()
        if not text:
            return ''
        text = re.sub(r'^\s*```(?:json|JSON)?\s*', '', text)
        text = re.sub(r'\s*```\s*$', '', text)
        return text.strip()

    @classmethod
    def _extract_first_json_object(cls, content: str) -> str:
        text = cls._normalize_json_candidate(content)
        if not text:
            return ''
        decoder = json.JSONDecoder()
        for match in re.finditer(r'[\{\[]', text):
            start = match.start()
            try:
                _parsed, end = decoder.raw_decode(text[start:])
                return text[start:start + end]
            except Exception:
                continue
        return ''

    @staticmethod
    def _guess_service_mode(context: dict[str, Any]) -> str:
        hay = ' '.join(str(context.get(k, '')) for k in ['main_type', 'all_types', 'website_meta_description', 'web_text'])
        hay = hay.lower()
        if any(token in hay for token in ['fast food', 'takeaway', 'delivery', 'hamburger', 'pizza delivery', 'counter service', 'drive thru']):
            return 'fast_food'
        if any(token in hay for token in ['bar', 'pub', 'cocktail', 'wine bar']):
            return 'bar'
        return 'table_service'

    @staticmethod
    def _guess_cuisine(context: dict[str, Any]) -> str:
        hay = ' '.join(str(context.get(k, '')) for k in ['main_type', 'all_types', 'website_meta_description', 'web_text'])
        hay = hay.lower()
        if 'pizza' in hay:
            return 'pizza'
        if 'burger' in hay or 'hamburger' in hay:
            return 'burger'
        if 'sushi' in hay or 'ramen' in hay:
            return 'japanese'
        if 'kebab' in hay or 'shawarma' in hay:
            return 'kebab'
        return 'generic'

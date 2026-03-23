from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from core.config_loader import load_yaml_config
from matcher.services.matcher_service import inspect_table_file

from .ai_review_models import AIReviewInput, AIReviewResult
from .ai_review_utils import csv_safe, estimate_total_rows, iter_rows, normalized_text
from .canonical_mapping_service import (
    AI_REVIEW_COLUMN_ALIASES,
    CANONICAL_FIELD_SPECS,
    CANONICAL_MULTI_FIELDS,
    CANONICAL_REQUIRED_FIELDS,
    CanonicalMappingService,
)
from .capability_engine import CapabilityEngine, AI_REVIEW_ACTION_PROFILES, AI_REVIEW_DEFAULT_ACTION_PROFILE
from .web_cache import FileWebCache
from .web_fetcher import candidate_urls, extract_meta_description, extract_title, fetch_html, host_label, strip_html_to_text
from ai_review.llm import (
    BudgetManager, CostEstimator, GuardedLLMClient, LLMCacheManager, LLMGuardrails,
    load_llm_guardrails_config, load_llm_providers_config, get_provider_choices,
    get_model_choices_grouped, get_default_model_for_provider, model_belongs_to_provider,
    resolve_llm_runtime_config,
)

ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]

logger = logging.getLogger(__name__)

_AI_THRESHOLDS = load_yaml_config('ai_review/thresholds.yaml')
_AI_BUDGET = load_yaml_config('ai_review/budget.yaml')
_AI_FEATURES = load_yaml_config('ai_review/feature_extraction.yaml')
_AI_LLM_GUARDRAILS = load_llm_guardrails_config()
_AI_LLM_PROVIDERS = load_llm_providers_config()

AI_REVIEW_MAPPING_FIELDS = list(CANONICAL_FIELD_SPECS.keys())
AI_REVIEW_REQUIRED_FIELDS = set(CANONICAL_REQUIRED_FIELDS)
AI_REVIEW_OUTPUT_FIELDS = [
    'ai_review_status', 'ai_confidence', 'ai_segment_suggested', 'ai_sources_used', 'ai_evidence_summary',
    'ai_requires_human_review', 'ai_input_pack_summary', 'ai_selected_for_review', 'ai_detected_service_mode',
    'ai_detected_cuisine', 'ai_detected_keywords', 'ai_detected_signals_json', 'ai_source_count',
    'ai_web_fetch_status', 'ai_sources_fetched', 'ai_web_text_content', 'ai_menu_text_excerpt',
    'ai_homepage_title', 'ai_homepage_meta_description', 'ai_action_profile', 'ai_enabled_capabilities',
    'ai_capability_field_usage', 'ai_llm_status', 'ai_llm_reason', 'ai_llm_configured',
    'ai_llm_live_ready', 'ai_llm_attempted', 'ai_llm_result_source', 'ai_llm_cost_estimated_eur',
    'ai_llm_cost_actual_eur', 'ai_llm_budget_remaining_eur', 'ai_llm_cache_hit', 'ai_llm_calls_used',
    'ai_llm_provider', 'ai_llm_model', 'ai_llm_result_json', 'ai_llm_raw_excerpt',
]
AI_THRESHOLD_LOW_CONFIDENCE = float(_AI_THRESHOLDS.get('low_confidence_threshold', 0.65))
AI_SKIP_IF_CONFIDENCE_ABOVE = float(_AI_THRESHOLDS.get('skip_if_confidence_above', 0.80))
AI_MAX_SOCIAL_LINKS = int(_AI_BUDGET.get('max_social_links', 3))
AI_MAX_PHOTO_LINKS = int(_AI_BUDGET.get('max_photo_links', 3))
AI_MAX_PAGES_PER_OUTLET = int(_AI_BUDGET.get('max_pages_per_outlet', 2))
AI_WEB_FETCH_ENABLED = bool(_AI_BUDGET.get('web_fetch_enabled', True))
AI_FETCH_TIMEOUT_SECONDS = int(_AI_BUDGET.get('timeout_seconds', 5))
AI_FETCH_MAX_TEXT_CHARS = int(_AI_BUDGET.get('max_text_chars_per_page', 3000))
AI_FETCH_MAX_DOWNLOAD_BYTES = int(_AI_BUDGET.get('max_download_bytes_per_page', 300000))
AI_FETCH_USER_AGENT = str(_AI_BUDGET.get('fetch_user_agent', 'cleanmatch-web-ai-review'))
AI_CACHE_SUBDIR = str(_AI_BUDGET.get('cache_subdir', 'ai_review_web_cache'))
AI_MAX_KEYWORDS = int(_AI_FEATURES.get('max_keywords_in_output', 18))
SERVICE_MODE_KEYWORDS = _AI_FEATURES.get('service_mode_keywords', {})
CUISINE_KEYWORDS = _AI_FEATURES.get('cuisine_keywords', {})
SIGNAL_KEYWORDS = _AI_FEATURES.get('signal_keywords', {})

AI_LLM_ENABLED = bool(_AI_LLM_GUARDRAILS.get('enabled', False))
AI_LLM_PROVIDER = str(_AI_LLM_GUARDRAILS.get('provider', 'openai_compatible_json'))
AI_LLM_PROVIDER_CHOICES = get_provider_choices()
AI_LLM_MODEL_CHOICES_GROUPED = get_model_choices_grouped()
AI_LLM_MODEL = str(_AI_LLM_GUARDRAILS.get('model', get_default_model_for_provider(AI_LLM_PROVIDER)))
AI_LLM_PROMPT_VERSION = str(_AI_LLM_GUARDRAILS.get('prompt_version', 'sprint30-v1'))
AI_LLM_MAX_BUDGET_EUR = float(_AI_LLM_GUARDRAILS.get('max_budget_eur', 20.0))
AI_LLM_MAX_COST_PER_ROW_EUR = float(_AI_LLM_GUARDRAILS.get('max_cost_per_row_eur', 0.01))
AI_LLM_MAX_CALLS_PER_ROW = int(_AI_LLM_GUARDRAILS.get('max_calls_per_row', 2))
AI_LLM_INPUT_PRICE_PER_1K_EUR = float(_AI_LLM_GUARDRAILS.get('input_price_per_1k_tokens_eur', 0.002))
AI_LLM_OUTPUT_PRICE_PER_1K_EUR = float(_AI_LLM_GUARDRAILS.get('output_price_per_1k_tokens_eur', 0.006))
AI_LLM_MOCK_COMPLETION_TOKENS = int(_AI_LLM_GUARDRAILS.get('mock_completion_tokens', 180))
AI_LLM_CACHE_SUBDIR = str(_AI_LLM_GUARDRAILS.get('cache_subdir', 'ai_review_llm_cache'))
AI_LLM_CAPABILITIES = set(_AI_LLM_GUARDRAILS.get('llm_capabilities', ['metadata_review', 'homepage_fetch', 'menu_fetch']))


def _noop_progress(percent: int, message: str) -> None:
    return None


def _noop_log(message: str) -> None:
    return None


@dataclass
class AIReviewOptions:
    ai_review_sheet_name: str | None = None
    ai_review_mapping: dict[str, str] = field(default_factory=dict)
    low_confidence_threshold: float | None = None
    only_low_confidence: bool = True
    action_profile: str = AI_REVIEW_DEFAULT_ACTION_PROFILE
    llm_enabled: bool | None = None
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_max_budget_eur: float | None = None
    llm_max_cost_per_row_eur: float | None = None
    llm_max_calls_per_row: int | None = None


class AIReviewService:
    def __init__(self, progress_callback: ProgressCallback | None = None, log_callback: LogCallback | None = None):
        self.progress_callback = progress_callback or _noop_progress
        self.log_callback = log_callback or _noop_log
        self.web_cache: FileWebCache | None = None
        self.llm_cache: LLMCacheManager | None = None
        self.budget_manager: BudgetManager | None = None
        self.cost_estimator: CostEstimator | None = None
        self.llm_client: GuardedLLMClient | None = None
        self._web_fetch_stats = {'pages_attempted': 0, 'pages_ok': 0, 'cache_hits': 0, 'outlets_with_fetch': 0}
        self._llm_stats = {'calls_attempted': 0, 'calls_executed': 0, 'cache_hits': 0, 'budget_skips': 0, 'row_budget_skips': 0, 'max_call_skips': 0}
        self.mapping_service: CanonicalMappingService | None = None

    def progress(self, percent: int, message: str) -> None:
        self.progress_callback(percent, message)

    def log(self, message: str) -> None:
        self.log_callback(message)

    def run(self, input_path: Path, output_path: Path, options: AIReviewOptions) -> Path:
        threshold = options.low_confidence_threshold if options.low_confidence_threshold is not None else AI_THRESHOLD_LOW_CONFIDENCE
        output_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_llm_enabled = AI_LLM_ENABLED if options.llm_enabled is None else bool(options.llm_enabled)
        runtime_llm_provider = str(options.llm_provider or AI_LLM_PROVIDER or 'openai_compatible_json')
        runtime_llm_model = str(options.llm_model or _AI_LLM_GUARDRAILS.get('model') or get_default_model_for_provider(runtime_llm_provider))
        runtime_llm_max_budget_eur = float(options.llm_max_budget_eur if options.llm_max_budget_eur is not None else AI_LLM_MAX_BUDGET_EUR)
        runtime_llm_max_cost_per_row_eur = float(options.llm_max_cost_per_row_eur if options.llm_max_cost_per_row_eur is not None else AI_LLM_MAX_COST_PER_ROW_EUR)
        runtime_llm_max_calls_per_row = int(options.llm_max_calls_per_row if options.llm_max_calls_per_row is not None else AI_LLM_MAX_CALLS_PER_ROW)
        self.web_cache = FileWebCache(output_path.parent / AI_CACHE_SUBDIR)
        self.llm_cache = LLMCacheManager(output_path.parent / AI_LLM_CACHE_SUBDIR)
        self.budget_manager = BudgetManager(runtime_llm_max_budget_eur, runtime_llm_max_cost_per_row_eur, runtime_llm_max_calls_per_row)
        self.cost_estimator = CostEstimator(AI_LLM_INPUT_PRICE_PER_1K_EUR, AI_LLM_OUTPUT_PRICE_PER_1K_EUR)
        llm_runtime_config = resolve_llm_runtime_config({
            **dict(_AI_LLM_GUARDRAILS),
            'enabled': runtime_llm_enabled,
            'provider': runtime_llm_provider,
            'model': runtime_llm_model,
            'max_budget_eur': runtime_llm_max_budget_eur,
            'max_cost_per_row_eur': runtime_llm_max_cost_per_row_eur,
            'max_calls_per_row': runtime_llm_max_calls_per_row,
        })
        self.llm_client = GuardedLLMClient(
            estimator=self.cost_estimator,
            guardrails=LLMGuardrails(self.budget_manager, self.llm_cache, AI_LLM_PROMPT_VERSION),
            config=llm_runtime_config,
        )
        self._web_fetch_stats = {'pages_attempted': 0, 'pages_ok': 0, 'cache_hits': 0, 'outlets_with_fetch': 0}
        self._llm_stats = {'calls_attempted': 0, 'calls_executed': 0, 'cache_hits': 0, 'budget_skips': 0, 'row_budget_skips': 0, 'max_call_skips': 0}
        self.mapping_service = CanonicalMappingService(options.ai_review_mapping)
        total = estimate_total_rows(input_path, options.ai_review_sheet_name) or 0
        processed = selected = skipped = extracted = 0
        source_headers: list[str] | None = None
        canonical_headers: list[str] | None = None

        self.progress(5, 'Préparation du process AI review hardened')
        self.log(f'[AI_REVIEW] Threshold bas confiance actif: {threshold:.2f} | profile={options.action_profile}')
        self.log(f'[AI_REVIEW] Guardrails LLM actifs: provider={runtime_llm_provider} model={runtime_llm_model} enabled={runtime_llm_enabled} budget={runtime_llm_max_budget_eur:.2f}€ row_max={runtime_llm_max_cost_per_row_eur:.4f}€ calls/row={runtime_llm_max_calls_per_row}')
        runtime_diag = self.llm_client.live_call_diagnostics()
        runtime_diag_json = json.dumps(runtime_diag, ensure_ascii=False)
        self.log(f'[AI_REVIEW] LLM runtime diagnostics: {runtime_diag_json}')
        logger.warning('[AI_REVIEW] LLM runtime diagnostics: %s', runtime_diag_json)

        with output_path.open('w', newline='', encoding='utf-8-sig') as fh:
            writer = None
            for raw_headers, raw_row in iter_rows(input_path, options.ai_review_sheet_name):
                if source_headers is None:
                    source_headers = list(raw_headers)
                    canonical_headers = self._prepare_headers(source_headers)
                    output_columns = list(canonical_headers) + [field for field in AI_REVIEW_OUTPUT_FIELDS if field not in canonical_headers]
                    writer = csv.DictWriter(fh, fieldnames=output_columns, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
                    writer.writeheader()

                mapped = self._map_row(raw_row, source_headers or [], canonical_headers or [])
                if not any(str(v).strip() for v in mapped.values()):
                    continue
                review_input = self._build_review_input(mapped, options.action_profile)
                result = self._build_feature_extraction_result(review_input, threshold, options.only_low_confidence)
                out = {col: csv_safe(mapped.get(col, '')) for col in (canonical_headers or [])}
                out.update({field: csv_safe(getattr(result, field, '')) for field in AI_REVIEW_OUTPUT_FIELDS})
                writer.writerow(out)
                processed += 1
                selected += 1 if result.ai_selected_for_review == 'yes' else 0
                skipped += 1 if result.ai_selected_for_review != 'yes' else 0
                extracted += 1 if result.ai_detected_service_mode or result.ai_detected_cuisine or result.ai_detected_keywords else 0
                if processed == 1 or processed % 500 == 0 or (total and processed == total):
                    self.progress(min(96, 12 + int((processed / max(total, processed)) * 84)), f'AI review hardening en cours : {processed}/{total or "?"}')

        summary = {
            'rows': int(processed),
            'selected_for_ai_review': int(selected),
            'skipped_high_confidence': int(skipped),
            'feature_extraction_rows': int(extracted),
            'low_confidence_threshold': threshold,
            'only_low_confidence': bool(options.only_low_confidence),
            'foundation_mode': False,
            'feature_extraction_enabled': True,
            'web_fetch_enabled': AI_WEB_FETCH_ENABLED,
            'llm_enabled': bool(getattr(self.llm_client, 'enabled', AI_LLM_ENABLED)),
            'llm_provider': str(getattr(self.llm_client, 'provider', AI_LLM_PROVIDER)),
            'llm_model': str(getattr(self.llm_client, 'model', AI_LLM_MODEL)),
            'llm_guardrails': {
                'prompt_version': AI_LLM_PROMPT_VERSION,
                'max_budget_eur': runtime_llm_max_budget_eur,
                'max_cost_per_row_eur': runtime_llm_max_cost_per_row_eur,
                'max_calls_per_row': runtime_llm_max_calls_per_row,
            },
            'web_fetch_stats': self._web_fetch_stats,
            'llm_stats': self._llm_stats,
            'llm_spend_eur': round(self.budget_manager.current_spend_eur if self.budget_manager else 0.0, 6),
            'llm_budget_remaining_eur': round(self.budget_manager.remaining_budget() if self.budget_manager else 0.0, 6),
            'action_profile': options.action_profile,
            'available_action_profiles': sorted(AI_REVIEW_ACTION_PROFILES.keys()),
            'canonical_mapping_fields': AI_REVIEW_MAPPING_FIELDS,
        }
        output_path.with_name(output_path.stem + '_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        self.log(f"[AI_REVIEW] {selected} / {processed} lignes envoyées en review, extraction locale sur {extracted} ligne(s), web_fetch_ok={self._web_fetch_stats['pages_ok']}, llm_calls={self._llm_stats['calls_executed']}, llm_spend={self.budget_manager.current_spend_eur if self.budget_manager else 0.0:.4f}€")
        self.progress(100, 'AI review hardening terminé')
        return output_path

    def _prepare_headers(self, source_headers: list[str]) -> list[str]:
        output_headers = [str(col) for col in source_headers]
        existing = set(output_headers)
        for field in AI_REVIEW_MAPPING_FIELDS:
            if field not in existing:
                output_headers.append(field)
                existing.add(field)
        return output_headers

    def _map_row(self, row: dict[str, str], source_headers: list[str], output_headers: list[str]) -> dict[str, object]:
        out: dict[str, object] = {str(col): '' if row.get(col) is None else str(row.get(col, '')) for col in source_headers}
        context = self.mapping_service.build_context(out) if self.mapping_service else CanonicalMappingService({}).build_context(out)
        for field in AI_REVIEW_MAPPING_FIELDS:
            out[field] = context.values.get(field, [] if field in CANONICAL_MULTI_FIELDS else '')
        for field in output_headers:
            out.setdefault(field, '' if field not in CANONICAL_MULTI_FIELDS else [])
        return out

    def _build_review_input(self, row: dict[str, object], action_profile: str) -> AIReviewInput:
        context = {field: row.get(field, [] if field in CANONICAL_MULTI_FIELDS else '') for field in AI_REVIEW_MAPPING_FIELDS}
        plan = CapabilityEngine(action_profile).build_plan(context)
        confidence = self._parse_float(context.get('segmentation_confidence', ''))
        descriptions = [str(x) for x in [context.get('description_1', ''), context.get('description_2', ''), context.get('description_3', '')] if str(x).strip()]
        social_links: list[str] = []
        for key in ['social_facebook_urls', 'social_instagram_urls', 'social_linkedin_urls', 'social_youtube_urls', 'social_twitter_urls']:
            value = context.get(key, [])
            if isinstance(value, list):
                social_links.extend(value)
        social_links = social_links[:AI_MAX_SOCIAL_LINKS]
        photo_urls = list(context.get('photo_urls', []) or [])[:AI_MAX_PHOTO_LINKS]
        return AIReviewInput(
            canonical_context=context,
            sources_used_map={},
            profile_name=plan.profile_name,
            enabled_capabilities=plan.enabled_capabilities,
            outlet_id=str(context.get('outlet_id', '')),
            name=str(context.get('outlet_name', '')),
            address=str(context.get('full_address', '')),
            country=str(context.get('country', '')),
            country_code=str(context.get('country_code', '')),
            google_id=str(context.get('google_id', '')),
            place_id=str(context.get('place_id', '')),
            website=str(context.get('website_url', '')),
            website_root_url=str(context.get('website_root_url', '')),
            website_title=str(context.get('website_title', '')),
            website_meta_description=str(context.get('website_meta_description', '')),
            menu_urls=list(context.get('menu_urls', []) or []),
            photo_urls=photo_urls,
            social_links=social_links,
            segmentation_confidence=confidence,
            initial_segments=[str(context.get('fyre_market_segment_type0', '')), str(context.get('fyre_market_segment_type1', '')), str(context.get('fyre_market_segment_type2', '')), str(context.get('fyre_market_segment_type3', ''))],
            main_type=str(context.get('main_type', '')),
            all_types=str(context.get('all_types', '')),
            descriptions=descriptions,
            reviews_tags=str(context.get('reviews_tags', '')),
            characteristics=str(context.get('characteristics', '')),
            hotel_additional_informations=str(context.get('hotel_additional_informations', '')),
            price_range=str(context.get('price_range_simplified', '')),
            customer_reported_price_range=str(context.get('customer_reported_price_range', '')),
            website_lang=str(context.get('website_lang', '')),
            segmentation_reasons=str(context.get('segmentation_reasons', '')),
        )

    def _build_feature_extraction_result(self, review_input: AIReviewInput, threshold: float, only_low_confidence: bool) -> AIReviewResult:
        raw_conf = review_input.segmentation_confidence
        selected = True
        if raw_conf is not None and only_low_confidence and raw_conf >= max(threshold, AI_SKIP_IF_CONFIDENCE_ABOVE):
            selected = False

        detected_service_mode = self._scan_keywords(review_input, SERVICE_MODE_KEYWORDS, review_input.enabled_capabilities)
        detected_cuisine = self._scan_keywords(review_input, CUISINE_KEYWORDS, review_input.enabled_capabilities)
        detected_signals = self._scan_keywords(review_input, SIGNAL_KEYWORDS, review_input.enabled_capabilities)

        sources_used = []
        source_count = 0
        if review_input.website:
            sources_used.append('website')
            source_count += 1
        if review_input.menu_urls:
            sources_used.append('menu')
            source_count += 1
        if review_input.social_links:
            sources_used.append('social')
            source_count += min(len(review_input.social_links), AI_MAX_SOCIAL_LINKS)
        if review_input.photo_urls:
            sources_used.append('photos')
            source_count += min(len(review_input.photo_urls), AI_MAX_PHOTO_LINKS)

        fetched_sources: list[str] = []
        homepage_text = ''
        homepage_title = review_input.website_title
        homepage_meta = review_input.website_meta_description
        menu_excerpt = ''
        web_fetch_status = 'disabled'
        if selected and AI_WEB_FETCH_ENABLED and {'homepage_fetch', 'menu_fetch'} & set(review_input.enabled_capabilities):
            fetch_payload = self._fetch_web_evidence(review_input)
            web_fetch_status = fetch_payload['status']
            fetched_sources = fetch_payload['sources_fetched']
            homepage_text = fetch_payload['homepage_text']
            homepage_title = fetch_payload['homepage_title'] or homepage_title
            homepage_meta = fetch_payload['homepage_meta_description'] or homepage_meta
            menu_excerpt = fetch_payload['menu_text_excerpt']
            if fetched_sources:
                sources_used.extend([f'web:{src}' for src in fetched_sources if f'web:{src}' not in sources_used])

        evidence = []
        if detected_service_mode:
            evidence.append(f"service_mode={', '.join(detected_service_mode[:3])}")
        if detected_cuisine:
            evidence.append(f"cuisine={', '.join(detected_cuisine[:3])}")
        if detected_signals:
            evidence.append(f"signals={', '.join(detected_signals[:4])}")
        if fetched_sources:
            evidence.append(f"web_fetch={', '.join(fetched_sources)}")

        keywords = list(dict.fromkeys(detected_service_mode + detected_cuisine + detected_signals))[:AI_MAX_KEYWORDS]
        all_text_blocks = [review_input.main_type, review_input.all_types, ' '.join(review_input.descriptions), review_input.reviews_tags, review_input.characteristics, review_input.website_title, review_input.website_meta_description, homepage_text, menu_excerpt]
        web_text_content = ' | '.join(part for part in [homepage_text, menu_excerpt] if part)[:AI_FETCH_MAX_TEXT_CHARS]

        capability_field_usage = {}
        for capability in review_input.enabled_capabilities:
            capability_field_usage[capability] = [field for field in CapabilityEngine(review_input.profile_name).profile.get('capabilities', []) if field == capability]
        # better field usage from plan not preserved; produce consumed fields from engine cfg
        from .capability_engine import AI_REVIEW_CAPABILITIES
        capability_field_usage = {capability: list(AI_REVIEW_CAPABILITIES.get(capability, {}).get('consumes', [])) for capability in review_input.enabled_capabilities}
        llm_payload = self._run_llm_guardrails(review_input, homepage_title=homepage_title, homepage_meta=homepage_meta, menu_excerpt=menu_excerpt, web_text=web_text_content)

        return AIReviewResult(
            ai_review_status='selected' if selected else 'skipped',
            ai_confidence='' if raw_conf is None else f'{raw_conf:.3f}',
            ai_segment_suggested=' > '.join([segment for segment in review_input.initial_segments if segment]),
            ai_sources_used=' | '.join(sources_used),
            ai_evidence_summary=' || '.join(evidence),
            ai_requires_human_review='yes' if selected else 'no',
            ai_input_pack_summary=self._build_input_pack_summary(review_input),
            ai_selected_for_review='yes' if selected else 'no',
            ai_detected_service_mode=' | '.join(detected_service_mode),
            ai_detected_cuisine=' | '.join(detected_cuisine),
            ai_detected_keywords=' | '.join(keywords),
            ai_detected_signals_json=json.dumps({'service_mode': detected_service_mode, 'cuisine': detected_cuisine, 'signals': detected_signals}, ensure_ascii=False, sort_keys=True),
            ai_source_count=str(source_count),
            ai_web_fetch_status=web_fetch_status,
            ai_sources_fetched=' | '.join(fetched_sources),
            ai_web_text_content=web_text_content,
            ai_menu_text_excerpt=menu_excerpt[:AI_FETCH_MAX_TEXT_CHARS],
            ai_homepage_title=homepage_title,
            ai_homepage_meta_description=homepage_meta,
            ai_action_profile=review_input.profile_name,
            ai_enabled_capabilities=' | '.join(review_input.enabled_capabilities),
            ai_capability_field_usage=json.dumps(capability_field_usage, ensure_ascii=False, sort_keys=True),
            ai_llm_status=llm_payload.get('status', ''),
            ai_llm_reason=llm_payload.get('reason', ''),
            ai_llm_configured='yes' if llm_payload.get('llm_configured') else 'no',
            ai_llm_live_ready='yes' if llm_payload.get('llm_live_call_ready') else 'no',
            ai_llm_attempted='yes' if llm_payload.get('live_call_attempted') else 'no',
            ai_llm_result_source=str(llm_payload.get('result_source', '')),
            ai_llm_cost_estimated_eur=str(llm_payload.get('estimated_cost_eur', '')),
            ai_llm_cost_actual_eur=str(llm_payload.get('actual_cost_eur', '')),
            ai_llm_budget_remaining_eur=str(llm_payload.get('budget_remaining_eur', '')),
            ai_llm_cache_hit='yes' if llm_payload.get('cache_hit') else 'no',
            ai_llm_calls_used=str(llm_payload.get('row_calls_used', '')),
            ai_llm_provider=str(llm_payload.get('provider', '')),
            ai_llm_model=str(llm_payload.get('model', '')),
            ai_llm_result_json=json.dumps(llm_payload.get('result_json', {}), ensure_ascii=False, sort_keys=True),
            ai_llm_raw_excerpt=str(llm_payload.get('raw_content', ''))[:3000],
        )

    def _scan_keywords(self, review_input: AIReviewInput, keyword_map: dict[str, list[str]], enabled_capabilities: list[str]) -> list[str]:
        text_parts = [review_input.main_type, review_input.all_types, review_input.website_title, review_input.website_meta_description, review_input.reviews_tags, review_input.characteristics, review_input.segmentation_reasons]
        text_parts.extend(review_input.descriptions)
        haystack = ' '.join(part for part in text_parts if part)
        haystack_norm = normalized_text(haystack)
        detected: list[str] = []
        for label, keywords in keyword_map.items():
            for keyword in keywords:
                if normalized_text(keyword) and normalized_text(keyword) in haystack_norm:
                    detected.append(label)
                    break
        return detected

    def _build_input_pack_summary(self, review_input: AIReviewInput) -> str:
        present = []
        if review_input.name:
            present.append('name')
        if review_input.address:
            present.append('address')
        if review_input.website:
            present.append('website')
        if review_input.menu_urls:
            present.append(f'menu:{len(review_input.menu_urls)}')
        if review_input.photo_urls:
            present.append(f'photos:{len(review_input.photo_urls)}')
        if review_input.social_links:
            present.append(f'social:{len(review_input.social_links)}')
        present.append(f'profile:{review_input.profile_name}')
        present.append(f'capabilities:{len(review_input.enabled_capabilities)}')
        return ' | '.join(present)


    def _run_llm_guardrails(self, review_input: AIReviewInput, homepage_title: str = '', homepage_meta: str = '', menu_excerpt: str = '', web_text: str = '') -> dict[str, object]:
        if not self.llm_client or not self.budget_manager:
            return {'status': 'disabled', 'reason': 'llm_client_unavailable', 'estimated_cost_eur': 0.0, 'actual_cost_eur': 0.0, 'budget_remaining_eur': 0.0, 'cache_hit': False, 'row_calls_used': 0, 'provider': '', 'model': '', 'result_json': {}, 'raw_content': ''}
        capability = next((c for c in review_input.enabled_capabilities if c in AI_LLM_CAPABILITIES), None)
        if not capability:
            return {'status': 'skipped', 'reason': 'no_llm_capability_enabled', 'estimated_cost_eur': 0.0, 'actual_cost_eur': 0.0, 'budget_remaining_eur': round(self.budget_manager.remaining_budget(), 6), 'cache_hit': False, 'row_calls_used': self.budget_manager.row_calls.get(review_input.outlet_id or review_input.name or 'unknown', 0), 'provider': str(getattr(self.llm_client, 'provider', AI_LLM_PROVIDER)), 'model': str(getattr(self.llm_client, 'model', AI_LLM_MODEL)), 'result_json': {}, 'raw_content': ''}
        prompt_text = self._build_llm_prompt(review_input, capability, homepage_title=homepage_title, homepage_meta=homepage_meta, menu_excerpt=menu_excerpt, web_text=web_text)
        row_key = review_input.outlet_id or review_input.google_id or review_input.place_id or review_input.name or 'unknown'
        context = dict(review_input.canonical_context)
        context.update({
            'website_title': homepage_title,
            'website_meta_description': homepage_meta,
            'web_text': web_text[:3000],
            'menu_excerpt': menu_excerpt[:2000],
        })
        self._llm_stats['calls_attempted'] += 1
        payload = self.llm_client.call(row_key=row_key, capability=capability, context=context, prompt_text=prompt_text)
        reason = payload.get('reason') or ''
        if payload.get('cache_hit'):
            self._llm_stats['cache_hits'] += 1
        elif payload.get('status') in {'live_success'}:
            self._llm_stats['calls_executed'] += 1
        elif reason == 'budget_exceeded':
            self._llm_stats['budget_skips'] += 1
        elif reason == 'row_budget_exceeded':
            self._llm_stats['row_budget_skips'] += 1
        elif reason == 'max_calls_per_row_exceeded':
            self._llm_stats['max_call_skips'] += 1
        payload['budget_remaining_eur'] = round(self.budget_manager.remaining_budget(), 6)
        payload['row_calls_used'] = self.budget_manager.row_calls.get(row_key, 0)
        return payload

    @staticmethod
    def _build_llm_prompt(review_input: AIReviewInput, capability: str, homepage_title: str = '', homepage_meta: str = '', menu_excerpt: str = '', web_text: str = '') -> str:
        descriptions = ' | '.join([d for d in review_input.descriptions if d])
        initial_segments = ' > '.join([seg for seg in review_input.initial_segments if seg])
        menu_urls = ';'.join(review_input.menu_urls[:2])
        blocks = [
            f'capability={capability}',
            f'name={review_input.name}',
            f'country={review_input.country or review_input.country_code}',
            f'address={review_input.address}',
            f'website={review_input.website}',
            f'main_type={review_input.main_type}',
            f'all_types={review_input.all_types}',
            f'website_title={review_input.website_title}',
            f'website_meta_description={review_input.website_meta_description}',
            f'descriptions={descriptions}',
            f'reviews_tags={review_input.reviews_tags}',
            f'characteristics={review_input.characteristics}',
            f'initial_segments={initial_segments}',
            f'segmentation_reasons={review_input.segmentation_reasons}',
            f'menu_urls={menu_urls}',
            f'fetched_homepage_title={homepage_title}',
            f'fetched_homepage_meta_description={homepage_meta}',
            f'fetched_menu_excerpt={menu_excerpt[:2000]}',
            f'fetched_web_text={web_text[:3000]}',
            'Return JSON only.',
        ]
        return '\n'.join(blocks)

    def _fetch_web_evidence(self, review_input: AIReviewInput) -> dict[str, object]:
        if not self.web_cache:
            return {'status': 'disabled', 'sources_fetched': [], 'homepage_text': '', 'homepage_title': '', 'homepage_meta_description': '', 'menu_text_excerpt': ''}
        homepage_urls = [url for _source_type, url in candidate_urls(review_input.website_root_url, review_input.website)]
        menu_urls = [url for url in review_input.menu_urls if str(url).strip()]
        queue: list[tuple[str, str]] = []
        if 'homepage_fetch' in review_input.enabled_capabilities:
            for url in homepage_urls[:1]:
                queue.append(('homepage', url))
        if 'menu_fetch' in review_input.enabled_capabilities:
            for url in menu_urls[:1]:
                queue.append(('menu', url))
        queue = queue[:AI_MAX_PAGES_PER_OUTLET]
        homepage_text = ''
        homepage_title = ''
        homepage_meta = ''
        menu_excerpt = ''
        sources_fetched: list[str] = []
        status = 'no_source'
        used_fetch = False
        for label, url in queue:
            self._web_fetch_stats['pages_attempted'] += 1
            cached = self.web_cache.get(url)
            try:
                if cached is not None:
                    html = cached
                    self._web_fetch_stats['cache_hits'] += 1
                else:
                    html = fetch_html(url, timeout_seconds=AI_FETCH_TIMEOUT_SECONDS, user_agent=AI_FETCH_USER_AGENT, max_bytes=AI_FETCH_MAX_DOWNLOAD_BYTES)
                    if html:
                        self.web_cache.set(url, html)
                    used_fetch = True
            except Exception:
                html = ''
            if not html:
                status = 'error' if status == 'no_source' else status
                continue
            self._web_fetch_stats['pages_ok'] += 1
            text = strip_html_to_text(html)[:AI_FETCH_MAX_TEXT_CHARS]
            sources_fetched.append(f'{label}:{host_label(url)}')
            status = 'ok'
            if label == 'homepage':
                homepage_text = text
                homepage_title = extract_title(html)
                homepage_meta = extract_meta_description(html)
            elif label == 'menu':
                menu_excerpt = text
        if used_fetch and sources_fetched:
            self._web_fetch_stats['outlets_with_fetch'] += 1
        return {'status': status, 'sources_fetched': sources_fetched, 'homepage_text': homepage_text, 'homepage_title': homepage_title, 'homepage_meta_description': homepage_meta, 'menu_text_excerpt': menu_excerpt}

    @staticmethod
    def _parse_float(value: object) -> float | None:
        try:
            text = str(value).replace(',', '.').strip()
            return float(text) if text != '' else None
        except Exception:
            return None


def suggest_ai_review_mapping(columns: list[str]) -> dict[str, str]:
    return CanonicalMappingService({}).suggest_column_mapping(columns)


def inspect_ai_review_file(uploaded_file, sheet_name: str | None = None) -> dict:
    payload = inspect_table_file(uploaded_file, sheet_name=sheet_name)
    for sheet in payload.get('sheets', []):
        sheet['mapping_suggestions'] = suggest_ai_review_mapping(sheet.get('detected_columns', []))
        sheet['missing_required'] = sorted(AI_REVIEW_REQUIRED_FIELDS - set(sheet['mapping_suggestions'].keys()))
    return payload

from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from ai_review.llm.config import describe_provider_runtime, get_default_model_for_provider, load_llm_guardrails_config, resolve_llm_runtime_config
from core.config_loader import active_config_origin, resolve_config_path


class Command(BaseCommand):
    help = 'Affiche la configuration LLM réellement active (catalogue, override, env, provider, modèle).'

    def add_arguments(self, parser):
        parser.add_argument('--provider', default='', help='Provider à inspecter (sinon provider des guardrails ou défaut catalogue).')
        parser.add_argument('--model', default='', help='Modèle attendu à comparer.')

    def handle(self, *args, **options):
        guardrails = load_llm_guardrails_config()
        provider = str(options.get('provider') or guardrails.get('provider') or '').strip()
        if not provider:
            provider = 'openai_compatible_json'
        model = str(options.get('model') or guardrails.get('model') or get_default_model_for_provider(provider) or '').strip()
        resolved_runtime = resolve_llm_runtime_config({**guardrails, 'provider': provider, 'model': model})
        payload = {
            'guardrails_config_path': str(resolve_config_path('ai_review/llm_guardrails.yaml')),
            'guardrails_config_origin': active_config_origin('ai_review/llm_guardrails.yaml'),
            'providers_config_path': str(resolve_config_path('ai_review/llm_providers.yaml')),
            'providers_config_origin': active_config_origin('ai_review/llm_providers.yaml'),
            'requested_provider': provider,
            'requested_model': model,
            'provider_runtime': describe_provider_runtime(provider),
            'resolved_runtime_config': resolved_runtime,
            'model_matches_provider_catalog': model in (describe_provider_runtime(provider).get('models') or []),
            'guardrails_enabled_default': bool(guardrails.get('enabled', False)),
            'guardrails_provider_default': str(guardrails.get('provider') or ''),
            'guardrails_model_default': str(guardrails.get('model') or ''),
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))

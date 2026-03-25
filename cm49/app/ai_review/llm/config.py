from __future__ import annotations

import os
from typing import Any

from core.config_loader import active_config_origin, load_yaml_config, resolve_config_path


PROVIDER_LOCKED_FIELDS = {
    'api_key_env',
    'base_url',
    'endpoint_style',
    'anthropic_version',
    'label',
    'models',
}


def load_llm_guardrails_config() -> dict:
    data = load_yaml_config('ai_review/llm_guardrails.yaml')
    return data if isinstance(data, dict) else {}


def load_llm_providers_config() -> dict:
    data = load_yaml_config('ai_review/llm_providers.yaml')
    return data if isinstance(data, dict) else {}


def get_provider_catalog() -> dict[str, dict[str, Any]]:
    data = load_llm_providers_config()
    providers = data.get('providers') or {}
    return providers if isinstance(providers, dict) else {}


def get_provider_runtime_config(provider_key: str | None) -> dict[str, Any]:
    provider_key = str(provider_key or '').strip()
    catalog = load_llm_providers_config()
    providers = catalog.get('providers') or {}
    provider_cfg = providers.get(provider_key) or {}
    if not isinstance(provider_cfg, dict):
        return {}
    return dict(provider_cfg)


def get_default_model_for_provider(provider_key: str | None) -> str:
    provider_key = str(provider_key or '').strip()
    catalog = load_llm_providers_config()
    defaults = catalog.get('default_model_by_provider') or {}
    if isinstance(defaults, dict) and provider_key in defaults:
        return str(defaults.get(provider_key) or '')
    provider_cfg = get_provider_runtime_config(provider_key)
    models = provider_cfg.get('models') or []
    if isinstance(models, list) and models:
        first = models[0]
        if isinstance(first, dict):
            return str(first.get('id') or '')
        return str(first or '')
    return ''


def get_provider_choices() -> list[tuple[str, str]]:
    providers = get_provider_catalog()
    choices = []
    for key, cfg in providers.items():
        label = str((cfg or {}).get('label') or key)
        choices.append((key, label))
    return choices


def get_model_choices_grouped() -> list[tuple[str, list[tuple[str, str]]]]:
    providers = get_provider_catalog()
    grouped: list[tuple[str, list[tuple[str, str]]]] = []
    for provider_key, cfg in providers.items():
        provider_label = str((cfg or {}).get('label') or provider_key)
        models = []
        for item in (cfg or {}).get('models', []) or []:
            if isinstance(item, dict):
                model_id = str(item.get('id') or '').strip()
                model_label = str(item.get('label') or model_id)
            else:
                model_id = str(item or '').strip()
                model_label = model_id
            if model_id:
                models.append((model_id, model_label))
        if models:
            grouped.append((provider_label, models))
    return grouped


def model_belongs_to_provider(provider_key: str | None, model_id: str | None) -> bool:
    provider_cfg = get_provider_runtime_config(provider_key)
    target = str(model_id or '').strip()
    if not target:
        return False
    for item in provider_cfg.get('models', []) or []:
        current = str(item.get('id') if isinstance(item, dict) else item or '').strip()
        if current == target:
            return True
    return False




def resolve_llm_runtime_config(config: dict | None) -> dict[str, Any]:
    base = dict(config or {})
    catalog = load_llm_providers_config()
    default_provider = str(catalog.get('default_provider') or 'openai_compatible_json').strip()
    provider_key = str(base.get('provider') or default_provider).strip() or default_provider
    provider_cfg = get_provider_runtime_config(provider_key)

    resolved: dict[str, Any] = {}
    if isinstance(provider_cfg, dict):
        resolved.update(provider_cfg)

    for key, value in base.items():
        if key in PROVIDER_LOCKED_FIELDS:
            continue
        resolved[key] = value

    resolved['provider'] = provider_key
    if not str(resolved.get('model') or '').strip():
        resolved['model'] = get_default_model_for_provider(provider_key)
    return resolved

def get_llm_api_key(config: dict) -> str:
    env_name = str(config.get('api_key_env', 'OPENAI_API_KEY') or 'OPENAI_API_KEY')
    return os.environ.get(env_name, '')



def describe_provider_runtime(provider_key: str | None) -> dict[str, Any]:
    provider_key = str(provider_key or '').strip()
    catalog = load_llm_providers_config()
    provider_cfg = get_provider_runtime_config(provider_key)
    defaults = catalog.get('default_model_by_provider') or {}
    config_path = resolve_config_path('ai_review/llm_providers.yaml')
    env_name = str(provider_cfg.get('api_key_env', 'OPENAI_API_KEY') or 'OPENAI_API_KEY')
    api_key = os.environ.get(env_name, '')
    return {
        'provider': provider_key,
        'config_origin': active_config_origin('ai_review/llm_providers.yaml'),
        'config_path': str(config_path),
        'provider_exists': bool(provider_cfg),
        'provider_label': str(provider_cfg.get('label') or provider_key),
        'endpoint_style': str(provider_cfg.get('endpoint_style') or ''),
        'api_key_env': env_name,
        'api_key_present': bool(api_key),
        'api_key_length': len(api_key or ''),
        'base_url': str(provider_cfg.get('base_url') or ''),
        'anthropic_version': str(provider_cfg.get('anthropic_version') or ''),
        'default_model': str(defaults.get(provider_key) or get_default_model_for_provider(provider_key)),
        'models': [
            str(item.get('id') if isinstance(item, dict) else item or '').strip()
            for item in (provider_cfg.get('models') or [])
            if str(item.get('id') if isinstance(item, dict) else item or '').strip()
        ],
    }

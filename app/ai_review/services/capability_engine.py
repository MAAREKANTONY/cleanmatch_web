from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.config_loader import load_yaml_config, list_country_config_files


_CFG_CAPABILITIES = load_yaml_config('ai_review/capabilities.yaml')
_CFG_ACTION_PROFILES = load_yaml_config('ai_review/action_profiles.yaml')
_CFG_DEFAULT_COUNTRY = load_yaml_config('ai_review/countries/default.yaml')

AI_REVIEW_CAPABILITIES: dict[str, dict[str, Any]] = _CFG_CAPABILITIES.get('capabilities', {})
AI_REVIEW_ACTION_PROFILES: dict[str, dict[str, Any]] = _CFG_ACTION_PROFILES.get('profiles', {})
AI_REVIEW_DEFAULT_ACTION_PROFILE = str(_CFG_ACTION_PROFILES.get('default_profile', 'standard'))
AI_REVIEW_COUNTRY_DEFAULT_OVERRIDES: dict[str, Any] = _CFG_DEFAULT_COUNTRY.get('overrides', {})
AI_REVIEW_COUNTRY_OVERRIDES: dict[str, dict[str, Any]] = {}
for _cfg_path in list_country_config_files('ai_review'):
    if _cfg_path.name == 'default.yaml':
        continue
    _payload = load_yaml_config(f'ai_review/countries/{_cfg_path.name}')
    _code = str(_payload.get('country_code', '')).upper()
    if _code:
        AI_REVIEW_COUNTRY_OVERRIDES[_code] = dict(_payload.get('overrides') or {})


@dataclass
class CapabilityPlan:
    profile_name: str
    enabled_capabilities: list[str]
    field_usage: dict[str, list[str]]


class CapabilityEngine:
    def __init__(self, profile_name: str | None = None):
        requested = str(profile_name or AI_REVIEW_DEFAULT_ACTION_PROFILE)
        self.profile_name = requested if requested in AI_REVIEW_ACTION_PROFILES else AI_REVIEW_DEFAULT_ACTION_PROFILE
        self.profile = AI_REVIEW_ACTION_PROFILES.get(self.profile_name, {})

    def build_plan(self, canonical_context: dict[str, Any]) -> CapabilityPlan:
        country_code = str(canonical_context.get('country_code') or '').upper()
        enabled: list[str] = []
        field_usage: dict[str, list[str]] = {}
        capabilities = list(self.profile.get('capabilities', []))
        capabilities = self._apply_country_overrides(capabilities, country_code)
        for capability in capabilities:
            if self.is_capability_available(canonical_context, capability):
                enabled.append(capability)
                field_usage[capability] = list(AI_REVIEW_CAPABILITIES.get(capability, {}).get('consumes', []))
        return CapabilityPlan(profile_name=self.profile_name, enabled_capabilities=enabled, field_usage=field_usage)

    def _apply_country_overrides(self, capabilities: list[str], country_code: str) -> list[str]:
        ordered = list(capabilities)
        combined = dict(AI_REVIEW_COUNTRY_DEFAULT_OVERRIDES)
        combined.update(AI_REVIEW_COUNTRY_OVERRIDES.get(country_code, {}))
        promote = [str(v) for v in combined.get('promote_capabilities', []) if str(v).strip()]
        demote = {str(v) for v in combined.get('demote_capabilities', []) if str(v).strip()}
        disable = {str(v) for v in combined.get('disable_capabilities', []) if str(v).strip()}
        promoted = [cap for cap in promote if cap in ordered and cap not in disable]
        remaining = [cap for cap in ordered if cap not in promoted and cap not in disable]
        remaining.sort(key=lambda cap: (cap in demote, ordered.index(cap)))
        return promoted + remaining

    def is_capability_available(self, canonical_context: dict[str, Any], capability_name: str) -> bool:
        cfg = AI_REVIEW_CAPABILITIES.get(capability_name, {})
        consumed = list(cfg.get('consumes', []))
        require_all = bool(cfg.get('require_all', False))
        if not consumed:
            return True
        available = [self._has_value(canonical_context.get(field)) for field in consumed]
        return all(available) if require_all else any(available)

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, list):
            return any(str(v).strip() for v in value)
        return bool(str(value).strip())

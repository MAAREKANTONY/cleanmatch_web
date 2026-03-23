from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.config_loader import load_yaml_config


_CFG_CAPABILITIES = load_yaml_config('ai_review/capabilities.yaml')
_CFG_ACTION_PROFILES = load_yaml_config('ai_review/action_profiles.yaml')
_CFG_DEFAULT_COUNTRY = load_yaml_config('ai_review/countries/default.yaml')

AI_REVIEW_CAPABILITIES: dict[str, dict[str, Any]] = _CFG_CAPABILITIES.get('capabilities', {})
AI_REVIEW_ACTION_PROFILES: dict[str, dict[str, Any]] = _CFG_ACTION_PROFILES.get('profiles', {})
AI_REVIEW_DEFAULT_ACTION_PROFILE = str(_CFG_ACTION_PROFILES.get('default_profile', 'standard'))
AI_REVIEW_COUNTRY_DEFAULT_OVERRIDES: dict[str, Any] = _CFG_DEFAULT_COUNTRY.get('overrides', {})


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
        enabled: list[str] = []
        field_usage: dict[str, list[str]] = {}
        for capability in self.profile.get('capabilities', []):
            if self.is_capability_available(canonical_context, capability):
                enabled.append(capability)
                field_usage[capability] = list(AI_REVIEW_CAPABILITIES.get(capability, {}).get('consumes', []))
        return CapabilityPlan(profile_name=self.profile_name, enabled_capabilities=enabled, field_usage=field_usage)

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

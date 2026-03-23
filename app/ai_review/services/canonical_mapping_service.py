from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from core.config_loader import load_yaml_config

from .ai_review_utils import first_non_empty, split_multi_value


_CFG_FIELDS = load_yaml_config('ai_review/mapping_fields.yaml')
_CFG_ALIASES = load_yaml_config('ai_review/column_aliases.yaml')

CANONICAL_FIELD_SPECS: dict[str, dict[str, Any]] = _CFG_FIELDS.get('mapping_fields', {})
CANONICAL_FIELD_TYPES: dict[str, str] = {k: str(v.get('type', 'string')) for k, v in CANONICAL_FIELD_SPECS.items()}
CANONICAL_REQUIRED_FIELDS: set[str] = {k for k, v in CANONICAL_FIELD_SPECS.items() if bool(v.get('required', False))}
CANONICAL_MULTI_FIELDS: set[str] = {k for k, t in CANONICAL_FIELD_TYPES.items() if t == 'list'}
AI_REVIEW_COLUMN_ALIASES: dict[str, list[str]] = _CFG_ALIASES.get('aliases', {})


@dataclass
class CanonicalMappingContext:
    values: dict[str, Any]
    sources_used: dict[str, list[str]]


class CanonicalMappingService:
    def __init__(self, manual_mapping: dict[str, str] | None = None):
        self.manual_mapping = {str(k): str(v) for k, v in (manual_mapping or {}).items() if str(v).strip()}

    def suggest_column_mapping(self, columns: list[str]) -> dict[str, str]:
        normalized_to_original = {self._normalize_label(col): col for col in columns}
        suggestions: dict[str, str] = {}
        used_sources: set[str] = set()
        for target, aliases in AI_REVIEW_COLUMN_ALIASES.items():
            for alias in aliases:
                candidate = normalized_to_original.get(self._normalize_label(alias))
                if candidate and candidate not in used_sources:
                    suggestions[target] = candidate
                    used_sources.add(candidate)
                    break
            if target in suggestions:
                continue
            for norm, original in normalized_to_original.items():
                if original in used_sources:
                    continue
                if any(self._normalize_label(alias) in norm or norm in self._normalize_label(alias) for alias in aliases):
                    suggestions[target] = original
                    used_sources.add(original)
                    break
        return suggestions

    def build_context(self, row: dict[str, Any]) -> CanonicalMappingContext:
        values: dict[str, Any] = {}
        sources_used: dict[str, list[str]] = {}
        for field_name in CANONICAL_FIELD_SPECS.keys():
            value, used = self._resolve_field(row, field_name)
            values[field_name] = value
            sources_used[field_name] = used
        return CanonicalMappingContext(values=values, sources_used=sources_used)

    def _resolve_field(self, row: dict[str, Any], field_name: str) -> tuple[Any, list[str]]:
        manual_source = self.manual_mapping.get(field_name)
        candidates = [manual_source] if manual_source else []
        candidates.extend(AI_REVIEW_COLUMN_ALIASES.get(field_name, []))
        candidates.append(field_name)
        seen: set[str] = set()
        deduped = []
        for candidate in candidates:
            if candidate and candidate not in seen:
                seen.add(candidate)
                deduped.append(candidate)
        if field_name in CANONICAL_MULTI_FIELDS:
            collected: list[str] = []
            used_sources: list[str] = []
            for candidate in deduped:
                if candidate in row and row.get(candidate) not in (None, ''):
                    used_sources.append(candidate)
                    collected.extend(split_multi_value(row.get(candidate, '')))
            return self._dedupe_list(collected), used_sources
        for candidate in deduped:
            if candidate in row and str(row.get(candidate, '')).strip():
                return str(row.get(candidate, '')).strip(), [candidate]
        return '' if CANONICAL_FIELD_TYPES.get(field_name) != 'list' else [], []

    @staticmethod
    def _dedupe_list(values: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = str(value or '').strip()
            if not cleaned:
                continue
            if cleaned not in seen:
                seen.add(cleaned)
                out.append(cleaned)
        return out

    @staticmethod
    def _normalize_label(value: str) -> str:
        lowered = str(value or '').strip().lower()
        out = []
        for ch in lowered:
            if ch.isalnum():
                out.append(ch)
            else:
                out.append('_')
        while '__' in ''.join(out):
            lowered = ''.join(out).replace('__', '_')
            out = list(lowered)
        return ''.join(out).strip('_')

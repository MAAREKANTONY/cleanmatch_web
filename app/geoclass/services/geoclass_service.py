from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd
from slugify import slugify

from core.config_loader import load_yaml_config

ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]

_GEOCLASS_MAPPING = load_yaml_config('geoclass/mapping_fields.yaml')
_GEOCLASS_RULES = load_yaml_config('geoclass/keyword_rules.yaml')

GEOCLASS_MAPPING_FIELDS = list(_GEOCLASS_MAPPING.get('mapping_fields', []))
GEOCLASS_REQUIRED_FIELDS = set(_GEOCLASS_MAPPING.get('required_fields', []))
KEYWORD_RULES = [
    (rule.get('code', ''), rule.get('category', ''), rule.get('subcategory', ''), list(rule.get('keywords', [])))
    for rule in _GEOCLASS_RULES.get('keyword_rules', [])
]


def _noop_progress(percent: int, message: str) -> None:
    return None


def _noop_log(message: str) -> None:
    return None


def _read_table(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {'.csv', '.txt'}:
        return pd.read_csv(path)
    if suffix in {'.xlsx', '.xlsm', '.xltx', '.xltm', '.xls'}:
        return pd.read_excel(path, sheet_name=sheet_name)
    raise ValueError(f'Format non supporté pour le geoclass: {path.suffix}')


@dataclass
class GeoclassOptions:
    geoclass_sheet_name: str | None = None
    geoclass_mapping: dict[str, str] = field(default_factory=dict)


class GeoclassService:
    def __init__(self, progress_callback: ProgressCallback | None = None, log_callback: LogCallback | None = None):
        self.progress_callback = progress_callback or _noop_progress
        self.log_callback = log_callback or _noop_log

    def progress(self, percent: int, message: str):
        self.progress_callback(percent, message)

    def log(self, message: str):
        self.log_callback(message)

    def run(self, input_path: Path, output_path: Path, options: GeoclassOptions) -> Path:
        self.progress(5, 'Chargement du fichier source du geoclass')
        df = _read_table(input_path, options.geoclass_sheet_name)
        self.log(f'📘 Geoclass source : {input_path.name} - {len(df)} lignes')
        df = self._apply_mapping(df, options.geoclass_mapping)
        self.progress(20, 'Classification heuristique')
        rows = []
        total = max(len(df), 1)
        stats = {}
        for index, (_, row) in enumerate(df.iterrows(), start=1):
            payload = self._classify_row(row)
            rows.append({**{f: row.get(f, '') for f in GEOCLASS_MAPPING_FIELDS if f in row.index}, **payload})
            stats[payload['geoclass_category']] = stats.get(payload['geoclass_category'], 0) + 1
            if index == 1 or index % 100 == 0 or index == total:
                pct = 20 + int(index / total * 70)
                self.progress(min(pct, 92), f'Classification en cours : {index}/{total}')
        out_df = pd.DataFrame(rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        output_path.with_name(output_path.stem + '_summary.json').write_text(json.dumps({'rows': len(out_df), 'category_stats': stats}, ensure_ascii=False, indent=2), encoding='utf-8')
        self.log(f'✅ Geoclass terminé : {stats}')
        self.progress(100, 'Geoclass terminé')
        return output_path

    def _apply_mapping(self, df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        df = df.copy()
        missing_mapping_sources = [source for source in mapping.values() if source not in df.columns]
        if missing_mapping_sources:
            raise ValueError(f'Le fichier geoclass ne contient pas certaines colonnes mappées: {", ".join(sorted(set(missing_mapping_sources)))}')
        reverse = {source: target for target, source in mapping.items() if source in df.columns}
        df = df.rename(columns=reverse)
        for field in GEOCLASS_MAPPING_FIELDS:
            if field not in df.columns:
                df[field] = ''
        for field in GEOCLASS_MAPPING_FIELDS:
            df[field] = df[field].fillna('').astype(str)
        return df

    def _classify_row(self, row: pd.Series) -> dict:
        haystack = ' '.join([str(row.get('name', '')), str(row.get('address', '')), str(row.get('website', '')), str(row.get('email', ''))]).lower()
        haystack_slug = slugify(haystack, separator=' ')
        for code, category, subcategory, keywords in KEYWORD_RULES:
            if any(slugify(k, separator=' ') in haystack_slug for k in keywords):
                return {
                    'geoclass_code': code,
                    'geoclass_category': category,
                    'geoclass_subcategory': subcategory,
                    'geoclass_confidence': 0.86,
                    'geoclass_reason': f'keyword:{code}',
                }
        return {
            'geoclass_code': 'unknown',
            'geoclass_category': 'unknown',
            'geoclass_subcategory': 'unknown',
            'geoclass_confidence': 0.35,
            'geoclass_reason': 'no_rule_match',
        }

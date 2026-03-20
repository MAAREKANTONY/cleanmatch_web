from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


CATALOG_DIR = Path(__file__).resolve().parents[1] / 'config_catalog'


@lru_cache(maxsize=None)
def load_yaml_config(relative_path: str) -> dict[str, Any]:
    path = CATALOG_DIR / relative_path
    if not path.exists():
        raise FileNotFoundError(f'Configuration introuvable: {relative_path}')
    data = yaml.safe_load(path.read_text(encoding='utf-8'))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f'Configuration invalide (dict attendu): {relative_path}')
    return data


@lru_cache(maxsize=None)
def load_csv_rows(relative_path: str) -> list[dict[str, str]]:
    path = CATALOG_DIR / relative_path
    if not path.exists():
        raise FileNotFoundError(f'Configuration CSV introuvable: {relative_path}')
    with path.open('r', encoding='utf-8-sig', newline='') as fh:
        return list(csv.DictReader(fh))


@lru_cache(maxsize=1)
def catalog_version() -> dict[str, Any]:
    return load_yaml_config('version.yaml')


@lru_cache(maxsize=1)
def list_country_config_files(module_name: str) -> list[Path]:
    path = CATALOG_DIR / module_name / 'countries'
    if not path.exists():
        return []
    return sorted(path.glob('*.yaml'))

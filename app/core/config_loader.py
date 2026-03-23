from __future__ import annotations

import csv
import os
import shutil
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CATALOG_DIR = Path(os.getenv('CONFIG_CATALOG_DIR', Path(__file__).resolve().parents[1] / 'config_catalog')).resolve()
OVERRIDE_CATALOG_DIR = Path(os.getenv('CONFIG_OVERRIDE_DIR', '/data/config_overrides')).resolve()
CATALOG_DIR = DEFAULT_CATALOG_DIR



def _normalize_relative_path(relative_path: str) -> Path:
    rel = Path(relative_path)
    if rel.is_absolute():
        raise ValueError(f'Configuration path must be relative: {relative_path}')
    return rel



def _candidate_config_paths(relative_path: str) -> list[Path]:
    rel = _normalize_relative_path(relative_path)
    return [OVERRIDE_CATALOG_DIR / rel, DEFAULT_CATALOG_DIR / rel]



def resolve_config_path(relative_path: str) -> Path:
    for path in _candidate_config_paths(relative_path):
        if path.exists():
            return path
    raise FileNotFoundError(f'Configuration introuvable: {relative_path}')



def config_exists(relative_path: str) -> bool:
    return any(path.exists() for path in _candidate_config_paths(relative_path))



def active_config_origin(relative_path: str) -> str:
    path = resolve_config_path(relative_path)
    try:
        path.relative_to(OVERRIDE_CATALOG_DIR)
        return 'override'
    except ValueError:
        return 'default'


@lru_cache(maxsize=None)
def load_yaml_config(relative_path: str) -> dict[str, Any]:
    path = resolve_config_path(relative_path)
    data = yaml.safe_load(path.read_text(encoding='utf-8'))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f'Configuration invalide (dict attendu): {relative_path}')
    return data


@lru_cache(maxsize=None)
def load_csv_rows(relative_path: str) -> list[dict[str, str]]:
    path = resolve_config_path(relative_path)
    with path.open('r', encoding='utf-8-sig', newline='') as fh:
        return list(csv.DictReader(fh))


@lru_cache(maxsize=1)
def catalog_version() -> dict[str, Any]:
    return load_yaml_config('version.yaml')


@lru_cache(maxsize=1)
def list_country_config_files(module_name: str) -> list[Path]:
    relative_dir = Path(module_name) / 'countries'
    names: set[str] = set()
    files: list[Path] = []
    for base_dir in [OVERRIDE_CATALOG_DIR, DEFAULT_CATALOG_DIR]:
        path = base_dir / relative_dir
        if not path.exists():
            continue
        for file_path in sorted(path.glob('*.yaml')):
            if file_path.name in names:
                continue
            names.add(file_path.name)
            files.append(file_path)
    return sorted(files, key=lambda item: item.name)



def bootstrap_config_overrides(force: bool = False) -> list[Path]:
    copied: list[Path] = []
    for source_path in sorted(DEFAULT_CATALOG_DIR.rglob('*')):
        if not source_path.is_file():
            continue
        relative_path = source_path.relative_to(DEFAULT_CATALOG_DIR)
        target_path = OVERRIDE_CATALOG_DIR / relative_path
        if target_path.exists() and not force:
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        copied.append(target_path)
    load_yaml_config.cache_clear()
    load_csv_rows.cache_clear()
    catalog_version.cache_clear()
    list_country_config_files.cache_clear()
    return copied

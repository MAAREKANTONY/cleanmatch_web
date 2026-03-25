from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from slugify import slugify

from core.config_loader import load_yaml_config

ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]

_GEOCODER_MAPPING = load_yaml_config('geocoder/mapping_fields.yaml')
_GEOCODER_ALIASES = load_yaml_config('geocoder/column_aliases.yaml')
_GEOCODER_COUNTRIES = load_yaml_config('geocoder/countries.yaml')
_GEOCODER_PROVIDERS = load_yaml_config('geocoder/providers.yaml')
_GEOCODER_CACHE = load_yaml_config('geocoder/cache.yaml')
_GEOCODER_CHECKPOINT = load_yaml_config('geocoder/checkpoint.yaml')
_GEOCODER_QUERY = load_yaml_config('geocoder/query_templates.yaml')

GEOCODER_MAPPING_FIELDS = list(_GEOCODER_MAPPING.get('mapping_fields', []))
GEOCODER_REQUIRED_FIELDS = set(_GEOCODER_MAPPING.get('required_fields', []))
GEOCODER_COLUMN_ALIASES = _GEOCODER_ALIASES.get('column_aliases', {})
GEOCODER_COUNTRY_ALIASES = _GEOCODER_COUNTRIES.get('country_aliases', {})
GEOCODER_DEFAULT_PROVIDER = _GEOCODER_PROVIDERS.get('default_provider', 'existing_or_nominatim')
GEOCODER_PROVIDER_SETTINGS = _GEOCODER_PROVIDERS.get('providers', {}).get('existing_or_nominatim', {})
GEOCODER_DEFAULT_USER_AGENT = str(GEOCODER_PROVIDER_SETTINGS.get('user_agent', 'cleanmatch-web'))
GEOCODER_TIMEOUT_SECONDS = int(GEOCODER_PROVIDER_SETTINGS.get('timeout_seconds', 10))
GEOCODER_MIN_DELAY_SECONDS = float(GEOCODER_PROVIDER_SETTINGS.get('min_delay_seconds', 1))
GEOCODER_PROVIDER_SOURCE_LABEL = str(GEOCODER_PROVIDER_SETTINGS.get('source_label', 'nominatim'))
GEOCODER_CACHE_FILENAME = str(_GEOCODER_CACHE.get('cache_filename', 'geocode_cache_web.sqlite3'))
GEOCODER_CHECKPOINT_EVERY = int(_GEOCODER_CHECKPOINT.get('checkpoint_every', 50))
GEOCODER_RESUME_ENABLED = bool(_GEOCODER_CHECKPOINT.get('resume_enabled', True))
GEOCODER_CHECKPOINT_SUFFIX = str(_GEOCODER_CHECKPOINT.get('checkpoint_suffix', '.checkpoint.json'))
GEOCODER_QUERY_ORDER = list(_GEOCODER_QUERY.get('default_query_order', ['name', 'address', 'zipcode', 'city', 'country']))


def _noop_progress(percent: int, message: str) -> None:
    return None


def _noop_log(message: str) -> None:
    return None


def normalized_label(value: str) -> str:
    return slugify(str(value or ''), separator='_')


def suggest_column_mapping(columns: list[str]) -> dict[str, str]:
    normalized_to_original = {normalized_label(col): col for col in columns}
    suggestions: dict[str, str] = {}
    used_sources: set[str] = set()
    for target, aliases in GEOCODER_COLUMN_ALIASES.items():
        for alias in aliases:
            candidate = normalized_to_original.get(normalized_label(alias))
            if candidate and candidate not in used_sources:
                suggestions[target] = candidate
                used_sources.add(candidate)
                break
        if target in suggestions:
            continue
        for norm, original in normalized_to_original.items():
            if original in used_sources:
                continue
            if any(normalized_label(alias) in norm or norm in normalized_label(alias) for alias in aliases):
                suggestions[target] = original
                used_sources.add(original)
                break
    return suggestions


def _read_table(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {'.csv', '.txt'}:
        return pd.read_csv(path)
    if suffix in {'.xlsx', '.xlsm', '.xltx', '.xltm', '.xls'}:
        return pd.read_excel(path, sheet_name=sheet_name)
    raise ValueError(f'Format non supporté pour le geocoder: {path.suffix}')


def inspect_geocoder_file(uploaded_file) -> dict:
    suffix = Path(uploaded_file.name).suffix.lower()
    if suffix in {'.xlsx', '.xlsm', '.xltx', '.xltm', '.xls'}:
        xls = pd.ExcelFile(uploaded_file)
        sheets = []
        for name in xls.sheet_names[:10]:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, sheet_name=name, nrows=20)
            columns = [str(col) for col in df.columns]
            suggestions = suggest_column_mapping(columns)
            sheets.append({
                'name': name,
                'max_row': None,
                'max_column': len(columns),
                'preview': [columns] + df.head(20).fillna('').astype(str).values.tolist(),
                'detected_columns': columns,
                'mapping_suggestions': suggestions,
                'missing_required': sorted(GEOCODER_REQUIRED_FIELDS - set(suggestions.keys())),
            })
        return {'filename': Path(uploaded_file.name).name, 'kind': 'excel', 'sheets': sheets}
    if suffix in {'.csv', '.txt'}:
        df = pd.read_csv(uploaded_file, nrows=20)
        columns = [str(col) for col in df.columns]
        suggestions = suggest_column_mapping(columns)
        return {
            'filename': Path(uploaded_file.name).name,
            'kind': 'csv',
            'sheets': [{
                'name': '__csv__',
                'max_row': None,
                'max_column': len(columns),
                'preview': [columns] + df.head(20).fillna('').astype(str).values.tolist(),
                'detected_columns': columns,
                'mapping_suggestions': suggestions,
                'missing_required': sorted(GEOCODER_REQUIRED_FIELDS - set(suggestions.keys())),
            }],
        }
    raise ValueError('Inspection geocoder disponible uniquement pour CSV et Excel.')


def _is_valid_coord(value, is_lat=True) -> bool:
    try:
        val = float(value)
    except Exception:
        return False
    return (-90 <= val <= 90) if is_lat else (-180 <= val <= 180)


def _clean_zip(zipcode) -> str:
    if pd.isna(zipcode):
        return ''
    return ''.join(ch for ch in str(zipcode).split('.')[0] if ch.isalnum())


def _clean_country(country) -> str:
    value = str(country or '').strip().upper()
    return GEOCODER_COUNTRY_ALIASES.get(value, value)


def _full_query(row: pd.Series, country_hint: str = '') -> str:
    country = _clean_country(row.get('country') or country_hint)
    values = {
        'name': str(row.get('name', '')).strip(),
        'address': str(row.get('address', '')).strip(),
        'zipcode': _clean_zip(row.get('zipcode', '')),
        'city': str(row.get('city', '')).strip(),
        'country': country.strip(),
    }
    parts = [values.get(key, '') for key in GEOCODER_QUERY_ORDER]
    return ', '.join([part for part in parts if part])


class GeocodeCache:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self):
        return sqlite3.connect(str(self.db_path))

    def _ensure_schema(self):
        with self._connect() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS geocode_cache (
                    cache_key TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )

    def get(self, cache_key: str):
        with self._connect() as conn:
            row = conn.execute('SELECT payload FROM geocode_cache WHERE cache_key = ?', (cache_key,)).fetchone()
        return json.loads(row[0]) if row else None

    def set(self, cache_key: str, provider: str, payload: dict):
        with self._connect() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO geocode_cache (cache_key, provider, payload) VALUES (?, ?, ?)',
                (cache_key, provider, json.dumps(payload, ensure_ascii=False)),
            )
            conn.commit()


class GeocodeCheckpoint:
    def __init__(self, checkpoint_path: Path):
        self.path = checkpoint_path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def save(self, payload: dict[str, dict]) -> None:
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


@dataclass
class GeocoderOptions:
    provider: str = GEOCODER_DEFAULT_PROVIDER
    geocoder_sheet_name: str | None = None
    geocoder_mapping: dict[str, str] = field(default_factory=dict)
    country_hint: str = ''
    user_agent: str = GEOCODER_DEFAULT_USER_AGENT
    cache_db_path: Path | None = None
    checkpoint_every: int = GEOCODER_CHECKPOINT_EVERY
    resume_enabled: bool = GEOCODER_RESUME_ENABLED


class GeocoderService:
    def __init__(self, progress_callback: ProgressCallback | None = None, log_callback: LogCallback | None = None):
        self.progress_callback = progress_callback or _noop_progress
        self.log_callback = log_callback or _noop_log

    def progress(self, percent: int, message: str):
        self.progress_callback(percent, message)

    def log(self, message: str):
        self.log_callback(message)

    def run(self, input_path: Path, output_path: Path, options: GeocoderOptions) -> Path:
        self.progress(5, 'Chargement du fichier source du geocoder')
        df = _read_table(input_path, options.geocoder_sheet_name)
        self.log(f'📘 Geocoder source : {input_path.name} - {len(df)} lignes')
        df = self._apply_mapping(df, options.geocoder_mapping)
        self.progress(15, 'Préparation des colonnes, du cache et du checkpoint')
        cache = GeocodeCache(options.cache_db_path or (output_path.parent / GEOCODER_CACHE_FILENAME))
        checkpoint = GeocodeCheckpoint(output_path.with_name(output_path.stem + GEOCODER_CHECKPOINT_SUFFIX))
        checkpoint_data = checkpoint.load() if options.resume_enabled else {}
        if checkpoint_data:
            self.log(f'♻️ Checkpoint détecté : {len(checkpoint_data)} lignes déjà résolues ou tentées')
        geocode_fn = None
        if options.provider == 'existing_or_nominatim':
            geolocator = Nominatim(user_agent=options.user_agent, timeout=GEOCODER_TIMEOUT_SECONDS)
            geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=GEOCODER_MIN_DELAY_SECONDS, swallow_exceptions=False)
            self.log('🌍 Provider actif : existing_or_nominatim (réutilise les coordonnées présentes, puis tente Nominatim)')
        else:
            self.log('📌 Provider actif : existing_only (réutilise uniquement les coordonnées existantes)')

        rows = []
        total = max(len(df), 1)
        resolved_existing = 0
        resolved_remote = 0
        unresolved = 0
        cache_hits = 0
        resumed_rows = 0
        for index, (_, row) in enumerate(df.iterrows(), start=1):
            base = {field: row.get(field, '') for field in GEOCODER_MAPPING_FIELDS if field in row.index}
            row_key = str(base.get('id') or index)
            if row_key in checkpoint_data:
                result = checkpoint_data[row_key]
                resumed_rows += 1
            else:
                result = self._resolve_row(base, cache, geocode_fn, options)
                checkpoint_data[row_key] = result
            if result['geocoder_status'] == 'resolved_existing':
                resolved_existing += 1
            elif result['geocoder_status'] in {'resolved_nominatim', 'resolved_cache'}:
                if result['geocoder_status'] == 'resolved_cache':
                    cache_hits += 1
                else:
                    resolved_remote += 1
            else:
                unresolved += 1
            rows.append({**base, **result})
            if index % max(options.checkpoint_every, 1) == 0 or index == total:
                checkpoint.save(checkpoint_data)
            if index == 1 or index % 25 == 0 or index == total:
                pct = 15 + int(index / total * 75)
                self.progress(min(pct, 92), f'Geocoding en cours : {index}/{total}')
        self.progress(94, 'Écriture du CSV geocoder')
        out_df = pd.DataFrame(rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        summary_path = output_path.with_name(output_path.stem + '_summary.json')
        summary = {
            'rows': int(len(out_df)),
            'resolved_existing': int(resolved_existing),
            'resolved_remote': int(resolved_remote),
            'cache_hits': int(cache_hits),
            'unresolved': int(unresolved),
            'resumed_rows': int(resumed_rows),
            'provider': options.provider,
            'country_hint': options.country_hint,
        }
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        self.log(f'✅ Lignes traitées : {len(out_df)} | existing={resolved_existing} | nominatim={resolved_remote} | cache={cache_hits} | resumed={resumed_rows} | unresolved={unresolved}')
        self.log(f'🧾 Summary geocoder : {summary_path.name}')
        self.progress(100, 'Geocoder terminé')
        return output_path

    def _apply_mapping(self, df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        df = df.copy()
        missing_mapping_sources = [source for source in mapping.values() if source not in df.columns]
        if missing_mapping_sources:
            raise ValueError(f'Le fichier geocoder ne contient pas certaines colonnes mappées: {", ".join(sorted(set(missing_mapping_sources)))}')
        reverse = {source: target for target, source in mapping.items() if source in df.columns}
        df = df.rename(columns=reverse)
        for field in GEOCODER_MAPPING_FIELDS:
            if field not in df.columns:
                df[field] = ''
        for field in ['id', 'name', 'address', 'zipcode', 'city', 'country', 'phone', 'email', 'website', 'hexa', 'legal_id']:
            df[field] = df[field].fillna('').astype(str)
        return df

    def _resolve_row(self, base: dict, cache: GeocodeCache, geocode_fn, options: GeocoderOptions) -> dict:
        if _is_valid_coord(base.get('lat'), True) and _is_valid_coord(base.get('lng'), False):
            return {
                'lat': base.get('lat'), 'lng': base.get('lng'), 'geocoder_status': 'resolved_existing',
                'geocoder_source': 'input', 'geocoder_query': '', 'geocoder_label': '',
            }
        query = _full_query(pd.Series(base), options.country_hint)
        cache_key = hashlib.sha256(f'{options.provider}|{query}'.encode('utf-8')).hexdigest()
        cached = cache.get(cache_key)
        if cached:
            return cached | {'geocoder_status': 'resolved_cache'}
        unresolved_payload = {
            'lat': '', 'lng': '', 'geocoder_status': 'unresolved',
            'geocoder_source': options.provider, 'geocoder_query': query, 'geocoder_label': '',
        }
        if not query or options.provider == 'existing_only' or geocode_fn is None:
            return unresolved_payload
        try:
            location = geocode_fn(query)
        except (GeocoderTimedOut, GeocoderServiceError, Exception):
            return unresolved_payload
        if not location:
            return unresolved_payload
        payload = {
            'lat': location.latitude,
            'lng': location.longitude,
            'geocoder_status': 'resolved_nominatim',
            'geocoder_source': GEOCODER_PROVIDER_SOURCE_LABEL,
            'geocoder_query': query,
            'geocoder_label': getattr(location, 'address', ''),
        }
        cache.set(cache_key, options.provider, payload)
        return payload

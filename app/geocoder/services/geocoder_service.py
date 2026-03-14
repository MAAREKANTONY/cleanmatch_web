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

ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]

GEOCODER_MAPPING_FIELDS = [
    'id', 'name', 'address', 'zipcode', 'city',
    'lat', 'lng', 'phone', 'email', 'website', 'country',
]
GEOCODER_REQUIRED_FIELDS = {'id', 'address', 'zipcode', 'city'}
GEOCODER_COLUMN_ALIASES = {
    'id': ['id', 'identifier', 'identifiant', 'outlet_id', 'store_id', 'restaurant_id'],
    'name': ['name', 'nom', 'enseigne', 'raison sociale', 'outlet_name', 'store_name'],
    'address': ['address', 'adresse', 'adresse1', 'street', 'rue', 'full_address'],
    'zipcode': ['zipcode', 'zip', 'postal_code', 'code_postal', 'cp', 'post_code'],
    'city': ['city', 'ville', 'commune', 'town'],
    'lat': ['lat', 'latitude'],
    'lng': ['lng', 'lon', 'long', 'longitude'],
    'phone': ['phone', 'telephone', 'tel', 'mobile', 'cellular'],
    'email': ['email', 'mail', 'e-mail'],
    'website': ['website', 'url', 'web', 'site'],
    'country': ['country', 'pays', 'country_code'],
}


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


def _full_query(row: pd.Series, country_hint: str = '') -> str:
    parts = [str(row.get('address', '')).strip(), _clean_zip(row.get('zipcode', '')), str(row.get('city', '')).strip(), country_hint.strip()]
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


@dataclass
class GeocoderOptions:
    provider: str = 'existing_or_nominatim'
    geocoder_sheet_name: str | None = None
    geocoder_mapping: dict[str, str] = field(default_factory=dict)
    country_hint: str = ''
    user_agent: str = 'cleanmatch-web'
    cache_db_path: Path | None = None


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
        self.progress(15, 'Préparation des colonnes et du cache')
        cache = GeocodeCache(options.cache_db_path or (output_path.parent / 'geocode_cache_web.sqlite3'))
        geolocator = None
        geocode_fn = None
        if options.provider == 'existing_or_nominatim':
            geolocator = Nominatim(user_agent=options.user_agent, timeout=10)
            geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=False)
            self.log('🌍 Provider actif : existing_or_nominatim (réutilise les coordonnées présentes, puis tente Nominatim)')
        else:
            self.log('📌 Provider actif : existing_only (réutilise uniquement les coordonnées existantes)')

        rows = []
        total = max(len(df), 1)
        resolved_existing = 0
        resolved_remote = 0
        unresolved = 0
        cache_hits = 0
        for index, (_, row) in enumerate(df.iterrows(), start=1):
            base = {field: row.get(field, '') for field in GEOCODER_MAPPING_FIELDS if field in row.index}
            result = self._resolve_row(base, cache, geocode_fn, options)
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
            if index == 1 or index % 25 == 0 or index == total:
                pct = 15 + int(index / total * 75)
                self.progress(min(pct, 92), f'Geocoding en cours : {index}/{total}')
        self.progress(94, 'Écriture du CSV geocoder')
        out_df = pd.DataFrame(rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        self.log(f'✅ Lignes traitées : {len(out_df)} | existing={resolved_existing} | nominatim={resolved_remote} | cache={cache_hits} | unresolved={unresolved}')
        self.progress(100, 'Geocoder terminé')
        return output_path

    def _apply_mapping(self, df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        mapped = pd.DataFrame(index=df.index)
        for canonical, source in mapping.items():
            if source in df.columns:
                mapped[canonical] = df[source]
        for field in GEOCODER_MAPPING_FIELDS:
            if field not in mapped.columns:
                mapped[field] = ''
        return mapped.fillna('')

    def _resolve_row(self, base: dict, cache: GeocodeCache, geocode_fn, options: GeocoderOptions) -> dict:
        lat = base.get('lat', '')
        lng = base.get('lng', '')
        if _is_valid_coord(lat, True) and _is_valid_coord(lng, False):
            return {
                'lat': float(lat), 'lng': float(lng), 'geocoder_status': 'resolved_existing',
                'geocoder_source': 'input', 'geocoder_query': '', 'geocoder_label': '',
            }
        query = _full_query(pd.Series(base), options.country_hint)
        cache_key = hashlib.sha256(f'{options.provider}|{query}'.encode('utf-8')).hexdigest()
        cached = cache.get(cache_key)
        if cached:
            return cached
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
            'geocoder_source': 'nominatim',
            'geocoder_query': query,
            'geocoder_label': getattr(location, 'address', ''),
        }
        cache.set(cache_key, options.provider, payload)
        return payload

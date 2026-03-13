from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd
from rapidfuzz import fuzz
from slugify import slugify

from normalizer.services.normalizer_service import detect_num_voie, detect_voie, make_matchcode

ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]

MATCHER_MAPPING_FIELDS = [
    'id', 'name', 'address', 'zipcode', 'city',
    'voie', 'num_voie', 'matchcode',
    'phone', 'cellular', 'siret', 'hexa', 'lat', 'lng',
]
MATCHER_REQUIRED_FIELDS = {'id', 'name', 'address', 'zipcode', 'city'}
MATCHER_COLUMN_ALIASES = {
    'id': ['id', 'identifier', 'identifiant', 'outlet_id', 'store_id', 'restaurant_id'],
    'name': ['name', 'nom', 'enseigne', 'raison sociale', 'outlet_name', 'store_name'],
    'address': ['address', 'adresse', 'adresse1', 'street', 'rue', 'full_address'],
    'zipcode': ['zipcode', 'zip', 'postal_code', 'code_postal', 'cp', 'post_code'],
    'city': ['city', 'ville', 'commune', 'town'],
    'voie': ['voie', 'street_name', 'road_name'],
    'num_voie': ['num_voie', 'street_number', 'house_number', 'numero'],
    'matchcode': ['matchcode'],
    'phone': ['phone', 'telephone', 'tel'],
    'cellular': ['cellular', 'mobile', 'mobile_phone'],
    'siret': ['siret'],
    'hexa': ['hexa', 'hexa_gmap', 'hexa_code'],
    'lat': ['lat', 'latitude'],
    'lng': ['lng', 'lon', 'long', 'longitude'],
}

FRENCH_STOP_WORDS = {
    'le', 'la', 'les', 'du', 'des', 'au', 'aux', 'de', 'et', 'a', 'l', 'd', 'un', 'une', 'en', 'dans', 'sur',
    'pour', 'par', 'avec', 'ce', 'ces', 'cette', 'cet', 'sans', 'ne', 'pas', 'plus', 'que', 'qui', 'quoi',
    'ou', 'donc', 'car', 'bar', 'restaurant', 'hotel', 'brasserie', 'camping', 'cafe', 'boulangerie',
    'patisserie', 'pizzeria', 'tabac', 'presse', 'boucherie', 'charcuterie', 'epicerie', 'pharmacie',
    'garage', 'salon', 'coiffure', 'karaoke', 'discotheque', 'cinema', 'cine', 'disco', 'creperie', 'pub',
    'hotels', 'bistrot', 'pizza', 'crepe', 'sandwich', 'bowling', 'billard', 'club', 'sarl', 'eurl', 'sas',
    'sasu', 'sa', 'snc', 'sci', 'gaec', 'entreprise', 'societe', 'etablissements', 'ets', 'cie', 'groupe',
    'maison', 'chez', 'association'
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
    for target, aliases in MATCHER_COLUMN_ALIASES.items():
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
    raise ValueError(f'Format non supporté pour le matcher: {path.suffix}')


def inspect_table_file(uploaded_file, sheet_name: str | None = None) -> dict:
    suffix = Path(uploaded_file.name).suffix.lower()
    if suffix in {'.xlsx', '.xlsm', '.xltx', '.xltm', '.xls'}:
        xls = pd.ExcelFile(uploaded_file)
        sheets = []
        for name in xls.sheet_names[:10]:
            df = pd.read_excel(uploaded_file, sheet_name=name, nrows=20)
            columns = [str(col) for col in df.columns]
            sheets.append({
                'name': name,
                'max_row': None,
                'max_column': len(columns),
                'preview': df.head(20).fillna('').astype(str).values.tolist(),
                'detected_columns': columns,
                'mapping_suggestions': suggest_column_mapping(columns),
                'missing_required': sorted(MATCHER_REQUIRED_FIELDS - set(suggest_column_mapping(columns).keys())),
            })
        return {'filename': Path(uploaded_file.name).name, 'kind': 'excel', 'sheets': sheets}

    if suffix in {'.csv', '.txt'}:
        df = pd.read_csv(uploaded_file, nrows=20)
        columns = [str(col) for col in df.columns]
        return {
            'filename': Path(uploaded_file.name).name,
            'kind': 'csv',
            'sheets': [{
                'name': '__csv__',
                'max_row': None,
                'max_column': len(columns),
                'preview': df.head(20).fillna('').astype(str).values.tolist(),
                'detected_columns': columns,
                'mapping_suggestions': suggest_column_mapping(columns),
                'missing_required': sorted(MATCHER_REQUIRED_FIELDS - set(suggest_column_mapping(columns).keys())),
            }],
        }

    raise ValueError('Inspection matcher disponible uniquement pour CSV et Excel.')


def clean_zipcode(zipcode) -> str:
    if pd.isna(zipcode):
        return ''
    zip_str = str(zipcode).split('.')[0]
    return ''.join(ch for ch in zip_str if ch.isalnum()).lower()


def clean_phone_number(phone) -> str:
    if pd.isna(phone) or phone == '':
        return ''
    s_phone = str(phone)
    if '.' in s_phone:
        s_phone = s_phone.split('.')[0]
    clean = ''.join(ch for ch in s_phone if ch.isdigit())
    if clean.startswith('33') and len(clean) == 11:
        clean = '0' + clean[2:]
    return clean if len(clean) >= 6 else ''


def clean_name_for_matching(name, city) -> str:
    name_slug = slugify(str(name or ''), separator=' ')
    city_words = set(slugify(str(city or ''), separator=' ').split())
    words = [w for w in name_slug.split() if w not in FRENCH_STOP_WORDS and w not in city_words]
    final_name = ' '.join(words).strip()
    return final_name or name_slug


def haversine_meters(lat1, lon1, lat2, lon2):
    try:
        lat1 = float(lat1); lon1 = float(lon1); lat2 = float(lat2); lon2 = float(lon2)
    except Exception:
        return math.nan
    r = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@dataclass
class MatcherOptions:
    threshold_name: int = 85
    threshold_voie: int = 70
    top_k_per_master: int = 5
    master_sheet_name: str | None = None
    slave_sheet_name: str | None = None
    master_mapping: dict[str, str] = field(default_factory=dict)
    slave_mapping: dict[str, str] = field(default_factory=dict)


class MatcherService:
    def __init__(self, progress_callback: ProgressCallback | None = None, log_callback: LogCallback | None = None):
        self.progress_callback = progress_callback or _noop_progress
        self.log_callback = log_callback or _noop_log

    def progress(self, percent: int, message: str) -> None:
        self.progress_callback(percent, message)

    def log(self, message: str) -> None:
        self.log_callback(message)

    def run(self, master_path: Path, slave_path: Path, output_path: Path, options: MatcherOptions) -> Path:
        self.progress(5, 'Chargement des fichiers master et slave')
        df_master = _read_table(master_path, options.master_sheet_name)
        df_slave = _read_table(slave_path, options.slave_sheet_name)
        self.log(f'📘 Master: {master_path.name} - {len(df_master)} lignes')
        self.log(f'📗 Slave: {slave_path.name} - {len(df_slave)} lignes')

        self.progress(15, 'Application du mapping et préparation des colonnes')
        df_master = self._apply_mapping(df_master, options.master_mapping, side='master')
        df_slave = self._apply_mapping(df_slave, options.slave_mapping, side='slave')

        self.progress(25, 'Enrichissement des colonnes dérivées')
        df_master = self._prepare_dataframe(df_master)
        df_slave = self._prepare_dataframe(df_slave)

        self.progress(35, 'Déduplication et indexation')
        df_master = df_master.drop_duplicates(subset=['id'], keep='first').copy()
        df_slave = df_slave.drop_duplicates(subset=['id'], keep='first').copy()
        slave_by_zip = {zipc: grp.copy() for zipc, grp in df_slave.groupby('zipcode_clean') if zipc}
        slave_by_city = {city: grp.copy() for city, grp in df_slave.groupby('city_clean') if city}
        self.log(f'🧭 Groupes slave indexés: {len(slave_by_zip)} zipcodes, {len(slave_by_city)} villes')

        results = []
        total = max(len(df_master), 1)
        for idx, (_, master_row) in enumerate(df_master.iterrows(), start=1):
            candidates = self._get_candidates(master_row, df_slave, slave_by_zip, slave_by_city)
            if candidates.empty:
                continue
            matches = self._score_candidates(master_row, candidates, options)
            if matches:
                results.extend(matches[: options.top_k_per_master])
            if idx == 1 or idx % 100 == 0 or idx == total:
                pct = 35 + int((idx / total) * 55)
                self.progress(min(pct, 90), f'Matching en cours: {idx}/{total} master')

        self.progress(92, 'Construction du fichier résultat')
        result_df = pd.DataFrame(results)
        if result_df.empty:
            result_df = pd.DataFrame(columns=[
                'master_id', 'master_name', 'slave_id', 'slave_name', 'score_name', 'score_voie', 'score_city',
                'score_phone', 'same_matchcode', 'same_hexa', 'same_siret', 'distance_meters', 'automatch', 'match_method'
            ])
        result_df = result_df.sort_values(by=['automatch', 'score_name', 'score_voie', 'score_city'], ascending=[False, False, False, False])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        self.log(f'✅ Sortie matcher écrite: {output_path.name} ({len(result_df)} correspondances)')
        self.progress(100, 'Matcher terminé')
        return output_path

    def _apply_mapping(self, df: pd.DataFrame, mapping: dict[str, str], side: str) -> pd.DataFrame:
        df = df.copy()
        reverse = {source: target for target, source in mapping.items() if source in df.columns}
        df = df.rename(columns=reverse)
        for field in MATCHER_MAPPING_FIELDS:
            if field not in df.columns:
                df[field] = ''
        missing = [field for field in MATCHER_REQUIRED_FIELDS if field not in reverse.values() and field not in df.columns]
        if missing:
            raise ValueError(f'Le fichier {side} ne contient pas les colonnes requises après mapping: {", ".join(sorted(missing))}')
        return df

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for field in MATCHER_REQUIRED_FIELDS:
            if field not in df.columns:
                raise ValueError(f'Colonne requise absente: {field}')
        df['zipcode_clean'] = df['zipcode'].apply(clean_zipcode)
        df['city_clean'] = df['city'].apply(lambda value: slugify(str(value or ''), separator=' '))
        df['name_clean'] = df.apply(lambda row: clean_name_for_matching(row.get('name', ''), row.get('city', '')), axis=1)
        df['voie'] = df.apply(lambda row: row.get('voie') or detect_voie(row.get('address')), axis=1)
        df['num_voie'] = df.apply(lambda row: row.get('num_voie') or detect_num_voie(row.get('address')), axis=1)
        df['matchcode'] = df.apply(lambda row: row.get('matchcode') or make_matchcode(row.get('address'), row.get('zipcode')), axis=1)
        df['phone_clean'] = df['phone'].apply(clean_phone_number)
        df['cellular_clean'] = df['cellular'].apply(clean_phone_number)
        return df

    def _get_candidates(self, master_row: pd.Series, df_slave: pd.DataFrame, slave_by_zip: dict[str, pd.DataFrame], slave_by_city: dict[str, pd.DataFrame]) -> pd.DataFrame:
        zipcode = master_row.get('zipcode_clean', '')
        city = master_row.get('city_clean', '')
        if zipcode and zipcode in slave_by_zip:
            return slave_by_zip[zipcode]
        if city and city in slave_by_city:
            return slave_by_city[city]
        return df_slave.head(5000)

    def _score_candidates(self, master_row: pd.Series, candidates: pd.DataFrame, options: MatcherOptions) -> list[dict]:
        out = []
        master_name = str(master_row.get('name', ''))
        master_city = str(master_row.get('city', ''))
        master_voie = slugify(str(master_row.get('voie', '') or ''), separator=' ')
        master_phone_raw = master_row.get('phone', '')
        master_cell_raw = master_row.get('cellular', '')
        master_phones = [p for p in [master_row.get('phone_clean', ''), master_row.get('cellular_clean', '')] if p]
        master_matchcode = master_row.get('matchcode')
        master_hexa = master_row.get('hexa')
        master_siret = str(master_row.get('siret', '')).strip()

        for _, slave_row in candidates.iterrows():
            score_name = fuzz.token_sort_ratio(master_row.get('name_clean', ''), slave_row.get('name_clean', ''))
            phone_scores = []
            for mp in master_phones:
                for sp in [slave_row.get('phone_clean', ''), slave_row.get('cellular_clean', '')]:
                    if mp and sp:
                        phone_scores.append(fuzz.ratio(mp, sp))
            score_phone = max(phone_scores) if phone_scores else 0
            if score_name < 50 and score_phone < 100:
                continue

            score_voie = fuzz.token_set_ratio(master_voie, slugify(str(slave_row.get('voie', '') or ''), separator=' '))
            score_city = fuzz.token_set_ratio(slugify(master_city, separator=' '), slugify(str(slave_row.get('city', '')), separator=' '))
            same_matchcode = bool(master_matchcode and slave_row.get('matchcode') and master_matchcode == slave_row.get('matchcode'))
            same_hexa = bool(master_hexa and slave_row.get('hexa') and str(master_hexa) == str(slave_row.get('hexa')))
            same_siret = bool(master_siret and str(slave_row.get('siret', '')).strip() and master_siret == str(slave_row.get('siret', '')).strip())

            distance = math.nan
            if str(master_row.get('lat', '')).strip() and str(master_row.get('lng', '')).strip() and str(slave_row.get('lat', '')).strip() and str(slave_row.get('lng', '')).strip():
                distance = haversine_meters(master_row.get('lat'), master_row.get('lng'), slave_row.get('lat'), slave_row.get('lng'))

            automatch = 0
            method = ''
            if same_matchcode and score_name >= options.threshold_name:
                automatch = 1; method = 'matchcode_name'
            elif score_name == 100 and score_voie >= options.threshold_voie:
                automatch = 1; method = 'algo_score'
            elif score_name >= options.threshold_name and score_voie >= 80:
                automatch = 1; method = 'algo_score'
            elif same_hexa:
                automatch = 1; method = 'hexa_match'
            elif score_phone == 100 and (master_row.get('zipcode_clean') == slave_row.get('zipcode_clean') or (not math.isnan(distance) and distance <= 50)):
                automatch = 1; method = 'phone_match'
            elif same_siret:
                automatch = 1; method = 'siret_match'

            out.append({
                'master_id': master_row.get('id'),
                'master_name': master_name,
                'master_address': master_row.get('address', ''),
                'master_zipcode': master_row.get('zipcode', ''),
                'master_city': master_city,
                'slave_id': slave_row.get('id'),
                'slave_name': slave_row.get('name', ''),
                'slave_address': slave_row.get('address', ''),
                'slave_zipcode': slave_row.get('zipcode', ''),
                'slave_city': slave_row.get('city', ''),
                'score_name': round(score_name, 1),
                'score_voie': round(score_voie, 1),
                'score_city': round(score_city, 1),
                'score_phone': round(score_phone, 1),
                'same_matchcode': same_matchcode,
                'same_hexa': same_hexa,
                'same_siret': same_siret,
                'distance_meters': None if math.isnan(distance) else round(distance, 2),
                'automatch': automatch,
                'match_method': method,
            })
        out.sort(key=lambda item: (item['automatch'], item['score_name'], item['score_voie'], item['score_city'], item['score_phone']), reverse=True)
        return out

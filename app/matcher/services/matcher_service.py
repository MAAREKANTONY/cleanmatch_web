from __future__ import annotations

import json
import math
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd
from rapidfuzz import fuzz
from slugify import slugify

from normalizer.services.normalizer_service import (
    country_profile,
    detect_num_voie,
    detect_voie,
    infer_legal_id_type,
    make_matchcode,
    normalize_country_code,
    normalize_legal_id,
)

ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]

MATCHER_MAPPING_FIELDS = [
    'id', 'name', 'address', 'zipcode', 'city', 'country', 'legal_id', 'legal_id_type',
    'voie', 'num_voie', 'matchcode',
    'phone', 'cellular', 'siret', 'hexa', 'lat', 'lng',
]
MATCHER_REQUIRED_FIELDS = {'id', 'name', 'address', 'zipcode', 'city'}
MATCHER_COLUMN_ALIASES = {
    'id': ['id', 'identifier', 'identifiant', 'outlet_id', 'store_id', 'restaurant_id'],
    'name': ['name', 'nom', 'enseigne', 'raison sociale', 'outlet_name', 'store_name', 'ragione sociale'],
    'address': ['address', 'adresse', 'adresse1', 'street', 'rue', 'full_address', 'final_address', 'legaladdress', 'legal_address', 'indirizzo', 'direccion', 'dirección', 'strasse', 'straat'],
    'zipcode': ['zipcode', 'zip', 'postal_code', 'code_postal', 'cp', 'post_code', 'cap', 'postcode', 'plz', 'legalzipcode', 'legal_zipcode', 'legalpostalcode', 'legal_postal_code'],
    'city': ['city', 'ville', 'commune', 'town', 'locality', 'citta', 'città', 'ciudad', 'stadt', 'gemeente', 'legalcity', 'legal_city'],
    'country': ['country', 'pays', 'country_code', 'nation', 'paese', 'pais', 'país', 'land'],
    'legal_id': ['legal_id', 'siret', 'siren', 'vat', 'vat_number', 'partita_iva', 'piva', 'codice_fiscale', 'nif', 'cif', 'btw', 'kvk', 'company_number', 'ust_id', 'ustid'],
    'legal_id_type': ['legal_id_type', 'id_type', 'vat_type', 'company_id_type'],
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

COMMON_STOP_WORDS = {
    'le', 'la', 'les', 'du', 'des', 'au', 'aux', 'de', 'et', 'a', 'l', 'd', 'un', 'une', 'en', 'dans', 'sur',
    'pour', 'par', 'avec', 'ce', 'ces', 'cette', 'cet', 'sans', 'ne', 'pas', 'plus', 'que', 'qui', 'quoi',
    'ou', 'donc', 'car', 'bar', 'restaurant', 'hotel', 'brasserie', 'camping', 'cafe', 'boulangerie',
    'patisserie', 'pizzeria', 'tabac', 'presse', 'boucherie', 'charcuterie', 'epicerie', 'pharmacie',
    'garage', 'salon', 'coiffure', 'karaoke', 'discotheque', 'cinema', 'cine', 'disco', 'creperie', 'pub',
    'hotels', 'bistrot', 'pizza', 'crepe', 'sandwich', 'bowling', 'billard', 'club', 'maison', 'chez',
    'association', 'group', 'store', 'shop', 'outlet', 'retail'
}
LEGAL_STOP_WORDS = {
    'sarl', 'eurl', 'sas', 'sasu', 'sa', 'snc', 'sci', 'gaec', 'entreprise', 'societe', 'società', 'societa',
    'etablissements', 'ets', 'cie', 'groupe', 'srl', 'spa', 'sl', 'slu', 'gmbh', 'ag', 'ug', 'kg', 'ohg',
    'sprl', 'bv', 'nv', 'vof', 'ltd', 'limited', 'plc', 'llp', 'lda'
}
MATCHER_STOP_WORDS = COMMON_STOP_WORDS | LEGAL_STOP_WORDS

AUTO_REASONS = {
    'legal_id_match': 'Identifiant légal identique',
    'matchcode_name': 'Matchcode identique + nom fort',
    'algo_score': 'Nom + voie très proches',
    'hexa_match': 'Hexa identique',
    'phone_match': 'Téléphone identique + proximité',
    'siret_match': 'SIRET identique',
    'distance_match': 'Coordonnées très proches',
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
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, sheet_name=name, nrows=20)
            columns = [str(col) for col in df.columns]
            suggestions = suggest_column_mapping(columns)
            sheets.append({
                'name': name,
                'max_row': None,
                'max_column': len(columns),
                'preview': df.head(20).fillna('').astype(str).values.tolist(),
                'detected_columns': columns,
                'mapping_suggestions': suggestions,
                'missing_required': sorted(MATCHER_REQUIRED_FIELDS - set(suggestions.keys())),
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
                'preview': df.head(20).fillna('').astype(str).values.tolist(),
                'detected_columns': columns,
                'mapping_suggestions': suggestions,
                'missing_required': sorted(MATCHER_REQUIRED_FIELDS - set(suggestions.keys())),
            }],
        }

    raise ValueError('Inspection matcher disponible uniquement pour CSV et Excel.')


def clean_zipcode(zipcode, country_code: str | None = None) -> str:
    if pd.isna(zipcode):
        return ''
    zip_str = str(zipcode).split('.')[0].strip()
    if not zip_str:
        return ''
    cleaned = ''.join(ch for ch in zip_str.upper() if ch.isalnum())
    profile = country_profile(country_code)
    regex = profile.get('postcode_regex') or ''
    if regex:
        with_space = ' '.join(cleaned[i:i + 4] for i in range(0, len(cleaned), 4)).strip()
        if cleaned and (pd.notna(cleaned)):
            if country_code == 'NL' and len(cleaned) >= 6:
                with_space = f'{cleaned[:4]} {cleaned[4:6]}'
            if country_code == 'GB' and len(cleaned) >= 5:
                with_space = f'{cleaned[:-3]} {cleaned[-3:]}'
        import re
        if re.match(regex, cleaned, flags=re.IGNORECASE):
            return cleaned.lower()
        if re.match(regex, with_space, flags=re.IGNORECASE):
            return with_space.replace(' ', '').lower()
    return cleaned.lower()


def clean_phone_number(phone, country_code: str | None = None) -> str:
    if pd.isna(phone) or phone == '':
        return ''
    s_phone = str(phone)
    if '.' in s_phone:
        s_phone = s_phone.split('.')[0]
    clean = ''.join(ch for ch in s_phone if ch.isdigit())
    if clean.startswith('00'):
        clean = clean[2:]
    country_code = normalize_country_code(country_code) if country_code else ''
    if country_code == 'FR' and clean.startswith('33') and len(clean) == 11:
        clean = '0' + clean[2:]
    elif country_code == 'BE' and clean.startswith('32') and len(clean) >= 10:
        clean = '0' + clean[2:]
    elif country_code in {'ES', 'PT', 'IT'} and clean.startswith({'34':'34','351':'351','39':'39'}.get(country_code,'')):
        pass
    return clean if len(clean) >= 6 else ''


def clean_name_for_matching(name, city, country_code: str | None = None) -> str:
    name_slug = slugify(str(name or ''), separator=' ')
    city_words = set(slugify(str(city or ''), separator=' ').split())
    words = [w for w in name_slug.split() if w not in MATCHER_STOP_WORDS and w not in city_words]
    final_name = ' '.join(words).strip()
    return final_name or name_slug


def haversine_meters(lat1, lon1, lat2, lon2):
    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
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
    threshold_phone_review: int = 100
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
        self.log(f'🗺️ Mapping master: {json.dumps(options.master_mapping, ensure_ascii=False)}')
        self.log(f'🗺️ Mapping slave: {json.dumps(options.slave_mapping, ensure_ascii=False)}')

        self.progress(25, 'Enrichissement des colonnes dérivées')
        df_master = self._prepare_dataframe(df_master)
        df_slave = self._prepare_dataframe(df_slave)
        self.log(f"🌍 Pays master détectés: {sorted(set(x for x in df_master['country'].dropna().astype(str).tolist() if x))[:12]}")
        self.log(f"🌍 Pays slave détectés: {sorted(set(x for x in df_slave['country'].dropna().astype(str).tolist() if x))[:12]}")

        self.progress(35, 'Déduplication et indexation')
        df_master = df_master.drop_duplicates(subset=['id'], keep='first').copy()
        df_slave = df_slave.drop_duplicates(subset=['id'], keep='first').copy()
        slave_by_country_zip = {(country, zipc): grp.copy() for (country, zipc), grp in df_slave.groupby(['country', 'zipcode_clean']) if country and zipc}
        slave_by_country_city = {(country, city): grp.copy() for (country, city), grp in df_slave.groupby(['country', 'city_clean']) if country and city}
        slave_by_zip = {zipc: grp.copy() for zipc, grp in df_slave.groupby('zipcode_clean') if zipc}
        slave_by_city = {city: grp.copy() for city, grp in df_slave.groupby('city_clean') if city}
        slave_by_legal_id = {legal_id: grp.copy() for legal_id, grp in df_slave.groupby('legal_id_clean') if legal_id}
        self.log(f'🧭 Groupes slave indexés: {len(slave_by_country_zip)} pays+zip, {len(slave_by_country_city)} pays+ville, {len(slave_by_legal_id)} legal_id')

        all_matches: list[dict] = []
        candidate_source_stats = {'legal_id': 0, 'country_zipcode': 0, 'country_city': 0, 'zipcode': 0, 'city': 0, 'fallback': 0}
        strongest_reason_stats: dict[str, int] = {}
        country_pair_stats: dict[str, int] = {}
        review_rows = []
        unmatched_rows = []
        total = max(len(df_master), 1)
        for idx, (_, master_row) in enumerate(df_master.iterrows(), start=1):
            candidates, candidate_source = self._get_candidates(master_row, df_slave, slave_by_country_zip, slave_by_country_city, slave_by_zip, slave_by_city, slave_by_legal_id)
            candidate_source_stats[candidate_source] = candidate_source_stats.get(candidate_source, 0) + 1
            matches = self._score_candidates(master_row, candidates, options, candidate_source) if not candidates.empty else []
            classification = self._classify_master(master_row, matches, options)
            if classification['best_row'] is not None:
                strongest_reason = classification['best_row'].get('match_reason') or 'N/A'
                strongest_reason_stats[strongest_reason] = strongest_reason_stats.get(strongest_reason, 0) + 1
                pair_key = f"{classification['best_row'].get('master_country','')}->{classification['best_row'].get('slave_country','')}"
                country_pair_stats[pair_key] = country_pair_stats.get(pair_key, 0) + 1
                review_rows.append(classification['best_row']) if classification['best_row']['match_status'] == 'review' else None
            if classification['unmatched_row'] is not None:
                unmatched_rows.append(classification['unmatched_row'])
            if matches:
                all_matches.extend(matches[: options.top_k_per_master])
            if idx == 1 or idx % 100 == 0 or idx == total:
                pct = 35 + int((idx / total) * 50)
                self.progress(min(pct, 90), f'Matching en cours: {idx}/{total} master')

        self.progress(92, 'Construction des livrables Matcher V4')
        all_matches_df = pd.DataFrame(all_matches)
        if all_matches_df.empty:
            all_matches_df = pd.DataFrame(columns=self._result_columns())
        else:
            all_matches_df = all_matches_df.sort_values(
                by=['automatch', 'composite_score', 'score_legal_id', 'score_name', 'score_voie', 'score_city', 'score_phone'],
                ascending=[False, False, False, False, False, False, False],
            )

        review_df = pd.DataFrame(review_rows)
        if review_df.empty:
            review_df = pd.DataFrame(columns=self._review_columns())

        unmatched_df = pd.DataFrame(unmatched_rows)
        if unmatched_df.empty:
            unmatched_df = pd.DataFrame(columns=self._unmatched_columns())

        summary = {
            'master_rows': int(len(df_master)),
            'slave_rows': int(len(df_slave)),
            'candidate_pairs': int(len(all_matches_df)),
            'automatch_rows': int((all_matches_df['match_status'] == 'automatch').sum()) if 'match_status' in all_matches_df.columns else 0,
            'review_rows': int(len(review_df)),
            'unmatched_rows': int(len(unmatched_df)),
            'threshold_name': int(options.threshold_name),
            'threshold_voie': int(options.threshold_voie),
            'top_k_per_master': int(options.top_k_per_master),
            'candidate_source_stats': candidate_source_stats,
            'strongest_reason_stats': strongest_reason_stats,
            'country_pair_stats': country_pair_stats,
            'master_country_stats': df_master['country'].fillna('').astype(str).value_counts().to_dict(),
            'slave_country_stats': df_slave['country'].fillna('').astype(str).value_counts().to_dict(),
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix.lower() != '.zip':
            output_path = output_path.with_suffix('.zip')

        with tempfile.TemporaryDirectory(prefix='matcher_v4_') as tmpdir:
            tmpdir_path = Path(tmpdir)
            all_path = tmpdir_path / 'all_matches.csv'
            auto_path = tmpdir_path / 'automatch.csv'
            review_path = tmpdir_path / 'review.csv'
            unmatched_path = tmpdir_path / 'unmatched.csv'
            diagnostics_path = tmpdir_path / 'diagnostics.csv'
            summary_path = tmpdir_path / 'summary.json'

            all_matches_df.to_csv(all_path, index=False, encoding='utf-8-sig')
            all_matches_df[all_matches_df['match_status'] == 'automatch'].to_csv(auto_path, index=False, encoding='utf-8-sig')
            review_df.to_csv(review_path, index=False, encoding='utf-8-sig')
            unmatched_df.to_csv(unmatched_path, index=False, encoding='utf-8-sig')
            diagnostics_df = all_matches_df[[c for c in self._diagnostic_columns() if c in all_matches_df.columns]].copy()
            diagnostics_df.to_csv(diagnostics_path, index=False, encoding='utf-8-sig')
            summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')

            with zipfile.ZipFile(output_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(all_path, arcname='all_matches.csv')
                zf.write(auto_path, arcname='automatch.csv')
                zf.write(review_path, arcname='review.csv')
                zf.write(unmatched_path, arcname='unmatched.csv')
                zf.write(diagnostics_path, arcname='diagnostics.csv')
                zf.write(summary_path, arcname='summary.json')

        self.log(f"✅ Livrable ZIP écrit: {output_path.name}")
        self.log(f"📊 Résumé matcher: {summary['automatch_rows']} automatch, {summary['review_rows']} review, {summary['unmatched_rows']} unmatched")
        self.log(f"🧪 Sources de candidats: {candidate_source_stats}")
        self.progress(100, 'Matcher V4 terminé')
        return output_path

    def _safe_text_series(self, df: pd.DataFrame, column: str, default: str = '') -> pd.Series:
        if column not in df.columns:
            return pd.Series([default] * len(df), index=df.index, dtype='object')
        series = df[column]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        return series.fillna(default).astype(str)

    def _apply_mapping(self, df: pd.DataFrame, mapping: dict[str, str], side: str) -> pd.DataFrame:
        df = df.copy()
        missing_mapping_sources = [source for source in mapping.values() if source not in df.columns]
        if missing_mapping_sources:
            raise ValueError(f'Le fichier {side} ne contient pas certaines colonnes mappées: {", ".join(sorted(set(missing_mapping_sources)))}')
        reverse = {source: target for target, source in mapping.items() if source in df.columns}
        df = df.rename(columns=reverse)
        required_missing = [field for field in MATCHER_REQUIRED_FIELDS if field not in reverse.values() and field not in df.columns]
        if required_missing:
            raise ValueError(f'Le fichier {side} ne contient pas les colonnes requises après mapping: {", ".join(sorted(required_missing))}')
        for field in MATCHER_MAPPING_FIELDS:
            if field not in df.columns:
                df[field] = ''
        return df

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for field in MATCHER_REQUIRED_FIELDS:
            if field not in df.columns:
                raise ValueError(f'Colonne requise absente: {field}')
        df['country'] = self._safe_text_series(df, 'country').apply(normalize_country_code)
        df['zipcode'] = self._safe_text_series(df, 'zipcode')
        df['city'] = self._safe_text_series(df, 'city')
        df['name'] = self._safe_text_series(df, 'name')
        df['address'] = self._safe_text_series(df, 'address')
        df['phone'] = self._safe_text_series(df, 'phone')
        df['cellular'] = self._safe_text_series(df, 'cellular')
        df['legal_id'] = self._safe_text_series(df, 'legal_id')
        df['legal_id_type'] = self._safe_text_series(df, 'legal_id_type')
        siret_series = self._safe_text_series(df, 'siret')
        df['legal_id'] = df.apply(
            lambda row: normalize_legal_id(row.get('legal_id') or row.get('siret') or '', row.get('country')),
            axis=1,
        )
        df['legal_id_type'] = df.apply(
            lambda row: (str(row.get('legal_id_type') or '').strip() or infer_legal_id_type(row.get('legal_id') or row.get('siret') or '', row.get('country'))),
            axis=1,
        )
        df['zipcode_clean'] = df.apply(lambda row: clean_zipcode(row.get('zipcode'), row.get('country')), axis=1)
        df['city_clean'] = df['city'].apply(lambda value: slugify(str(value or ''), separator=' '))
        df['name_clean'] = df.apply(lambda row: clean_name_for_matching(row.get('name', ''), row.get('city', ''), row.get('country')), axis=1)
        df['voie'] = df.apply(lambda row: row.get('voie') or detect_voie(row.get('address'), row.get('country')), axis=1)
        df['num_voie'] = df.apply(lambda row: row.get('num_voie') or detect_num_voie(row.get('address'), row.get('country')), axis=1)
        df['matchcode'] = df.apply(lambda row: row.get('matchcode') or make_matchcode(row.get('address'), row.get('zipcode'), row.get('country')), axis=1)
        df['phone_clean'] = df.apply(lambda row: clean_phone_number(row.get('phone'), row.get('country')), axis=1)
        df['cellular_clean'] = df.apply(lambda row: clean_phone_number(row.get('cellular'), row.get('country')), axis=1)
        df['siret_clean'] = siret_series.apply(lambda value: normalize_legal_id(value, 'FR'))
        df['legal_id_clean'] = df['legal_id'].apply(lambda value: str(value or '').strip())
        return df

    def _get_candidates(
        self,
        master_row: pd.Series,
        df_slave: pd.DataFrame,
        slave_by_country_zip: dict[tuple[str, str], pd.DataFrame],
        slave_by_country_city: dict[tuple[str, str], pd.DataFrame],
        slave_by_zip: dict[str, pd.DataFrame],
        slave_by_city: dict[str, pd.DataFrame],
        slave_by_legal_id: dict[str, pd.DataFrame],
    ) -> tuple[pd.DataFrame, str]:
        country = str(master_row.get('country', '') or '')
        zipcode = str(master_row.get('zipcode_clean', '') or '')
        city = str(master_row.get('city_clean', '') or '')
        legal_id = str(master_row.get('legal_id_clean', '') or '')
        if legal_id and legal_id in slave_by_legal_id:
            return slave_by_legal_id[legal_id], 'legal_id'
        if country and zipcode and (country, zipcode) in slave_by_country_zip:
            return slave_by_country_zip[(country, zipcode)], 'country_zipcode'
        if country and city and (country, city) in slave_by_country_city:
            return slave_by_country_city[(country, city)], 'country_city'
        if zipcode and zipcode in slave_by_zip:
            return slave_by_zip[zipcode], 'zipcode'
        if city and city in slave_by_city:
            return slave_by_city[city], 'city'
        return df_slave.head(5000), 'fallback'

    def _score_candidates(self, master_row: pd.Series, candidates: pd.DataFrame, options: MatcherOptions, candidate_source: str) -> list[dict]:
        out = []
        master_name = str(master_row.get('name', ''))
        master_city = str(master_row.get('city', ''))
        master_country = str(master_row.get('country', ''))
        master_voie = slugify(str(master_row.get('voie', '') or ''), separator=' ')
        master_phones = [p for p in [master_row.get('phone_clean', ''), master_row.get('cellular_clean', '')] if p]
        master_matchcode = master_row.get('matchcode')
        master_hexa = master_row.get('hexa')
        master_siret = str(master_row.get('siret_clean', '')).strip()
        master_legal_id = str(master_row.get('legal_id_clean', '')).strip()
        master_legal_type = str(master_row.get('legal_id_type', '')).strip()

        for _, slave_row in candidates.iterrows():
            slave_country = str(slave_row.get('country', ''))
            country_match = bool(master_country and slave_country and master_country == slave_country)
            score_name = fuzz.token_sort_ratio(master_row.get('name_clean', ''), slave_row.get('name_clean', ''))
            phone_scores = []
            for mp in master_phones:
                for sp in [slave_row.get('phone_clean', ''), slave_row.get('cellular_clean', '')]:
                    if mp and sp:
                        phone_scores.append(fuzz.ratio(mp, sp))
            score_phone = max(phone_scores) if phone_scores else 0
            if score_name < 50 and score_phone < 100 and not (master_legal_id and master_legal_id == str(slave_row.get('legal_id_clean', '')).strip()):
                continue

            score_voie = fuzz.token_set_ratio(master_voie, slugify(str(slave_row.get('voie', '') or ''), separator=' '))
            score_city = fuzz.token_set_ratio(slugify(master_city, separator=' '), slugify(str(slave_row.get('city', '')), separator=' '))
            same_matchcode = bool(master_matchcode and slave_row.get('matchcode') and master_matchcode == slave_row.get('matchcode'))
            same_hexa = bool(master_hexa and slave_row.get('hexa') and str(master_hexa) == str(slave_row.get('hexa')))
            same_siret = bool(master_siret and str(slave_row.get('siret_clean', '')).strip() and master_siret == str(slave_row.get('siret_clean', '')).strip())
            same_legal_id = bool(master_legal_id and str(slave_row.get('legal_id_clean', '')).strip() and master_legal_id == str(slave_row.get('legal_id_clean', '')).strip())
            same_legal_id_type = bool(master_legal_type and str(slave_row.get('legal_id_type', '')).strip() and master_legal_type == str(slave_row.get('legal_id_type', '')).strip())
            score_legal_id = 100 if same_legal_id else 0

            distance = math.nan
            if str(master_row.get('lat', '')).strip() and str(master_row.get('lng', '')).strip() and str(slave_row.get('lat', '')).strip() and str(slave_row.get('lng', '')).strip():
                distance = haversine_meters(master_row.get('lat'), master_row.get('lng'), slave_row.get('lat'), slave_row.get('lng'))

            automatch = 0
            method = ''
            if same_legal_id and (country_match or not master_country or not slave_country):
                automatch = 1
                method = 'legal_id_match'
            elif same_matchcode and score_name >= options.threshold_name and (country_match or score_city >= 80):
                automatch = 1
                method = 'matchcode_name'
            elif score_name == 100 and score_voie >= options.threshold_voie and (country_match or score_city >= 80):
                automatch = 1
                method = 'algo_score'
            elif score_name >= options.threshold_name and score_voie >= 80 and (country_match or score_city >= 80):
                automatch = 1
                method = 'algo_score'
            elif same_hexa and (country_match or not master_country or not slave_country):
                automatch = 1
                method = 'hexa_match'
            elif score_phone == 100 and (master_row.get('zipcode_clean') == slave_row.get('zipcode_clean') or (not math.isnan(distance) and distance <= 50)) and (country_match or score_city >= 80):
                automatch = 1
                method = 'phone_match'
            elif same_siret:
                automatch = 1
                method = 'siret_match'
            elif not math.isnan(distance) and distance <= 15 and score_name >= 75 and (country_match or score_city >= 90):
                automatch = 1
                method = 'distance_match'

            country_bonus = 8 if country_match else -10 if (master_country and slave_country and master_country != slave_country) else 0
            legal_type_bonus = 4 if same_legal_id_type and same_legal_id else 0
            composite_score = round(
                (score_name * 0.36) + (score_voie * 0.18) + (score_city * 0.12) + (score_phone * 0.10) + score_legal_id * 0.14
                + (10 if same_matchcode else 0) + (8 if same_hexa else 0) + (10 if same_siret else 0) + country_bonus + legal_type_bonus,
                1,
            )
            match_status = 'automatch' if automatch else self._review_status(score_name, score_voie, score_city, score_phone, same_matchcode, same_hexa, same_siret, same_legal_id, country_match)
            reason = AUTO_REASONS.get(method, '') if automatch else self._review_reason(score_name, score_voie, score_city, score_phone, same_matchcode, same_hexa, same_siret, same_legal_id, country_match)

            out.append({
                'master_id': master_row.get('id'),
                'master_name': master_name,
                'master_address': master_row.get('address', ''),
                'master_zipcode': master_row.get('zipcode', ''),
                'master_city': master_city,
                'master_country': master_country,
                'master_legal_id': master_legal_id,
                'master_legal_id_type': master_legal_type,
                'slave_id': slave_row.get('id'),
                'slave_name': slave_row.get('name', ''),
                'slave_address': slave_row.get('address', ''),
                'slave_zipcode': slave_row.get('zipcode', ''),
                'slave_city': slave_row.get('city', ''),
                'slave_country': slave_country,
                'slave_legal_id': str(slave_row.get('legal_id_clean', '') or ''),
                'slave_legal_id_type': str(slave_row.get('legal_id_type', '') or ''),
                'score_name': round(score_name, 1),
                'score_voie': round(score_voie, 1),
                'score_city': round(score_city, 1),
                'score_phone': round(score_phone, 1),
                'score_legal_id': score_legal_id,
                'composite_score': composite_score,
                'same_matchcode': same_matchcode,
                'same_hexa': same_hexa,
                'same_siret': same_siret,
                'same_legal_id': same_legal_id,
                'same_legal_id_type': same_legal_id_type,
                'country_match': country_match,
                'distance_meters': None if math.isnan(distance) else round(distance, 2),
                'candidate_source': candidate_source,
                'automatch': automatch,
                'match_status': match_status,
                'match_method': method,
                'match_reason': reason,
            })
        out.sort(key=lambda item: (item['automatch'], item['composite_score'], item['score_legal_id'], item['score_name'], item['score_voie'], item['score_city'], item['score_phone']), reverse=True)
        for rank, row in enumerate(out, start=1):
            row['rank_for_master'] = rank
        return out

    def _review_status(self, score_name: float, score_voie: float, score_city: float, score_phone: float, same_matchcode: bool, same_hexa: bool, same_siret: bool, same_legal_id: bool, country_match: bool) -> str:
        if same_legal_id:
            return 'review'
        if same_siret or same_hexa:
            return 'review'
        if same_matchcode and score_name >= 70 and country_match:
            return 'review'
        if score_name >= 82 and score_voie >= 60 and (country_match or score_city >= 85):
            return 'review'
        if score_name >= 75 and score_voie >= 75 and score_city >= 80:
            return 'review'
        if score_phone >= 100 and (country_match or score_city >= 80):
            return 'review'
        return 'candidate'

    def _review_reason(self, score_name: float, score_voie: float, score_city: float, score_phone: float, same_matchcode: bool, same_hexa: bool, same_siret: bool, same_legal_id: bool, country_match: bool) -> str:
        reasons = []
        if same_legal_id:
            reasons.append('Identifiant légal identique')
        if same_siret:
            reasons.append('SIRET identique')
        if same_hexa:
            reasons.append('Hexa identique')
        if same_matchcode:
            reasons.append('Matchcode identique')
        if score_phone >= 100:
            reasons.append('Téléphone identique')
        if country_match:
            reasons.append('Pays identique')
        if score_name >= 80:
            reasons.append('Nom proche')
        if score_voie >= 70:
            reasons.append('Voie proche')
        if score_city >= 80:
            reasons.append('Ville proche')
        return ' + '.join(reasons[:5]) if reasons else 'À vérifier'

    def _classify_master(self, master_row: pd.Series, matches: list[dict], options: MatcherOptions) -> dict:
        if not matches:
            return {
                'best_row': None,
                'unmatched_row': {
                    'master_id': master_row.get('id'),
                    'master_name': master_row.get('name', ''),
                    'master_address': master_row.get('address', ''),
                    'master_zipcode': master_row.get('zipcode', ''),
                    'master_city': master_row.get('city', ''),
                    'master_country': master_row.get('country', ''),
                    'reason': 'Aucun candidat retenu',
                },
            }
        best = matches[0].copy()
        if best['match_status'] == 'candidate':
            return {
                'best_row': None,
                'unmatched_row': {
                    'master_id': master_row.get('id'),
                    'master_name': master_row.get('name', ''),
                    'master_address': master_row.get('address', ''),
                    'master_zipcode': master_row.get('zipcode', ''),
                    'master_city': master_row.get('city', ''),
                    'master_country': master_row.get('country', ''),
                    'reason': f"Meilleur candidat insuffisant (score composite {best['composite_score']})",
                },
            }
        return {'best_row': best, 'unmatched_row': None}

    @staticmethod
    def _result_columns() -> list[str]:
        return [
            'master_id', 'master_name', 'master_address', 'master_zipcode', 'master_city', 'master_country', 'master_legal_id', 'master_legal_id_type',
            'slave_id', 'slave_name', 'slave_address', 'slave_zipcode', 'slave_city', 'slave_country', 'slave_legal_id', 'slave_legal_id_type',
            'score_name', 'score_voie', 'score_city', 'score_phone', 'score_legal_id', 'composite_score',
            'same_matchcode', 'same_hexa', 'same_siret', 'same_legal_id', 'same_legal_id_type', 'country_match', 'distance_meters', 'candidate_source',
            'automatch', 'match_status', 'match_method', 'match_reason', 'rank_for_master'
        ]

    @staticmethod
    def _diagnostic_columns() -> list[str]:
        return [
            'master_id', 'slave_id', 'master_country', 'slave_country', 'match_status', 'match_reason', 'candidate_source',
            'score_name', 'score_voie', 'score_city', 'score_phone', 'score_legal_id', 'composite_score',
            'same_matchcode', 'same_hexa', 'same_siret', 'same_legal_id', 'same_legal_id_type', 'country_match', 'distance_meters', 'rank_for_master'
        ]

    @staticmethod
    def _review_columns() -> list[str]:
        return MatcherService._result_columns()

    @staticmethod
    def _unmatched_columns() -> list[str]:
        return ['master_id', 'master_name', 'master_address', 'master_zipcode', 'master_city', 'master_country', 'reason']

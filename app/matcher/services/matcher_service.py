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


AUTO_REASONS = {
    'matchcode_name': 'Matchcode identique + nom fort',
    'algo_score': 'Nom + voie très proches',
    'hexa_match': 'Hexa identique',
    'phone_match': 'Téléphone identique + proximité',
    'siret_match': 'SIRET identique',
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

        all_matches = []
        review_rows = []
        unmatched_rows = []
        total = max(len(df_master), 1)
        for idx, (_, master_row) in enumerate(df_master.iterrows(), start=1):
            candidates = self._get_candidates(master_row, df_slave, slave_by_zip, slave_by_city)
            matches = self._score_candidates(master_row, candidates, options) if not candidates.empty else []
            classification = self._classify_master(master_row, matches, options)
            if classification['best_row'] is not None:
                review_rows.append(classification['best_row']) if classification['best_row']['match_status'] == 'review' else None
            if classification['unmatched_row'] is not None:
                unmatched_rows.append(classification['unmatched_row'])
            if matches:
                all_matches.extend(matches[: options.top_k_per_master])
            if idx == 1 or idx % 100 == 0 or idx == total:
                pct = 35 + int((idx / total) * 50)
                self.progress(min(pct, 90), f'Matching en cours: {idx}/{total} master')

        self.progress(92, 'Construction des livrables Matcher V2')
        all_matches_df = pd.DataFrame(all_matches)
        if all_matches_df.empty:
            all_matches_df = pd.DataFrame(columns=self._result_columns())
        else:
            all_matches_df = all_matches_df.sort_values(
                by=['automatch', 'composite_score', 'score_name', 'score_voie', 'score_city', 'score_phone'],
                ascending=[False, False, False, False, False, False],
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
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix.lower() != '.zip':
            output_path = output_path.with_suffix('.zip')

        with tempfile.TemporaryDirectory(prefix='matcher_v2_') as tmpdir:
            tmpdir_path = Path(tmpdir)
            all_path = tmpdir_path / 'all_matches.csv'
            auto_path = tmpdir_path / 'automatch.csv'
            review_path = tmpdir_path / 'review.csv'
            unmatched_path = tmpdir_path / 'unmatched.csv'
            summary_path = tmpdir_path / 'summary.json'

            all_matches_df.to_csv(all_path, index=False, encoding='utf-8-sig')
            all_matches_df[all_matches_df['match_status'] == 'automatch'].to_csv(auto_path, index=False, encoding='utf-8-sig')
            review_df.to_csv(review_path, index=False, encoding='utf-8-sig')
            unmatched_df.to_csv(unmatched_path, index=False, encoding='utf-8-sig')
            summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')

            with zipfile.ZipFile(output_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(all_path, arcname='all_matches.csv')
                zf.write(auto_path, arcname='automatch.csv')
                zf.write(review_path, arcname='review.csv')
                zf.write(unmatched_path, arcname='unmatched.csv')
                zf.write(summary_path, arcname='summary.json')

        self.log(f"✅ Livrable ZIP écrit: {output_path.name}")
        self.log(f"📊 Résumé matcher: {summary['automatch_rows']} automatch, {summary['review_rows']} review, {summary['unmatched_rows']} unmatched")
        self.progress(100, 'Matcher V2 terminé')
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

            composite_score = round((score_name * 0.48) + (score_voie * 0.24) + (score_city * 0.14) + (score_phone * 0.14), 1)
            match_status = 'automatch' if automatch else self._review_status(score_name, score_voie, score_city, score_phone, same_matchcode, same_hexa, same_siret)
            reason = AUTO_REASONS.get(method, '') if automatch else self._review_reason(score_name, score_voie, score_city, score_phone, same_matchcode, same_hexa, same_siret)

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
                'composite_score': composite_score,
                'same_matchcode': same_matchcode,
                'same_hexa': same_hexa,
                'same_siret': same_siret,
                'distance_meters': None if math.isnan(distance) else round(distance, 2),
                'automatch': automatch,
                'match_status': match_status,
                'match_method': method,
                'match_reason': reason,
            })
        out.sort(key=lambda item: (item['automatch'], item['composite_score'], item['score_name'], item['score_voie'], item['score_city'], item['score_phone']), reverse=True)
        for rank, row in enumerate(out, start=1):
            row['rank_for_master'] = rank
        return out

    def _review_status(self, score_name: float, score_voie: float, score_city: float, score_phone: float, same_matchcode: bool, same_hexa: bool, same_siret: bool) -> str:
        if same_siret or same_hexa:
            return 'review'
        if same_matchcode and score_name >= 70:
            return 'review'
        if score_name >= 82 and score_voie >= 60:
            return 'review'
        if score_name >= 75 and score_voie >= 75 and score_city >= 80:
            return 'review'
        if score_phone == 100:
            return 'review'
        return 'candidate'

    def _review_reason(self, score_name: float, score_voie: float, score_city: float, score_phone: float, same_matchcode: bool, same_hexa: bool, same_siret: bool) -> str:
        reasons = []
        if same_siret:
            reasons.append('SIRET identique')
        if same_hexa:
            reasons.append('Hexa identique')
        if same_matchcode:
            reasons.append('Matchcode identique')
        if score_phone == 100:
            reasons.append('Téléphone identique')
        if score_name >= 80:
            reasons.append('Nom proche')
        if score_voie >= 70:
            reasons.append('Voie proche')
        if score_city >= 80:
            reasons.append('Ville proche')
        return ' + '.join(reasons[:4]) if reasons else 'À vérifier'

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
                    'reason': f"Meilleur candidat insuffisant (score composite {best['composite_score']})",
                },
            }
        return {'best_row': best, 'unmatched_row': None}

    @staticmethod
    def _result_columns() -> list[str]:
        return [
            'master_id', 'master_name', 'master_address', 'master_zipcode', 'master_city',
            'slave_id', 'slave_name', 'slave_address', 'slave_zipcode', 'slave_city',
            'score_name', 'score_voie', 'score_city', 'score_phone', 'composite_score',
            'same_matchcode', 'same_hexa', 'same_siret', 'distance_meters',
            'automatch', 'match_status', 'match_method', 'match_reason', 'rank_for_master'
        ]

    @staticmethod
    def _review_columns() -> list[str]:
        return MatcherService._result_columns()

    @staticmethod
    def _unmatched_columns() -> list[str]:
        return ['master_id', 'master_name', 'master_address', 'master_zipcode', 'master_city', 'reason']

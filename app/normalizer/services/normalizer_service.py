from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd
from slugify import slugify


ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]


CHAINES_CSV_PATH = Path(__file__).resolve().parent.parent.parent / 'legacy_data' / 'chaines.csv'
CHAINES_DATA_AVAILABLE = False
CHAINES_REGEX = None
CHAINES_LOOKUP: dict[str, str] = {}


REFERENCE_COLUMNS = {
    'id', 'hexa', 'name', 'address', 'zipcode', 'city', 'lat', 'lng',
    'vat', 'siren', 'siret', 'phone', 'email', 'website', 'voie',
    'num_voie', 'accessibility_gmap', 'activities_gmap', 'activity_gmap',
    'address_gmap', 'address_comp_gmap', 'advice_gmap', 'all_bookings_gmap',
    'all_deliveries_gmap', 'all_services_gmap', 'amenities_gmap',
    'atmosphere_gmap', 'business_status_gmap', 'cid_gmap', 'city_gmap',
    'code_plus_gmap', 'code_plus_city_gmap', 'country_gmap',
    'country_code_gmap', 'crowd_gmap', 'currency_gmap', 'delivery_gmap',
    'department_gmap', 'dining_options_gmap', 'full_address_gmap',
    'geocode_gmap', 'geoid_gmap', 'google_link_gmap', 'hexa_gmap',
    'hexa_link_gmap', 'info_link_gmap', 'label_gmap',
    'last_review_author_id_gmap', 'last_review_author_name_gmap',
    'last_review_date_gmap', 'lat_gmap', 'lng_gmap', 'name_gmap',
    'num_voie_gmap', 'offerings_gmap', 'outlet_description_gmap',
    'outlet_info_gmap', 'outlet_logo_gmap', 'owner_id_gmap',
    'owner_link_gmap', 'owner_name_gmap', 'payments_gmap', 'phone_gmap',
    'photo_gmap', 'place_id_gmap', 'planning_gmap', 'postal_code_gmap',
    'price_gmap', 'rate_gmap', 'region_gmap', 'social_link_gmap',
    'takeaway_gmap', 'voie_gmap', 'web_gmap', 'web_in_gmap',
    'week_schedule_gmap', 'zipcode_gmap', 'score_name_gmap',
    'score_address_gmap', 'score_city_gmap', 'score_zipcode_gmap',
    'score_num_voie', 'score_voie', 'distance_gmap', 'anomalie_distance',
    'gmap_automatch', 'geocode', 'matchcode', 'chaine'
}

COLUMNS_TO_KEEP = {
    'id', 'name', 'address', 'zipcode', 'city', 'lat', 'lng',
    'hexa_gmap', 'phone_gmap', 'social_link_gmap'
}

_VOIE_STOP_WORDS_LIST = sorted([
    'CENTRE COMMERCIAL', 'DEPARTEMENTALE', 'BOULEVARD', 'AVENUE', 'NATIONALE',
    'CHEMIN', 'ALLEE', 'ROUTE', 'PLACE', 'PL', 'PARKING', 'IMPASSE', 'QUAI',
    'CCIAL', 'LES', 'DES', 'AUX', 'BVD', 'RTE', 'AVE', 'IMP', 'CHEM', 'CHE',
    'ALL', 'LE', 'LA', 'DU', 'DE', 'ET', 'DES', 'AU', 'AUX', 'LES', 'BD',
    'BLD', 'BLVD', 'AV', 'B', 'A', 'D', 'L', 'R', 'RUE', 'NAT', 'DPT', 'NTL',
    'RN', 'RD', 'AIRE', 'ZONE', 'ZAC', 'ZI', 'ZC'
], key=len, reverse=True)

_VOIE_STOP_WORDS_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(w) for w in _VOIE_STOP_WORDS_LIST) + r'|[a-zA-Z]\.)\b',
    flags=re.IGNORECASE,
)


def _noop_progress(percent: int, message: str) -> None:
    return None


def _noop_log(message: str) -> None:
    return None


def load_chaines_data() -> tuple[bool, str | None]:
    global CHAINES_DATA_AVAILABLE, CHAINES_REGEX, CHAINES_LOOKUP
    if CHAINES_DATA_AVAILABLE:
        return True, None
    if not CHAINES_CSV_PATH.is_file():
        return False, f"Fichier chaînes absent : {CHAINES_CSV_PATH.name}. La colonne 'chaine' restera vide."

    try:
        df = pd.read_csv(CHAINES_CSV_PATH)
        if 'name' not in df.columns or 'keyword' not in df.columns:
            return False, "Le fichier chaines.csv doit contenir les colonnes 'name' et 'keyword'."

        df = df.dropna(subset=['keyword', 'name']).copy()
        df['slug_keyword'] = df['keyword'].apply(lambda x: slugify(str(x)))
        df = df.dropna(subset=['slug_keyword'])
        df = df.drop_duplicates(subset=['slug_keyword'])
        df['keyword_len'] = df['slug_keyword'].str.len()
        df = df.sort_values(by='keyword_len', ascending=False)

        CHAINES_LOOKUP = pd.Series(df.name.values, index=df.slug_keyword).to_dict()
        bounded_keywords = [r'\b' + re.escape(k) + r'\b' for k in df['slug_keyword']]
        CHAINES_REGEX = re.compile('|'.join(bounded_keywords)) if bounded_keywords else None
        CHAINES_DATA_AVAILABLE = True
        return True, f"Chaînes chargées : {len(df)} mots-clés"
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"Impossible de charger chaines.csv : {exc}"


def contains_at_least_one_number(value) -> bool:
    if pd.isna(value):
        return False
    return bool(re.search(r'\d', str(value)))


def _find_raw_num_voie_match(address) -> str | None:
    if pd.isna(address):
        return None
    address = str(address)
    patterns = [r'\b(\d+)\b', r'\b(\d+[a-zA-Z]*)|\b([a-zA-Z]+\d+)\b']
    for pattern in patterns:
        match = re.search(pattern, address)
        if match:
            return match.group(0)
    return None


def detect_num_voie(address) -> str | None:
    raw_match = _find_raw_num_voie_match(address)
    if raw_match:
        return re.sub(r'[\s\-_]', '', raw_match)
    return None


def detect_voie(address) -> str | None:
    if pd.isna(address):
        return None
    address_str = str(address)
    raw_num_voie = _find_raw_num_voie_match(address_str)
    if raw_num_voie:
        initial_voie = address_str.replace(raw_num_voie, '', 1).strip()
    else:
        initial_voie = address_str.strip()
    if not initial_voie:
        return initial_voie
    cleaned_voie = _VOIE_STOP_WORDS_PATTERN.sub('', initial_voie)
    processed_voie = slugify(cleaned_voie, separator=' ').upper()
    processed_voie = ' '.join(processed_voie.split())
    if not processed_voie:
        return slugify(initial_voie, separator=' ').upper()
    return processed_voie


def make_matchcode(address, zipcode) -> str | None:
    if pd.isna(address) or pd.isna(zipcode) or not address or not zipcode:
        return None
    zipcode = str(zipcode).replace(' ', '').strip()
    address_proc = str(address).replace('-', ' ').replace("'", ' ')
    words = [word for word in address_proc.split() if word]
    if not words:
        return None

    number_candidate, last_word = None, None
    first_word, last_word = words[0], words[-1]
    if contains_at_least_one_number(first_word):
        number_candidate = first_word
    elif contains_at_least_one_number(last_word):
        number_candidate = last_word

    if number_candidate and number_candidate == words[-1]:
        temp_address_words = words[:-1]
        last_word = temp_address_words[-1] if temp_address_words else None

    if not number_candidate and len(words) > 1 and contains_at_least_one_number(words[-2]):
        number_candidate = words[-2]

    if not number_candidate:
        match = re.search(r'\b(\d+[a-zA-Z]*)\b', address_proc)
        if match:
            number_candidate = match.group(1)
            parts = address_proc.split(number_candidate, 1)
            if parts[0].strip():
                last_word = parts[0].strip().split()[-1]

    if not number_candidate or not last_word:
        return None

    final_number = number_candidate
    addr_slug = slugify(address_proc, separator=' ')
    for suffix in ["bis", "ter", "quater", "quinquies", "sexies"]:
        if f"{slugify(final_number)} {suffix}" in addr_slug:
            final_number = f"{final_number}{suffix}"
            break

    return f"{zipcode}-{slugify(last_word)}-{slugify(final_number)}"


def find_chaine_local(name: str) -> str | None:
    if not CHAINES_DATA_AVAILABLE or pd.isna(name) or not CHAINES_REGEX:
        return None
    slug_name = slugify(str(name))
    if not slug_name:
        return None
    match = CHAINES_REGEX.search(slug_name)
    if match:
        return CHAINES_LOOKUP.get(match.group(0))
    return None


@dataclass(slots=True)
class NormalizerOptions:
    do_clean: bool = True
    do_matchcode: bool = True
    sheet_name: str | None = None


class NormalizerService:
    def __init__(self, progress_callback: ProgressCallback | None = None, log_callback: LogCallback | None = None):
        self.progress_callback = progress_callback or _noop_progress
        self.log_callback = log_callback or _noop_log

    def _progress(self, percent: int, message: str) -> None:
        self.progress_callback(percent, message)

    def _log(self, message: str) -> None:
        self.log_callback(message)

    def run(self, input_path: str | Path, output_path: str | Path, options: NormalizerOptions) -> Path:
        input_path = Path(input_path)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if not input_path.exists():
            raise FileNotFoundError(f"Le fichier n'existe pas : {input_path}")
        if input_path.suffix.lower() not in {'.xlsx', '.xlsm', '.xltx', '.xltm'}:
            raise ValueError('Le normalizer V1 web supporte uniquement les fichiers Excel .xlsx/.xlsm/.xltx/.xltm.')

        self._progress(5, 'Analyse du fichier Excel')
        xls = pd.ExcelFile(input_path)
        sheet_names = xls.sheet_names
        chosen_sheet = options.sheet_name.strip() if options.sheet_name else None
        if chosen_sheet and chosen_sheet not in sheet_names:
            raise ValueError(f"Onglet introuvable : {chosen_sheet}. Onglets disponibles : {', '.join(sheet_names)}")
        if not chosen_sheet:
            chosen_sheet = sheet_names[0]
            if len(sheet_names) > 1:
                self._log(f"ℹ️ Plusieurs onglets détectés : {', '.join(sheet_names)}")
                self._log(f"ℹ️ Aucun onglet fourni, utilisation du premier : {chosen_sheet}")

        self._log(f"📄 Onglet utilisé : {chosen_sheet}")
        df = pd.read_excel(xls, sheet_name=chosen_sheet)
        self._log(f"✓ Fichier chargé : {len(df)} lignes, {len(df.columns)} colonnes")
        self._progress(15, 'Lecture du classeur terminée')

        if options.do_matchcode:
            base_required = {'address', 'zipcode', 'city'}
            missing = sorted(base_required - set(df.columns))
            if missing:
                raise ValueError(
                    "Colonnes requises manquantes pour le matchcode : " + ', '.join(missing) + ". "
                    "Le mapping interactif PyQt n'est pas encore migré dans cette itération."
                )

        if options.do_clean:
            df = self._perform_cleaning(df)

        if options.do_matchcode:
            df = self._perform_matchcode(df)

        self._progress(95, 'Écriture du fichier résultat')
        df.to_excel(output_path, index=False, engine='openpyxl')
        self._log(f"✓ Fichier sauvegardé : {output_path.name}")
        self._log(f"📊 Résumé : {len(df)} lignes, {len(df.columns)} colonnes")
        self._progress(100, 'Traitement terminé')
        return output_path

    def _perform_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log('--- Nettoyage des données ---')
        self._progress(25, 'Nettoyage des colonnes')

        columns_to_remove = []
        custom_columns = []
        for col in df.columns:
            if col not in COLUMNS_TO_KEEP:
                if col in REFERENCE_COLUMNS or 'gmap' in col.lower():
                    columns_to_remove.append(col)
                else:
                    custom_columns.append(col)

        df = df.drop(columns=columns_to_remove, errors='ignore')
        if columns_to_remove:
            self._log(f"✓ {len(columns_to_remove)} colonnes supprimées")
        if custom_columns:
            self._log(f"✓ {len(custom_columns)} colonnes personnalisées conservées : {', '.join(custom_columns)}")

        old_columns_to_drop = []
        if 'hexa' in df.columns and 'hexa_gmap' in df.columns:
            old_columns_to_drop.append('hexa')
        if 'phone' in df.columns and 'phone_gmap' in df.columns:
            old_columns_to_drop.append('phone')
        if 'website' in df.columns and 'social_link_gmap' in df.columns:
            old_columns_to_drop.append('website')
        if old_columns_to_drop:
            df = df.drop(columns=old_columns_to_drop, errors='ignore')
            self._log(f"✓ Anciennes colonnes supprimées avant renommage : {', '.join(old_columns_to_drop)}")

        rename_mapping = {
            'hexa_gmap': 'hexa',
            'phone_gmap': 'phone',
            'social_link_gmap': 'website',
        }
        df = df.rename(columns=rename_mapping)
        self._log('✓ Colonnes renommées avec succès')
        self._progress(40, 'Nettoyage terminé')
        return df

    def _perform_matchcode(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log('--- Génération des matchcodes ---')
        self._progress(50, 'Préparation des colonnes address/zipcode/city')

        for col in ["name", "address", "zipcode", "city"]:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
                if col == 'zipcode':
                    df[col] = df[col].str.replace(r'\.0$', '', regex=True)
                    df[col] = df[col].str.strip()
                    df[col] = df[col].apply(lambda x: x.zfill(5) if x.isdigit() and len(x) > 0 else x)

        self._progress(60, 'Calcul des colonnes num_voie et voie')
        df['num_voie'] = df['address'].apply(detect_num_voie)
        df['voie'] = df['address'].apply(detect_voie)

        self._progress(72, 'Calcul des matchcodes')
        df['matchcode'] = df.apply(lambda row: make_matchcode(row['address'], row['zipcode']), axis=1)
        self._log('✓ Matchcodes générés')

        chains_loaded, chains_message = load_chaines_data()
        if chains_message:
            self._log(('✓ ' if chains_loaded else 'ℹ️ ') + chains_message)
        self._progress(82, 'Recherche des chaînes')
        if 'name' in df.columns and chains_loaded:
            df['chaine'] = df['name'].apply(find_chaine_local)
            self._log('✓ Recherche des chaînes terminée')
        else:
            df['chaine'] = None
            self._log("ℹ️ Colonne 'chaine' laissée vide")

        cols = df.columns.tolist()
        if 'city' in cols:
            city_index = cols.index('city')
            new_cols_order = ['chaine', 'matchcode', 'voie', 'num_voie']
            for column in new_cols_order:
                if column in cols:
                    cols.remove(column)
            for column in reversed(new_cols_order):
                if column in df.columns:
                    cols.insert(city_index + 1, column)
            df = df[cols]

        self._progress(90, 'Réorganisation des colonnes terminée')
        return df

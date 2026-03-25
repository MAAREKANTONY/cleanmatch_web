"""
Normalizer - Outil unifié de nettoyage et génération de Matchcode pour fichiers Excel.

Ce script fournit une interface graphique pour :
1. Nettoyer les données Excel (suppression de colonnes, renommage)
2. Générer les colonnes 'num_voie', 'voie', et 'matchcode'
3. Rechercher les chaînes correspondantes via un fichier CSV local (chaines.csv)

Les deux opérations peuvent être exécutées séparément ou enchaînées.
"""

import sys
import re
import os
from pathlib import Path
from typing import Optional
import traceback

# --- Bibliothèques tierces ---
import pandas as pd
from slugify import slugify

try:
    from tqdm.auto import tqdm
    tqdm.pandas()
except ImportError:
    print("La bibliothèque 'tqdm' est requise. Veuillez l'installer avec : pip install tqdm")
    tqdm = None

# --- Bibliothèques PyQt6 ---
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QFileDialog, QComboBox, QMessageBox, QDialog,
    QDialogButtonBox, QStatusBar, QProgressBar, QTextEdit, QCheckBox,
    QGroupBox
)
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThread
from PyQt6.QtGui import QFont

# Import pour les thèmes
try:
    import qdarkstyle
except ImportError:
    qdarkstyle = None


# =============================================================================
# 0. CONFIGURATION ET CHARGEMENT DES DONNÉES LOCALES (CHAÎNES)
# =============================================================================
CHAINES_CSV_PATH = Path(__file__).parent / "chaines.csv"
CHAINES_DATA_AVAILABLE = False
CHAINES_REGEX = None
CHAINES_LOOKUP = {}


def load_chaines_data():
    """
    Charge les données depuis chaines.csv et les pré-compile en une expression
    régulière unique pour des recherches ultra-rapides.
    """
    global CHAINES_DATA_AVAILABLE, CHAINES_REGEX, CHAINES_LOOKUP
    if not CHAINES_CSV_PATH.is_file():
        print(f"⚠️ Fichier 'chaines.csv' non trouvé à l'emplacement : {CHAINES_CSV_PATH}")
        return

    try:
        df = pd.read_csv(CHAINES_CSV_PATH)
        if 'name' not in df.columns or 'keyword' not in df.columns:
            print("❌ Le fichier 'chaines.csv' doit contenir les colonnes 'name' et 'keyword'.")
            return

        df.dropna(subset=['keyword', 'name'], inplace=True)
        df['slug_keyword'] = df['keyword'].apply(lambda x: slugify(str(x)))
        df.dropna(subset=['slug_keyword'], inplace=True)
        df = df.drop_duplicates(subset=['slug_keyword'])

        df['keyword_len'] = df['slug_keyword'].str.len()
        df.sort_values(by='keyword_len', ascending=False, inplace=True)

        CHAINES_LOOKUP = pd.Series(df.name.values, index=df.slug_keyword).to_dict()

        bounded_keywords = [r'\b' + re.escape(k) + r'\b' for k in df['slug_keyword']]
        pattern = '|'.join(bounded_keywords)

        CHAINES_REGEX = re.compile(pattern)

        CHAINES_DATA_AVAILABLE = True
        print(f"✅ Fichier 'chaines.csv' chargé et compilé avec succès ({len(df)} mots-clés).")

    except Exception as e:
        print(f"🚨 Erreur lors du chargement de 'chaines.csv' : {e}")


# Charger et compiler les données au démarrage du script
load_chaines_data()


# =============================================================================
# 1. CLASSES UTILITAIRES POUR LA BARRE DE PROGRESSION
# =============================================================================
class ProgressEmitter(QObject):
    progress_signal = pyqtSignal(int, str)


class TqdmStreamWriter(QObject):
    def __init__(self, emitter: ProgressEmitter):
        super().__init__()
        self.emitter = emitter

    def write(self, text):
        text = text.strip()
        if not text:
            return
        match_percent = re.search(r'(\d+)%\|', text)
        match_time = re.search(r'\[(.*?)\]', text)
        if match_percent:
            percentage = int(match_percent.group(1))
            eta_text = f"Temps : {match_time.group(1)}" if match_time else ""
            self.emitter.progress_signal.emit(percentage, eta_text)

    def flush(self):
        pass


# =============================================================================
# 2. FONCTIONS DE TRAITEMENT - MATCHCODE
# =============================================================================
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
    flags=re.IGNORECASE
)


def contains_at_least_one_number(s) -> bool:
    if pd.isna(s):
        return False
    return bool(re.search(r'\d', str(s)))


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

    address_proc = str(address).replace('-', ' ').replace("'", " ").replace("'", " ")

    words = [word for word in address_proc.split() if word]
    if not words:
        return None

    number_candidate, last_word = None, None
    if words:
        first_word, last_word = words[0], words[-1]
        if contains_at_least_one_number(first_word):
            number_candidate = first_word
        elif contains_at_least_one_number(last_word):
            number_candidate = last_word

    if number_candidate and number_candidate == words[-1]:
        temp_address_words = words[:-1]
        last_word = temp_address_words[-1] if temp_address_words else None

    if not number_candidate and len(words) > 1:
        if contains_at_least_one_number(words[-2]):
            number_candidate = words[-2]

    if not number_candidate:
        match = re.search(r'\b(\d+[a-zA-Z]*)\b', address_proc)
        if match:
            number_candidate = match.group(1)
            parts = address_proc.split(number_candidate, 1)
            if parts[0].strip():
                segment_words = parts[0].strip().split()
                last_word = segment_words[-1]

    if not number_candidate or not last_word:
        return None

    final_number = number_candidate
    addr_slug = slugify(address_proc, separator=' ')
    suffixes = ["bis", "ter", "quater", "quinquies", "sexies"]
    for suffix in suffixes:
        if f"{slugify(final_number)} {suffix}" in addr_slug:
            final_number = f"{final_number}{suffix}"
            break

    return f"{zipcode}-{slugify(last_word)}-{slugify(final_number)}"


def find_chaine_local(name: str) -> str | None:
    if not CHAINES_DATA_AVAILABLE or pd.isna(name):
        return None
    slug_name = slugify(str(name))
    if not slug_name:
        return None
    match = CHAINES_REGEX.search(slug_name)
    if match:
        found_keyword = match.group(0)
        return CHAINES_LOOKUP.get(found_keyword)
    return None


# =============================================================================
# 3. CONFIGURATION DU NETTOYAGE (CLEANER)
# =============================================================================
# Colonnes de référence (colonnes connues du système)
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

# Colonnes à conserver après nettoyage
COLUMNS_TO_KEEP = {
    'id', 'name', 'address', 'zipcode', 'city', 'lat', 'lng',
    'hexa_gmap', 'phone_gmap', 'social_link_gmap'
}


# =============================================================================
# 4. WORKER UNIFIÉ POUR LE TRAITEMENT
# =============================================================================
class NormalizerWorker(QObject):
    progress = pyqtSignal(int, str)
    status = pyqtSignal(str)
    log = pyqtSignal(str)
    finished = pyqtSignal(bool, str)
    request_sheet = pyqtSignal(list)
    request_mapping = pyqtSignal(list, list)

    def __init__(self, input_path: Path, do_clean: bool, do_matchcode: bool):
        super().__init__()
        self.input_file_path = input_path
        self.do_clean = do_clean
        self.do_matchcode = do_matchcode
        self._chosen_sheet = None
        self._mapping = None

    def run(self):
        try:
            self.log.emit("🔄 Démarrage du traitement...")
            self.progress.emit(0, "")

            # Vérifier l'existence du fichier
            if not self.input_file_path.exists():
                raise FileNotFoundError(f"Le fichier n'existe pas : {self.input_file_path}")

            self.log.emit(f"📂 Fichier ouvert : {self.input_file_path}")

            # Charger le fichier Excel
            self.status.emit("Analyse du fichier en cours...")
            try:
                xls = pd.ExcelFile(self.input_file_path)
            except PermissionError:
                raise PermissionError(
                    "Le fichier est actuellement ouvert dans une autre application. "
                    "Veuillez le fermer avant de continuer."
                )

            # Gestion des onglets multiples
            sheet_names = xls.sheet_names
            if len(sheet_names) > 1:
                self.request_sheet.emit(sheet_names)
                while self._chosen_sheet is None:
                    QThread.msleep(100)
            else:
                self.set_chosen_sheet(sheet_names[0])

            if self._chosen_sheet is None:
                self.finished.emit(False, "Traitement annulé par l'utilisateur.")
                return

            # Lecture du DataFrame
            df = pd.read_excel(xls, sheet_name=self._chosen_sheet)
            self.log.emit(f"✓ Fichier chargé : {len(df)} lignes, {len(df.columns)} colonnes")
            self.progress.emit(10, "")

            # Vérifier les colonnes requises pour le matchcode
            base_required = ["address", "zipcode", "city"]
            if self.do_matchcode and not set(base_required).issubset(set(df.columns)):
                self.status.emit("En-têtes manquants, attente du mappage...")
                self.request_mapping.emit(list(df.columns), base_required)
                while self._mapping is None:
                    QThread.msleep(100)
                if not self._mapping:
                    self.finished.emit(False, "Traitement annulé par l'utilisateur.")
                    return
                df.rename(columns=self._mapping, inplace=True)

            # =========== ÉTAPE 1 : NETTOYAGE (si activé) ===========
            if self.do_clean:
                df = self._perform_cleaning(df)

            # =========== ÉTAPE 2 : MATCHCODE (si activé) ===========
            if self.do_matchcode:
                df = self._perform_matchcode(df)

            # Générer le nom du fichier de sortie
            output_path = self._generate_output_filename()

            # Sauvegarder le fichier
            self.status.emit(f"Sauvegarde vers {output_path.name}...")
            df.to_excel(output_path, index=False, engine='openpyxl')
            self.log.emit(f"✓ Fichier sauvegardé : {output_path}")
            self.log.emit(f"📊 Résumé : {len(df)} lignes, {len(df.columns)} colonnes")

            self.progress.emit(100, "")
            self.finished.emit(True, f"Traitement réussi !\nFichier : {output_path.name}")

        except Exception as e:
            error_msg = f"❌ Erreur : {str(e)}\n\n{traceback.format_exc()}"
            self.log.emit(error_msg)
            self.progress.emit(0, "")
            self.finished.emit(False, f"Erreur : {str(e)}")

    def _perform_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Effectue le nettoyage des colonnes."""
        self.status.emit("Nettoyage des colonnes...")
        self.log.emit("--- Nettoyage des données ---")

        # Déterminer les colonnes à supprimer et celles à garder
        columns_to_remove = []
        custom_columns = []

        for col in df.columns:
            if col not in COLUMNS_TO_KEEP:
                if col in REFERENCE_COLUMNS or 'gmap' in col.lower():
                    columns_to_remove.append(col)
                else:
                    custom_columns.append(col)

        # Supprimer les colonnes de référence non désirées
        df = df.drop(columns=columns_to_remove, errors='ignore')

        if columns_to_remove:
            self.log.emit(f"✓ {len(columns_to_remove)} colonnes supprimées")
        if custom_columns:
            self.log.emit(f"✓ {len(custom_columns)} colonnes personnalisées conservées : {', '.join(custom_columns)}")

        self.progress.emit(30, "")

        # Supprimer les anciennes colonnes avant renommage
        old_columns_to_drop = []
        if 'hexa' in df.columns and 'hexa_gmap' in df.columns:
            old_columns_to_drop.append('hexa')
        if 'phone' in df.columns and 'phone_gmap' in df.columns:
            old_columns_to_drop.append('phone')
        if 'website' in df.columns and 'social_link_gmap' in df.columns:
            old_columns_to_drop.append('website')

        if old_columns_to_drop:
            df = df.drop(columns=old_columns_to_drop, errors='ignore')
            self.log.emit(f"✓ Anciennes colonnes {old_columns_to_drop} supprimées avant renommage")

        # Renommer les colonnes
        rename_mapping = {
            'hexa_gmap': 'hexa',
            'phone_gmap': 'phone',
            'social_link_gmap': 'website'
        }
        df = df.rename(columns=rename_mapping)
        self.log.emit("✓ Colonnes renommées avec succès")

        self.progress.emit(40, "")
        return df

    def _perform_matchcode(self, df: pd.DataFrame) -> pd.DataFrame:
        """Effectue la génération des matchcodes."""
        self.status.emit("Génération des matchcodes...")
        self.log.emit("--- Génération des matchcodes ---")

        # Nettoyage des données
        all_potential = ["name", "address", "zipcode", "city"]
        for col in all_potential:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
                if col == 'zipcode':
                    df[col] = df[col].str.replace(r'\.0$', '', regex=True)
                    df[col] = df[col].str.strip()
                    df[col] = df[col].apply(lambda x: x.zfill(5) if x.isdigit() and len(x) > 0 else x)

        has_name_column = 'name' in df.columns
        perform_chain_lookup = has_name_column and CHAINES_DATA_AVAILABLE

        # Calcul des matchcodes avec progression
        self.status.emit("Calcul des matchcodes...")

        if tqdm:
            emitter_cpu = ProgressEmitter()
            emitter_cpu.progress_signal.connect(
                lambda p, e: self.progress.emit(50 + int(p * 0.3), e)
            )
            tqdm.pandas(file=TqdmStreamWriter(emitter_cpu), desc="Traitement Matchcode")
            df['num_voie'] = df['address'].progress_apply(detect_num_voie)
            df['voie'] = df['address'].progress_apply(detect_voie)
            df['matchcode'] = df.progress_apply(
                lambda r: make_matchcode(r['address'], r['zipcode']), axis=1
            )
        else:
            df['num_voie'] = df['address'].apply(detect_num_voie)
            df['voie'] = df['address'].apply(detect_voie)
            df['matchcode'] = df.apply(
                lambda r: make_matchcode(r['address'], r['zipcode']), axis=1
            )

        self.log.emit("✓ Matchcodes générés")
        self.progress.emit(80, "")

        # Recherche des chaînes
        if perform_chain_lookup:
            self.status.emit("Recherche des chaînes...")
            self.log.emit("Recherche des chaînes (local)...")

            if tqdm:
                emitter_chaine = ProgressEmitter()
                emitter_chaine.progress_signal.connect(
                    lambda p, e: self.progress.emit(80 + int(p * 0.15), e)
                )
                tqdm.pandas(file=TqdmStreamWriter(emitter_chaine), desc="Recherche Chaînes")
                df['chaine'] = df['name'].progress_apply(find_chaine_local)
            else:
                df['chaine'] = df['name'].apply(find_chaine_local)

            self.log.emit("✓ Recherche des chaînes terminée")
        else:
            df['chaine'] = None

        # Réorganisation des colonnes
        self.status.emit("Réorganisation des colonnes...")
        cols = df.columns.tolist()
        if 'city' in cols:
            city_index = cols.index('city')
            new_cols_order = ['chaine', 'matchcode', 'voie', 'num_voie']
            for c in new_cols_order:
                if c in cols:
                    cols.remove(c)
            for i, new_col in enumerate(reversed(new_cols_order)):
                if new_col in df.columns:
                    cols.insert(city_index + 1, new_col)
            df = df[cols]

        self.progress.emit(95, "")
        return df

    def _generate_output_filename(self) -> Path:
        """Génère le nom du fichier de sortie selon les opérations effectuées."""
        stem = self.input_file_path.stem

        # Retirer les suffixes existants
        for suffix in ['_enriched', '_cleaned', '_matchcoded', '_normalized']:
            if stem.endswith(suffix):
                stem = stem[:-len(suffix)]
                break

        # Ajouter le suffixe approprié
        if self.do_clean and self.do_matchcode:
            output_name = f"{stem}_normalized.xlsx"
        elif self.do_clean:
            output_name = f"{stem}_cleaned.xlsx"
        else:
            output_name = f"{stem}_matchcoded.xlsx"

        return self.input_file_path.parent / output_name

    def set_chosen_sheet(self, sheet_name):
        self._chosen_sheet = sheet_name

    def set_mapping(self, mapping):
        self._mapping = mapping


# =============================================================================
# 5. DIALOGUES PERSONNALISÉS
# =============================================================================
class SheetSelectDialog(QDialog):
    def __init__(self, sheet_names, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Choisir un onglet")
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Le fichier contient plusieurs onglets.\nLequel souhaitez-vous analyser ?"))
        self.combo = QComboBox()
        self.combo.addItems(sheet_names)
        layout.addWidget(self.combo)
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_selected_sheet(self):
        return self.combo.currentText()


class ColumnMappingDialog(QDialog):
    def __init__(self, file_columns, required_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Mapper les colonnes")
        self.file_columns = list(file_columns)
        self.required_columns = required_columns
        self.mapping = {}
        self.combos = {}

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Les en-têtes requis n'ont pas été trouvés.\nVeuillez mapper les colonnes :"))

        for req_col in self.required_columns:
            h_layout = QHBoxLayout()
            h_layout.addWidget(QLabel(f"Colonne requise : <b>{req_col}</b>"))
            combo = QComboBox()
            combo.addItems(self.file_columns)
            for i, f_col in enumerate(self.file_columns):
                if req_col.lower() in str(f_col).lower():
                    combo.setCurrentIndex(i)
            h_layout.addWidget(combo)
            layout.addLayout(h_layout)
            self.combos[req_col] = combo

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept_mapping)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def accept_mapping(self):
        self.mapping = {
            self.combos[req_col].currentText(): req_col
            for req_col in self.required_columns
        }
        if len(self.mapping) != len(set(self.mapping.keys())):
            QMessageBox.warning(
                self, "Erreur",
                "Vous ne pouvez pas assigner la même colonne à plusieurs cibles."
            )
            return
        self.accept()


# =============================================================================
# 6. FENÊTRE PRINCIPALE DE L'APPLICATION
# =============================================================================
class NormalizerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Normalizer - Nettoyage & Matchcode Excel")
        self.setGeometry(100, 100, 750, 650)
        self.input_file_path: Optional[Path] = None
        self.thread: Optional[QThread] = None
        self.worker: Optional[NormalizerWorker] = None
        self.setup_ui()
        self.apply_theme("Système")
        self.update_status_bar()

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # Titre
        title = QLabel("Normalizer")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        main_layout.addWidget(title)

        # Thème
        theme_layout = QHBoxLayout()
        theme_layout.addWidget(QLabel("Thème :"))
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Système", "Clair", "Sombre"])
        if not qdarkstyle:
            self.theme_combo.setEnabled(False)
        theme_layout.addWidget(self.theme_combo)
        theme_layout.addStretch()
        main_layout.addLayout(theme_layout)

        # Section fichier
        file_group = QGroupBox("1. Sélection du fichier")
        file_layout = QHBoxLayout(file_group)
        self.file_label = QLabel("Aucun fichier sélectionné")
        self.file_label.setStyleSheet("color: #666; font-style: italic;")
        self.browse_button = QPushButton("📂 Parcourir...")
        self.browse_button.setMinimumHeight(35)
        file_layout.addWidget(self.file_label, stretch=1)
        file_layout.addWidget(self.browse_button)
        main_layout.addWidget(file_group)

        # Section opérations
        ops_group = QGroupBox("2. Opérations à effectuer")
        ops_layout = QVBoxLayout(ops_group)

        self.clean_checkbox = QCheckBox("Nettoyer les données (suppression colonnes gmap, renommage)")
        self.clean_checkbox.setChecked(True)
        ops_layout.addWidget(self.clean_checkbox)

        self.matchcode_checkbox = QCheckBox("Générer les matchcodes (num_voie, voie, matchcode, chaine)")
        self.matchcode_checkbox.setChecked(True)
        ops_layout.addWidget(self.matchcode_checkbox)

        main_layout.addWidget(ops_group)

        # Bouton de traitement
        self.process_button = QPushButton("🚀 Lancer le traitement")
        self.process_button.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.process_button.setMinimumHeight(45)
        self.process_button.setEnabled(False)
        self.process_button.setStyleSheet(
            "QPushButton:enabled { background-color: #4CAF50; color: white; font-weight: bold; }"
            "QPushButton:disabled { background-color: #ccc; color: #999; }"
        )
        main_layout.addWidget(self.process_button)

        # Barre de progression
        progress_group = QGroupBox("Progression")
        progress_layout = QVBoxLayout(progress_group)
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar)
        self.eta_label = QLabel("")
        self.eta_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        progress_layout.addWidget(self.eta_label)
        main_layout.addWidget(progress_group)

        # Zone de logs
        log_group = QGroupBox("3. Logs et détails")
        log_layout = QVBoxLayout(log_group)
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(180)
        log_font = QFont("Courier")
        log_font.setPointSize(9)
        self.log_text.setFont(log_font)
        log_layout.addWidget(self.log_text)
        main_layout.addWidget(log_group)

        # Barre de statut
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)

        # Connexions
        self.browse_button.clicked.connect(self.select_file)
        self.process_button.clicked.connect(self.start_processing)
        self.clean_checkbox.stateChanged.connect(self.update_process_button)
        self.matchcode_checkbox.stateChanged.connect(self.update_process_button)
        if qdarkstyle:
            self.theme_combo.currentTextChanged.connect(self.apply_theme)

    def select_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Sélectionner un fichier Excel",
            "",
            "Fichiers Excel (*.xlsx *.xls)"
        )
        if file_path:
            self.input_file_path = Path(file_path)
            self.file_label.setText(f"✓ {self.input_file_path.name}")
            self.file_label.setStyleSheet("color: #4CAF50;")
            self.update_process_button()
            self.log_text.clear()
            self.log_text.append(f"📁 Fichier sélectionné : {file_path}")
            self.update_status_bar()

    def update_process_button(self):
        has_file = self.input_file_path is not None
        has_operation = self.clean_checkbox.isChecked() or self.matchcode_checkbox.isChecked()
        self.process_button.setEnabled(has_file and has_operation)

    def start_processing(self):
        if not self.input_file_path:
            QMessageBox.warning(self, "Aucun fichier", "Veuillez d'abord sélectionner un fichier.")
            return

        if not self.clean_checkbox.isChecked() and not self.matchcode_checkbox.isChecked():
            QMessageBox.warning(self, "Aucune opération", "Veuillez sélectionner au moins une opération.")
            return

        if self.matchcode_checkbox.isChecked() and not tqdm:
            QMessageBox.critical(self, "Dépendance manquante", "La bibliothèque 'tqdm' est requise pour les matchcodes.")
            return

        self.set_ui_for_processing(True)
        self.log_text.clear()
        self.progress_bar.setValue(0)

        # Créer et lancer le worker
        self.thread = QThread()
        self.worker = NormalizerWorker(
            self.input_file_path,
            self.clean_checkbox.isChecked(),
            self.matchcode_checkbox.isChecked()
        )
        self.worker.moveToThread(self.thread)

        # Connexions
        self.worker.progress.connect(self.update_progress)
        self.worker.status.connect(self.update_status)
        self.worker.log.connect(self.append_log)
        self.worker.finished.connect(self.processing_finished)
        self.worker.request_sheet.connect(self.prompt_for_sheet)
        self.worker.request_mapping.connect(self.prompt_for_mapping)

        self.thread.started.connect(self.worker.run)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.start()

    def update_progress(self, percentage, eta_text):
        self.progress_bar.setValue(percentage)
        self.eta_label.setText(eta_text)

    def update_status(self, message):
        self.statusBar.showMessage(message)

    def append_log(self, message):
        self.log_text.append(message)
        self.log_text.verticalScrollBar().setValue(
            self.log_text.verticalScrollBar().maximum()
        )

    def processing_finished(self, success: bool, message: str):
        self.set_ui_for_processing(False)
        if success:
            self.append_log(f"\n✅ {message}")
            self.update_status_bar(f"Terminé avec succès.")
        else:
            self.append_log(f"\n❌ {message}")
            self.update_status_bar("Échec du traitement.")
        self.cleanup_thread()

    def cleanup_thread(self):
        if self.thread and self.thread.isRunning():
            self.thread.quit()
            self.thread.wait()
        self.thread = None
        self.worker = None

    def set_ui_for_processing(self, is_processing: bool):
        self.browse_button.setEnabled(not is_processing)
        self.process_button.setEnabled(not is_processing)
        self.clean_checkbox.setEnabled(not is_processing)
        self.matchcode_checkbox.setEnabled(not is_processing)
        if is_processing:
            self.progress_bar.setValue(0)
            self.eta_label.setText("Démarrage...")

    def update_status_bar(self, message=None):
        if message:
            self.statusBar.showMessage(message)
            return
        base_msg = "Prêt." if not self.input_file_path else f"Fichier '{self.input_file_path.name}' chargé."
        chaines_status = "Base chaînes disponible." if CHAINES_DATA_AVAILABLE else "Base chaînes non trouvée."
        self.statusBar.showMessage(f"{base_msg} {chaines_status}")

    def apply_theme(self, theme_name: str):
        if not qdarkstyle:
            return
        stylesheet = ""
        if theme_name == "Clair":
            stylesheet = qdarkstyle.load_stylesheet(palette=qdarkstyle.LightPalette)
        elif theme_name == "Sombre":
            stylesheet = qdarkstyle.load_stylesheet(palette=qdarkstyle.DarkPalette)
        QApplication.instance().setStyleSheet(stylesheet)

    def prompt_for_sheet(self, sheet_names):
        dialog = SheetSelectDialog(sheet_names, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.worker.set_chosen_sheet(dialog.get_selected_sheet())
        else:
            self.worker.set_chosen_sheet(None)

    def prompt_for_mapping(self, columns, required):
        dialog = ColumnMappingDialog(columns, required, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.worker.set_mapping(dialog.mapping)
        else:
            self.worker.set_mapping(None)

    def closeEvent(self, event):
        self.cleanup_thread()
        event.accept()


# =============================================================================
# 7. POINT D'ENTRÉE DE L'APPLICATION
# =============================================================================
def main():
    app = QApplication(sys.argv)
    window = NormalizerApp()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

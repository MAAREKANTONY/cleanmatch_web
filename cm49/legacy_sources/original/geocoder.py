import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import googlemaps
from googlemaps.exceptions import ApiError, Timeout, TransportError

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QPushButton, QLabel, QLineEdit, QComboBox, QProgressBar, QTextEdit,
    QFileDialog, QMessageBox, QGroupBox, QDialog, QDialogButtonBox,
    QInputDialog, QCheckBox # Added QCheckBox for potential future use (like CSV headers)
)

from PyQt6.QtCore import (
    QObject, QThread, pyqtSignal, pyqtSlot, Qt, QMetaObject, Q_ARG, QSize,
    QEventLoop, QTimer
)

from PyQt6.QtGui import QTextCursor, QIcon, QScreen, QColor # Added QColor for status label
import os
import time
import json
from datetime import timedelta
import requests
import sqlite3
import hashlib
from slugify import slugify # slugify est utilisé pour le job_id et les noms de fichiers
import logging
import sys
import concurrent.futures
import threading
import traceback # Import traceback for detailed error logging
from pathlib import Path # Use Path objects

# AJOUTÉ : Pour gérer l'avertissement de sécurité lors de la désactivation de la vérification SSL
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# AJOUTÉ : Désactive les avertissements pour les requêtes SSL non vérifiées.
# Ceci est nécessaire car nous allons utiliser verify=False pour contourner
# le problème de certificat expiré sur data.fyre.one.
# AVERTISSEMENT DE SÉCURITÉ : N'utilisez cette approche que si vous faites confiance
# à l'endpoint et comprenez les risques.
urllib3.disable_warnings(InsecureRequestWarning)


# NEW GMAPSCRAPER INTEGRATION: Import GmapScraper logic
try:
    # Assuming geoclass.py is in the same directory or accessible via PYTHONPATH
    from geoclass import get_data as gmap_get_data_function
    GMAP_SCRAPER_AVAILABLE = True
    logging.info("✅ Module GmapScraper (geoclass.py) chargé avec succès pour geocoder.py.") # Updated log
except ImportError as e:
    GMAP_SCRAPER_AVAILABLE = False
    logging.warning(f"⚠️ GmapScraper non disponible pour geocoder.py. Erreur d'import depuis geoclass.py: {e}") # Updated log
    gmap_get_data_function = None # Explicitly set to None

def get_script_dir():
    if getattr(sys, 'frozen', False):
        # If the application is run as a bundle/exe, the script dir is the exe dir
        return Path(sys.executable).parent
    else:
        # If run as a normal script, use __file__
        try:
            return Path(__file__).parent
        except NameError:
            # Fallback for interactive environments like IPython
            return Path(os.getcwd())

script_dir = get_script_dir()
LOG_FILENAME = script_dir / 'geocoding_pyqt.log' # Use Path object

# Configure logging
# Ensure the log directory exists (useful if script_dir is complex)
try:
    LOG_FILENAME.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
        handlers=[
            logging.FileHandler(str(LOG_FILENAME), encoding='utf-8'), # Convert Path to string for handler
            # logging.StreamHandler(sys.stdout) # Uncomment for console logging during development
        ]
    )
except Exception as log_init_error:
    print(f"CRITICAL ERROR: Failed to initialize logging to {LOG_FILENAME}. Error: {log_init_error}")
    # Optionally fallback to basic console logging
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')


logging.info(f"✨ Script directory determined as: {script_dir}")
logging.info(f"📄 Log file path: {LOG_FILENAME}")


# --- Constantes ---
CACHE_DB_FILE = script_dir / 'geocode_cache.db' # Use Path object
CHECKPOINT_SUFFIX = '_checkpoint.json'
TEMP_SUFFIX = '_temp.xlsx'

# --- Column Definitions (Inspired by fyre.py) ---
# Target column names expected by the geocoding logic and API calls internally
# MODIFIÉ: Ajout de 'hexa'
TARGET_COLS_GEOCODER = ['id', 'hexa', 'name', 'address', 'zipcode', 'city', 'lat', 'lng', 'vat', 'siren', 'siret', 'phone', 'email', 'website']
# Columns that *must* be mapped for the core geocoding functionality
ESSENTIAL_COLS_GEOCODER = ['id', 'address', 'zipcode', 'city']
# Columns marked with '*' in the mapping dialog (usually the essential ones)
MANDATORY_COLS_FOR_MAPPING_DIALOG_GEO = ['id'] # Seul 'id' est strictement obligatoire au mapping
# Base columns always included in output if present
# MODIFIÉ: Ajout de 'hexa'
BASE_OUTPUT_COLS_GEOCODER = ['id', 'hexa', 'name', 'address', 'zipcode', 'city'] # lat/lng added dynamically if present


NOMINATIM_OUTPUT_COLS = ['google_address', 'google_street', 'google_region', 'google_state', 'google_zipcode', 'google_city', 'google_country']

# API Endpoints
BASE_ENDOPOINT = "https://data.fyre.one/api/"
FYRE_API_ENDPOINT = BASE_ENDOPOINT + "hexa"
HERE_API_ENDPOINT = BASE_ENDOPOINT + "bing"
ETABS_API_ENDPOINT = BASE_ENDOPOINT + "etabs"

# Batch API settings
BATCH_API_MAX_WORKERS = 6
BATCH_API_BATCH_SIZE = 10

# --- API Keys (Load from environment variables) ---
GOOGLE_API_KEY = os.getenv("GOOGLE_PLACE_API_KEY")
FYRE_API_KEY = os.getenv("FYRE_API_KEY")

# --- QSS Themes ---
LIGHT_MODE_QSS = """
    QWidget {
        background-color: #f0f0f0;
        color: #000000;
        font-family: Segoe UI, Arial, sans-serif; /* Police plus standard */
        font-size: 9pt;
    }
    QMainWindow, QDialog, QGroupBox {
        background-color: #f0f0f0;
    }
    QLineEdit, QComboBox, QTextEdit {
        background-color: #ffffff;
        color: #000000;
        border: 1px solid #cccccc;
        border-radius: 3px;
        padding: 3px;
    }
    QTextEdit {
        font-family: Consolas, Courier New, monospace; /* Garder la police pour les logs */
        font-size: 9pt;
    }
    QPushButton {
        background-color: #e0e0e0;
        color: #000000;
        border: 1px solid #cccccc;
        border-radius: 5px;
        padding: 5px;
        min-height: 20px; /* Hauteur minimale */
    }
    QPushButton:hover {
        background-color: #d0d0d0;
    }
    QPushButton:pressed {
        background-color: #c0c0c0;
    }
    QPushButton:disabled {
        background-color: #f0f0f0;
        color: #a0a0a0;
    }
    QGroupBox {
        font-weight: bold;
        border: 1px solid #c0c0c0;
        border-radius: 5px;
        margin-top: 10px; /* Espace pour le titre */
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left; /* Alignement du titre */
        padding: 0 5px 0 5px;
        left: 10px; /* Décalage du titre */
    }
    QLabel#inputLabel, QLabel#outputLabel { /* ID pour les labels spécifiques */
        color: #222222;
        background-color: #e9e9e9; /* Légèrement différent du fond global */
        border: 1px solid #c0c0c0;
        border-radius: 3px;
        padding: 3px;
        min-height: 25px;
    }
    QLabel#fileStatusLabel[status="ok"] { color: green; }
    QLabel#fileStatusLabel[status="warning"] { color: orange; }
    QLabel#fileStatusLabel[status="error"] { color: red; }
    QLabel#fileStatusLabel { /* Default style */
        color: #555555;
        font-size: 8pt;
        margin-left: 5px;
    }
    QProgressBar {
        border: 1px solid #b0b0b0;
        border-radius: 5px;
        text-align: center;
        background-color: #e0e0e0;
        color: #000000; /* S'assurer que le texte est visible si text-align: center et value()=0 */
    }
    QProgressBar::chunk {
        background-color: #98b6c4; /* Vert plus doux */
        border-radius: 4px;
    }
"""

DARK_MODE_QSS = """
    QWidget {
        background-color: #2e2e2e;
        color: #e0e0e0;
        font-family: Segoe UI, Arial, sans-serif;
        font-size: 9pt;
    }
    QMainWindow, QDialog, QGroupBox {
        background-color: #2e2e2e;
    }
    QLineEdit, QComboBox, QTextEdit {
        background-color: #3c3c3c;
        color: #e0e0e0;
        border: 1px solid #555555;
        border-radius: 3px;
        padding: 3px;
    }
    QTextEdit {
        font-family: Consolas, Courier New, monospace;
        font-size: 9pt;
    }
    QPushButton {
        background-color: #4a4a4a;
        color: #e0e0e0;
        border: 1px solid #555555;
        border-radius: 5px;
        padding: 5px;
        min-height: 20px;
    }
    QPushButton:hover {
        background-color: #5a5a5a;
    }
    QPushButton:pressed {
        background-color: #6a6a6a;
    }
    QPushButton:disabled {
        background-color: #383838;
        color: #707070;
    }
    QGroupBox {
        font-weight: bold;
        border: 1px solid #505050;
        border-radius: 5px;
        margin-top: 10px;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        padding: 0 5px 0 5px;
        left: 10px;
    }
    QLabel#inputLabel, QLabel#outputLabel {
        color: #e0e0e0;
        background-color: #383838;
        border: 1px solid #505050;
        border-radius: 3px;
        padding: 3px;
        min-height: 25px;
    }
    QLabel#fileStatusLabel[status="ok"] { color: lightgreen; }
    QLabel#fileStatusLabel[status="warning"] { color: orange; }
    QLabel#fileStatusLabel[status="error"] { color: salmon; }
     QLabel#fileStatusLabel { /* Default style */
        color: #aaaaaa;
        font-size: 8pt;
        margin-left: 5px;
    }
    QProgressBar {
        border: 1px solid #606060;
        border-radius: 5px;
        text-align: center;
        background-color: #3c3c3c;
        color: #e0e0e0;
    }
    QProgressBar::chunk {
        background-color: #98b6c4; /* Use the same light blueish color */
        border-radius: 4px;
    }
"""

# ==============================================================================
# WorkerSignals Class
# ==============================================================================
class WorkerSignals(QObject):
    """Defines the signals available from a running worker thread."""
    finished = pyqtSignal()
    error = pyqtSignal(tuple) # (Exception type, Exception instance, traceback string)
    progress_update = pyqtSignal(tuple) # (percentage, processed_str, eta_str, status_label)
    log_message = pyqtSignal(str, int) # (message, logging_level)
    show_message = pyqtSignal(str, str, str) # (type: info/warning/error, title, message)
    state_change = pyqtSignal(bool, bool, bool) # (is_processing, is_paused, cancel_requested)
    reset_ui_request = pyqtSignal(dict) # (final_checkpoint_data)

# ==============================================================================
# OperationCanceledError Custom Exception
# ==============================================================================
class OperationCanceledError(Exception):
    """Custom exception for handling user cancellation."""
    pass

# ==============================================================================
# GeocoderColumnMappingDialog Class (Adapted from fyre.py)
# ==============================================================================
class GeocoderColumnMappingDialog(QDialog):
    """Dialog for manually mapping source file columns to required geocoder columns."""
    def __init__(self, parent, available_columns: list[str], mandatory_target_cols_for_asterisk: list[str], all_target_cols: list[str]):
        super().__init__(parent)
        self.setWindowTitle("📑 Mapper les Colonnes (Géocodeur)")
        self.setModal(True)
        self.mapping_result: dict[str, str] | None = None # Stores {target: source}

        self.available_source_cols = ['<Ignorer>'] + available_columns
        self.column_vars_combos: dict[str, QComboBox] = {}

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Veuillez mapper les colonnes de votre fichier aux champs requis/reconnus par le géocodeur:"))

        form_layout = QFormLayout()
        for target_col in all_target_cols:
            is_mandatory_for_asterisk = target_col in mandatory_target_cols_for_asterisk
            label_text = f"{target_col}{'*' if is_mandatory_for_asterisk else ''} :"

            combo = QComboBox(self)
            combo.addItems(self.available_source_cols)

            pre_selected_source_col = None
            target_col_lower = target_col.lower()

            match = next((s_col for s_col in available_columns if s_col.lower() == target_col_lower), None)
            if match:
                pre_selected_source_col = match
            else:
                if target_col_lower == 'lat':
                    alias_match = next((s_col for s_col in available_columns if s_col.lower() == 'latitude'), None)
                    if alias_match: pre_selected_source_col = alias_match
                elif target_col_lower == 'lng':
                    alias_match = next((s_col for s_col in available_columns if s_col.lower() == 'longitude'), None)
                    if alias_match: pre_selected_source_col = alias_match

            if pre_selected_source_col:
                combo.setCurrentText(pre_selected_source_col)
            else:
                combo.setCurrentIndex(0)

            form_layout.addRow(label_text, combo)
            self.column_vars_combos[target_col] = combo
        layout.addLayout(form_layout)

        # MODIFIÉ: Ajout de l'explication pour 'hexa'
        explanation_text = "* Champs essentiels requis pour le géocodage.\n"
        explanation_text += "Le champ 'hexa' est optionnel et peut remplacer l'adresse/CP/ville.\n"
        explanation_text += "Les champs 'lat' et 'lng' sont optionnels et peuvent servir d'indices."
        expl_label = QLabel(explanation_text)
        expl_label.setStyleSheet("font-style: italic; font-size: 9pt;")
        layout.addWidget(expl_label)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept_mapping)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
        self.setMinimumWidth(450)

    def accept_mapping(self):
        current_mapping: dict[str, str] = {}
        source_columns_used = set()
        duplicate_source_error = ""

        for target_col, combo in self.column_vars_combos.items():
            selected_source = combo.currentText()
            current_mapping[target_col] = selected_source

            if selected_source != '<Ignorer>':
                if selected_source in source_columns_used:
                    duplicate_source_error = f"La colonne source '{selected_source}' ne peut pas être mappée à plusieurs cibles."
                    targets = [t for t, s in current_mapping.items() if s == selected_source]
                    duplicate_source_error += f" (Utilisée pour: {', '.join(targets)})"
                    break
                source_columns_used.add(selected_source)

        if duplicate_source_error:
            QMessageBox.warning(self, "Mapping Invalide", duplicate_source_error)
            return

        error_messages = []
        missing_strictly_required = [
            col for col in MANDATORY_COLS_FOR_MAPPING_DIALOG_GEO
            if current_mapping.get(col) == '<Ignorer>' or not current_mapping.get(col)
        ]
        if missing_strictly_required:
            error_messages.append(f"La colonne essentielle 'id' doit être mappée.")

        if error_messages:
            QMessageBox.warning(self, "Champs Essentiels Non Mappés", "\n".join(error_messages))
            return

        self.mapping_result = current_mapping
        self.accept()

    def get_mapping(self) -> dict[str, str] | None:
        if self.exec() == QDialog.DialogCode.Accepted:
            return self.mapping_result
        return None

# ==============================================================================
# Helper functions for geocode_single_address hints
# ==============================================================================
def create_bounds_from_point(lat, lng, delta=0.01): # delta for ~1km radius approx
    """Creates a 'bounds' dictionary for Google Maps API."""
    try:
        lat, lng = float(lat), float(lng)
        return {
            'northeast': {'lat': lat + delta, 'lng': lng + delta},
            'southwest': {'lat': lat - delta, 'lng': lng - delta}
        }
    except (ValueError, TypeError):
        return None

def create_viewbox_from_point(lat, lng, delta=0.01):
    """Creates a 'viewbox' list for Nominatim API: [lon_min, lat_min, lon_max, lat_max]."""
    try:
        lat, lng = float(lat), float(lng)
        return [lng - delta, lat - delta, lng + delta, lat + delta]
    except (ValueError, TypeError):
        return None

# ==============================================================================
# Worker Class
# ==============================================================================
class Worker(QObject):
    def __init__(self, input_file_path, selected_sheet_name, output_dir_path, geocoder_choice, country, initial_checkpoint_data, job_id, user_column_mapping):
        super().__init__()
        self.signals = WorkerSignals()
        self.input_file_path = input_file_path
        self.selected_sheet_name = selected_sheet_name
        self.output_dir_path = output_dir_path
        self.selected_geocoder = geocoder_choice
        self.country_input = country
        self.initial_checkpoint_data = initial_checkpoint_data.copy()
        self.current_job_id = job_id
        self.user_column_mapping = user_column_mapping
        self._is_paused = False
        self._cancel_requested = False
        self.gmaps_client = None
        self.geocode_func = None
        self.df_original_source = None
        self.df_geocoding_input = None
        self.current_checkpoint_data = self.initial_checkpoint_data.copy()
        self._geocode_etabs_fyre_completed = False

    @pyqtSlot()
    def request_cancel(self):
        self.log_status_worker("Annulation demandée par l'utilisateur.", logging.WARNING)
        self._cancel_requested = True
        self.signals.state_change.emit(True, self._is_paused, self._cancel_requested)

    @pyqtSlot()
    def toggle_pause(self):
        self._is_paused = not self._is_paused
        if self._is_paused:
            self.log_status_worker("Traitement mis en PAUSE.", logging.INFO)
        else:
            self.log_status_worker("Traitement REPRIS.", logging.INFO)
        self.signals.state_change.emit(True, self._is_paused, self._cancel_requested)

    def log_status_worker(self, message, level=logging.INFO):
        logging.log(level, message)
        if hasattr(self, 'signals') and self.signals:
             try:
                self.signals.log_message.emit(message, level)
             except RuntimeError:
                logging.warning(f"Failed to emit log message (RuntimeError): {message}")
             except Exception as e_log:
                logging.error(f"Unexpected error emitting log message: {e_log}")

    def init_cache_db(self):
        try:
            conn = sqlite3.connect(str(CACHE_DB_FILE), check_same_thread=False)
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS geocode_cache
                         (address_hash TEXT PRIMARY KEY, lat REAL, lng REAL,
                          geocoder_used TEXT, raw_response TEXT,
                          timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            try:
                c.execute("ALTER TABLE geocode_cache ADD COLUMN raw_response TEXT")
            except sqlite3.OperationalError:
                pass
            c.execute("CREATE INDEX IF NOT EXISTS idx_address_hash ON geocode_cache (address_hash)")
            conn.commit()
            conn.close()
            self.log_status_worker("Cache DB initialisé (Google Places uniquement).", logging.DEBUG)
        except sqlite3.Error as e:
            self.log_status_worker(f"Erreur SQLite lors de l'initialisation du cache: {e}", logging.ERROR)

    def get_coordinates_from_cache(self, address_hash):
        if not address_hash:
            return None
        conn = None
        try:
            conn = sqlite3.connect(str(CACHE_DB_FILE), timeout=10, check_same_thread=False)
            c = conn.cursor()
            c.execute("SELECT raw_response FROM geocode_cache WHERE address_hash = ? AND geocoder_used = 'Google Places'", (address_hash,))
            result = c.fetchone()
            conn.close()
            if result and result[0]:
                self.log_status_worker(f"Cache hit (Google) pour hash {address_hash[:7]}...", logging.DEBUG)
                return result[0]
            else:
                return None
        except sqlite3.Error as e:
            self.log_status_worker(f"Erreur SQLite lecture cache (Google) pour {address_hash[:7]}: {e}", logging.ERROR)
            if conn:
                try: conn.close()
                except sqlite3.Error: pass
            return None

    def save_to_cache(self, address_hash, lat, lng, geocoder_name, raw_response_str=None):
        if not address_hash or geocoder_name != "Google Places" or raw_response_str is None:
            return
        conn = None
        try:
            conn = sqlite3.connect(str(CACHE_DB_FILE), timeout=10, check_same_thread=False)
            c = conn.cursor()
            c.execute("""INSERT OR REPLACE INTO geocode_cache
                         (address_hash, lat, lng, geocoder_used, raw_response, timestamp)
                         VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                      (address_hash, lat, lng, geocoder_name, raw_response_str))
            conn.commit()
            conn.close()
            self.log_status_worker(f"Cache saved (Google) pour hash {address_hash[:7]}...", logging.DEBUG)
        except sqlite3.Error as e:
            self.log_status_worker(f"Erreur SQLite écriture cache (Google) pour {address_hash[:7]}: {e}", logging.ERROR)
            if conn:
                try: conn.close()
                except sqlite3.Error: pass

    def get_checkpoint_file(self, job_id):
        return str(script_dir / f"{job_id}{CHECKPOINT_SUFFIX}")

    def get_temp_file(self, job_id):
        return str(script_dir / f"{job_id}{TEMP_SUFFIX}")

    def save_checkpoint(self, df_to_save, processed_count, total_rows):
        if not self.current_job_id:
            self.log_status_worker("Impossible de sauvegarder checkpoint: Job ID non défini.", logging.ERROR)
            return False
        if df_to_save is None:
            self.log_status_worker("Impossible de sauvegarder checkpoint: DataFrame non disponible.", logging.ERROR)
            return False

        checkpoint_file = self.get_checkpoint_file(self.current_job_id)
        temp_excel_file = self.get_temp_file(self.current_job_id)
        self.log_status_worker(f"💾 Sauvegarde checkpoint: {processed_count}/{total_rows}...", logging.DEBUG)

        try:
            df_copy = df_to_save.copy()
            required_output_cols = list(BASE_OUTPUT_COLS_GEOCODER)

            output_geocoder = self.selected_geocoder
            if self.current_checkpoint_data and self.current_checkpoint_data.get('job_id') == self.current_job_id:
                 checkpoint_geocoder_used = self.current_checkpoint_data.get('geocoder')
                 if checkpoint_geocoder_used : output_geocoder = checkpoint_geocoder_used

            if 'lat' in df_copy.columns: required_output_cols.append('lat')
            if 'lng' in df_copy.columns: required_output_cols.append('lng')

            if output_geocoder not in ["Etabs", "geocodeEtabs"]:
                if 'lat' not in df_copy.columns: df_copy['lat'] = pd.NA
                if 'lng' not in df_copy.columns: df_copy['lng'] = pd.NA
                if 'lat' not in required_output_cols: required_output_cols.append('lat')
                if 'lng' not in required_output_cols: required_output_cols.append('lng')

                if output_geocoder == 'Nominatim':
                    required_output_cols.extend([c for c in NOMINATIM_OUTPUT_COLS if c in df_copy.columns])
                if output_geocoder in ['Fyre', 'Here']:
                    fyre_here_cols = ['gmap_address', 'gmap_street', 'gmap_region', 'gmap_state', 'gmap_zipcode', 'gmap_city', 'gmap_country', 'gmap_score_name', 'gmap_score_address', 'gmap_automatch']
                    required_output_cols.extend([c for c in fyre_here_cols if c in df_copy.columns])
                if output_geocoder == 'GmapScraper':
                     gmapscraper_cols = [col for col in df_copy.columns if col.startswith('gmap_')]
                     required_output_cols.extend(gmapscraper_cols)

                for col in ['lat', 'lng']:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

            if 'full_address' in df_copy.columns and 'full_address' not in required_output_cols:
                 required_output_cols.append('full_address')

            cols_to_keep = list(dict.fromkeys(required_output_cols))
            final_cols_to_keep = [col for col in cols_to_keep if col in df_copy.columns]

            df_for_checkpoint = df_copy[final_cols_to_keep].copy()
            df_for_checkpoint.to_excel(temp_excel_file, index=False, engine='openpyxl', na_rep='')
            self.log_status_worker(f"📄 Excel temporaire sauvegardé: {os.path.basename(temp_excel_file)}", logging.DEBUG)
        except Exception as e:
            self.log_status_worker(f"❌ Erreur sauvegarde Excel temporaire '{os.path.basename(temp_excel_file)}': {e}", logging.ERROR)
            logging.exception("Erreur sauvegarde Excel temp:")
            return False

        try:
            abs_input_path = os.path.abspath(self.input_file_path)
            checkpoint_info = {
                "job_id": self.current_job_id,
                "file_path": abs_input_path,
                "sheet_name": self.selected_sheet_name,
                "temp_file": temp_excel_file,
                "processed": processed_count,
                "total": total_rows,
                "timestamp": time.time(),
                "geocoder": self.selected_geocoder,
                "column_mapping": self.user_column_mapping
            }
            if self.selected_geocoder == "geocodeEtabs" and hasattr(self, '_geocode_etabs_fyre_completed') and self._geocode_etabs_fyre_completed:
                checkpoint_info["geocode_etabs_stage"] = "etabs_pending"

            temp_json_path = checkpoint_file + ".tmp"
            with open(temp_json_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_info, f, indent=4)
            os.replace(temp_json_path, checkpoint_file)

            self.current_checkpoint_data = checkpoint_info
            self.log_status_worker(f"💾 Checkpoint JSON sauvegardé ({processed_count}/{total_rows}).", logging.INFO)
            return True
        except Exception as e:
            self.log_status_worker(f"❌ Erreur sauvegarde JSON checkpoint '{os.path.basename(checkpoint_file)}': {e}", logging.ERROR)
            logging.exception("Erreur sauvegarde JSON Checkpoint:")
            if 'temp_json_path' in locals() and os.path.exists(temp_json_path):
                try: os.remove(temp_json_path);
                except OSError: pass
            return False

    def cleanup_temp_files(self, job_id_to_clean):
        if not job_id_to_clean:
            return
        checkpoint_file = self.get_checkpoint_file(job_id_to_clean)
        temp_excel_file = self.get_temp_file(job_id_to_clean)
        temp_json_file = checkpoint_file + ".tmp" if checkpoint_file else None

        files_to_remove = [f for f in [checkpoint_file, temp_excel_file, temp_json_file] if f and isinstance(f, str)]

        self.log_status_worker(f"🧹 Nettoyage fichiers temporaires pour Job ID: {job_id_to_clean}", logging.DEBUG)
        for f_path in files_to_remove:
            try:
                if os.path.exists(f_path):
                    os.remove(f_path)
                    self.log_status_worker(f"🗑️ Fichier supprimé: {os.path.basename(f_path)}", logging.DEBUG)
            except OSError as e:
                self.log_status_worker(f"❌ Erreur suppression fichier {os.path.basename(f_path)}: {e}", logging.ERROR)

    def geocode_batch_api_worker(self, index, payload, address_hash, api_endpoint_url):
        if not FYRE_API_KEY:
            return index, address_hash, None, f"Worker {index}: ❌ Clé API Fyre/Here/Etabs manquante.", logging.ERROR
        if not api_endpoint_url:
            return index, address_hash, None, f"Worker {index}: ❌ URL Endpoint API manquante.", logging.ERROR

        headers = {"Authorization": f"Bearer {FYRE_API_KEY}", "Content-Type": "application/json"}
        address_display = str(payload.get('full_address', payload.get('address', f'Index {index}')))[:50]

        api_name = "Unknown"
        if "api/hexa" in api_endpoint_url: api_name = "Fyre (Hexa)"
        elif "api/bing" in api_endpoint_url: api_name = "Here (Bing)"
        elif "api/etabs" in api_endpoint_url: api_name = "Etabs"

        worker_log_prefix = f"Thread {threading.current_thread().name} ({api_name} - {address_display}...):"

        try:
            if api_name == "Etabs" and 'name' not in payload:
                payload['name'] = ""
                self.log_status_worker(f"{worker_log_prefix} ℹ️ Champ 'name' absent, ajout de 'name: \"\"' pour l'API Etabs.", logging.DEBUG)

            # MODIFIÉ : Ajout de verify=False pour ignorer la vérification du certificat SSL
            response = requests.post(api_endpoint_url, headers=headers, json=payload, timeout=30, verify=False)
            response.raise_for_status()

            raw_text = response.text.strip()
            if raw_text.startswith('\ufeff'):
                raw_text = raw_text[1:]

            if raw_text and not raw_text.isspace():
                try:
                    data = json.loads(raw_text)
                    self.log_status_worker(f"{worker_log_prefix} ✨ Réponse API {api_name} reçue (Taille: {len(raw_text)}).", logging.DEBUG)
                    return index, address_hash, data, f"{worker_log_prefix} ✅ Succès API {api_name}.", logging.DEBUG
                except json.JSONDecodeError as e:
                    err_msg = f"{worker_log_prefix} ❌ Erreur JSON {api_name}: {e}. Réponse (tronquée): '{raw_text[:100]}'"
                    return index, address_hash, None, err_msg, logging.ERROR
            else:
                warn_msg = f"{worker_log_prefix} ⚠️ Réponse vide {api_name}."
                log_level = logging.INFO if api_name == "Etabs" else logging.WARNING
                return index, address_hash, None, warn_msg, log_level

        except requests.exceptions.Timeout:
            warn_msg = f"{worker_log_prefix} ⚠️ Timeout API {api_name}."
            return index, address_hash, None, warn_msg, logging.WARNING
        except requests.exceptions.HTTPError as e:
            response_text = e.response.text[:100] if e.response else ""
            err_msg = f"{worker_log_prefix} ❌ Erreur HTTP {e.response.status_code} {api_name}: {response_text}"
            return index, address_hash, None, err_msg, logging.ERROR
        except requests.exceptions.RequestException as e:
            err_msg = f"{worker_log_prefix} ❌ Erreur Réseau {api_name}: {e}"
            return index, address_hash, None, err_msg, logging.ERROR
        except Exception as e:
            logging.exception(f"Traceback Erreur {api_name} Worker pour Index {index}")
            err_msg = f"{worker_log_prefix} 💥 Erreur inattendue {api_name}: {e}"
            return index, address_hash, None, err_msg, logging.ERROR

    def geocode_single_address(self, address, source_lat=None, source_lng=None):
        result_dict = {'lat': None, 'lng': None}
        if not address or pd.isna(address):
            self.log_status_worker(f"⚠️ Adresse vide/invalide fournie pour géocodage single: {address}", logging.WARNING)
            return result_dict

        address_hash = None
        try:
            address_slug = slugify(str(address).lower())
            address_hash = hashlib.sha1(address_slug.encode('utf-8')).hexdigest()
        except Exception as e:
            self.log_status_worker(f"❌ Erreur hashage adresse '{str(address)[:60]}...': {e}", logging.ERROR)

        geocoder_name = self.selected_geocoder
        raw_google_response_str = None

        if geocoder_name == "Google Places" and address_hash:
            cached_raw_response = self.get_coordinates_from_cache(address_hash)
            if cached_raw_response is not None:
                try:
                    cached_data = json.loads(cached_raw_response)
                    if cached_data and isinstance(cached_data, list) and len(cached_data) > 0:
                        loc = cached_data[0].get('geometry', {}).get('location')
                        if loc and 'lat' in loc and 'lng' in loc:
                            result_dict['lat'] = loc['lat']
                            result_dict['lng'] = loc['lng']
                            self.log_status_worker(f"✨ Cache (Google) utilisé pour {address_hash[:7]}...", logging.DEBUG)
                            return result_dict
                    self.log_status_worker(f"⚠️ Cache Google pour {address_hash[:7]} invalide. Re-fetching.", logging.WARNING)
                except json.JSONDecodeError:
                     self.log_status_worker(f"❌ Erreur JSON décodage cache Google pour {address_hash[:7]}. Re-fetching.", logging.ERROR)
                except Exception as e:
                     self.log_status_worker(f"❌ Erreur traitement cache Google pour {address_hash[:7]}: {e}. Re-fetching.", logging.ERROR)

        lat, lng = None, None
        primary_geocoder_failed = False
        nominatim_details = {}

        gmaps_bounds_hint = None
        nominatim_viewbox_hint = None

        valid_source_coords = False
        try:
            s_lat_float = float(source_lat)
            s_lng_float = float(source_lng)
            if pd.notna(s_lat_float) and pd.notna(s_lng_float):
                valid_source_coords = True
                if geocoder_name == "Google Places":
                    gmaps_bounds_hint = create_bounds_from_point(s_lat_float, s_lng_float)
                elif geocoder_name == "Nominatim":
                    nominatim_viewbox_hint = create_viewbox_from_point(s_lat_float, s_lng_float)
        except (ValueError, TypeError):
            pass

        try:
            self.log_status_worker(f"🗺️ API ({geocoder_name}): {str(address)[:60]}...", logging.DEBUG)
            if geocoder_name == "Nominatim":
                query_params = {'timeout': 10}
                if nominatim_viewbox_hint:
                    query_params['viewbox'] = nominatim_viewbox_hint
                    query_params['bounded'] = 1
                    self.log_status_worker(f"  ➡️ Nominatim: Using viewbox hint from source lat/lng.", logging.DEBUG)

                location = self.geocode_func(address, **query_params)
                if location:
                    lat, lng = location.latitude, location.longitude
                    raw_addr = location.raw.get('address', {})
                    nominatim_details['google_address'] = location.address
                    nominatim_details['google_street'] = raw_addr.get('road')
                    nominatim_details['google_region'] = raw_addr.get('state_district', raw_addr.get('region'))
                    nominatim_details['google_state'] = raw_addr.get('state')
                    nominatim_details['google_zipcode'] = raw_addr.get('postcode')
                    nominatim_details['google_city'] = raw_addr.get('city', raw_addr.get('town', raw_addr.get('village')))
                    nominatim_details['google_country'] = raw_addr.get('country')
                    self.log_status_worker(f"✅ Géocodage single ({geocoder_name}) succès.", logging.DEBUG)
                else:
                    primary_geocoder_failed = True
                    self.log_status_worker(f"ℹ️ Géocodage single ({geocoder_name}) n'a pas trouvé de résultat pour '{str(address)[:60]}...'.", logging.INFO)

            elif geocoder_name == "Google Places":
                query_params_g = {}
                if gmaps_bounds_hint:
                    query_params_g['bounds'] = gmaps_bounds_hint
                    self.log_status_worker(f"  ➡️ Google Places: Using bounds hint from source lat/lng.", logging.DEBUG)

                google_result = self.geocode_func(address, **query_params_g)
                if google_result and isinstance(google_result, list) and len(google_result) > 0:
                    loc = google_result[0].get('geometry', {}).get('location')
                    if loc and 'lat' in loc and 'lng' in loc:
                        result_dict['lat'] = loc['lat']
                        result_dict['lng'] = loc['lng']
                        lat, lng = result_dict['lat'], result_dict['lng']
                        try: raw_google_response_str = json.dumps(google_result)
                        except Exception: raw_google_response_str = None; self.log_status_worker(f"⚠️ Impossible sérialiser réponse Google pour cache ({address_hash[:7]}).", logging.WARNING)
                        self.log_status_worker(f"✅ Géocodage single ({geocoder_name}) succès.", logging.DEBUG)
                    else:
                        primary_geocoder_failed = True
                        self.log_status_worker(f"ℹ️ Géocodage single ({geocoder_name}) succès mais sans coordonnées pour '{str(address)[:60]}...'.", logging.INFO)
                else:
                    primary_geocoder_failed = True
                    self.log_status_worker(f"ℹ️ Géocodage single ({geocoder_name}) n'a pas trouvé de résultat pour '{str(address)[:60]}...'.", logging.INFO)

            elif geocoder_name in ["Fyre", "Here", "Etabs", "geocodeEtabs", "GmapScraper"]:
                self.log_status_worker(f"⚠️ Avertissement: Appel inattendu à geocode_single_address pour {geocoder_name}.", logging.WARNING)
                primary_geocoder_failed = True

            if lat is None or lng is None: primary_geocoder_failed = True

        except (GeocoderTimedOut, Timeout) as e:
            self.log_status_worker(f"⚠️ Timeout API {geocoder_name} pour {str(address)[:60]}...: {e}", logging.WARNING)
            primary_geocoder_failed = True
        except (GeocoderServiceError, ApiError, TransportError) as e:
            self.log_status_worker(f"❌ Erreur Service/API {geocoder_name} pour {str(address)[:60]}...: {e}", logging.ERROR)
            primary_geocoder_failed = True
        except Exception as e:
            self.log_status_worker(f"💥 Erreur inattendue géocodage {geocoder_name} pour {str(address)[:60]}...: {e}", logging.ERROR)
            logging.exception(f"Traceback Erreur Géocodage Standard pour {address}")
            primary_geocoder_failed = True

        if primary_geocoder_failed and FYRE_API_KEY and geocoder_name not in ["Fyre", "Here", "Etabs", "geocodeEtabs", "GmapScraper"]:
            self.log_status_worker(f"🔄 Fallback Fyre pour: {str(address)[:60]}...", logging.INFO)
            headers = {"Authorization": f"Bearer {FYRE_API_KEY}", "Content-Type": "application/json"}
            payload = {"full_address": address}
            if valid_source_coords:
                try:
                    payload['lat'] = float(source_lat)
                    payload['lng'] = float(source_lng)
                    self.log_status_worker(f"  ➡️ Fallback Fyre: Using source lat/lng hint.", logging.DEBUG)
                except (ValueError, TypeError): pass

            try:
                # MODIFIÉ : Ajout de verify=False pour ignorer la vérification du certificat SSL
                response = requests.post(FYRE_API_ENDPOINT, headers=headers, json=payload, timeout=15, verify=False)
                response.raise_for_status()
                raw_text_fyre = response.text.strip();
                if raw_text_fyre.startswith('\ufeff'): raw_text_fyre = raw_text_fyre[1:]

                if raw_text_fyre and not raw_text_fyre.isspace():
                    data_fyre = json.loads(raw_text_fyre)
                    fyre_lat, fyre_lng = data_fyre.get("lat"), data_fyre.get("lng")
                    if fyre_lat is not None and fyre_lng is not None:
                        try:
                            lat, lng = float(fyre_lat), float(fyre_lng)
                            self.log_status_worker(f"✅ Fallback Fyre: Trouvé ({lat:.5f}, {lng:.5f})", logging.INFO)
                            nominatim_details = {}
                            raw_google_response_str = None
                        except (ValueError, TypeError):
                             self.log_status_worker(f"⚠️ Fallback Fyre: Lat/Lng non numériques ({fyre_lat}, {fyre_lng}).", logging.WARNING)
                             lat, lng = None, None
            except requests.exceptions.RequestException as e: self.log_status_worker(f"❌ Erreur Fallback Fyre (Réseau/HTTP): {e}", logging.ERROR)
            except json.JSONDecodeError as e: self.log_status_worker(f"❌ Erreur Fallback Fyre (JSON): {e}", logging.ERROR)
            except Exception as e: self.log_status_worker(f"❌ Erreur Fallback Fyre (Inattendue): {e}", logging.ERROR)

        if lat is not None and lng is not None:
            result_dict['lat'], result_dict['lng'] = lat, lng
            if geocoder_name == "Nominatim" and nominatim_details and not primary_geocoder_failed:
                result_dict.update(nominatim_details)
            if geocoder_name == "Google Places" and not primary_geocoder_failed and address_hash and raw_google_response_str:
                try:
                    self.save_to_cache(address_hash, float(lat), float(lng), geocoder_name, raw_google_response_str)
                except (ValueError, TypeError): self.log_status_worker(f"⚠️ N'a pas pu sauvegarder cache Google pour {address_hash[:7]}: Lat/Lng non numériques.", logging.WARNING)
                except Exception as e_cache: self.log_status_worker(f"❌ Erreur inattendue sauvegarde cache Google pour {address_hash[:7]}: {e_cache}", logging.ERROR)

        return result_dict

    def apply_mapping_to_df(self, original_df: pd.DataFrame, column_mapping: dict[str, str]) -> pd.DataFrame:
        self.log_status_worker(f"⚙️ Application du mapping des colonnes: {column_mapping}", logging.DEBUG)
        df_with_target_cols = pd.DataFrame()
        original_df_cols_as_str = {str(col): col for col in original_df.columns}

        for target_col in TARGET_COLS_GEOCODER:
            df_with_target_cols[target_col] = pd.NA

        for target_col in TARGET_COLS_GEOCODER:
            source_col_from_mapping = column_mapping.get(target_col)
            if source_col_from_mapping and source_col_from_mapping != '<Ignorer>':
                if source_col_from_mapping in original_df_cols_as_str:
                    original_col_name = original_df_cols_as_str[source_col_from_mapping]
                    df_with_target_cols[target_col] = original_df[original_col_name].copy()
                    self.log_status_worker(f" Mappé: '{target_col}' <- '{original_col_name}'", logging.DEBUG)
                else:
                    self.log_status_worker(f"⚠️ Colonne source '{source_col_from_mapping}' pour cible '{target_col}' non trouvée. Utilisation de NA.", logging.WARNING)
            else:
                if not source_col_from_mapping:
                    self.log_status_worker(f"ℹ️ Colonne cible '{target_col}' non mappée explicitement. Utilisation de NA.", logging.DEBUG)
                else:
                    self.log_status_worker(f"ℹ️ Colonne cible '{target_col}' ignorée par l'utilisateur. Utilisation de NA.", logging.DEBUG)

        for col_name in ['lat', 'lng']:
            if col_name in df_with_target_cols:
                df_with_target_cols[col_name] = pd.to_numeric(df_with_target_cols[col_name], errors='coerce')

        string_cols_in_target = [tc for tc in TARGET_COLS_GEOCODER if tc not in ['lat', 'lng']]
        for col in string_cols_in_target:
            if col in df_with_target_cols:
                df_with_target_cols[col] = df_with_target_cols[col].fillna("")

        self.log_status_worker(f"📊 Colonnes après mapping: {df_with_target_cols.columns.tolist()}", logging.DEBUG)
        return df_with_target_cols

    @pyqtSlot()
    def run_geocoding_task(self):
        self.log_status_worker(f"▶️ Thread worker démarré pour {self.selected_geocoder}.", logging.INFO)
        self.signals.state_change.emit(True, self._is_paused, self._cancel_requested)
        self.init_cache_db()

        not_geocoded_count = 0
        etabs_results_list = []
        df_source = None
        df_mapped_for_processing = None
        starting_index = 0
        total_rows = 0
        main_loop_error = None
        current_row_index = 0
        self.df_original_source = None
        self.df_geocoding_input = None
        self._geocode_etabs_fyre_completed = False
        source_df_for_etabs_stage2 = None

        try:
            resuming_geocode_etabs_etabs_stage = False

            if self.current_checkpoint_data and self.current_checkpoint_data.get('job_id') == self.current_job_id:
                self.log_status_worker("🔄 Tentative de reprise depuis checkpoint valide...", logging.INFO)
                temp_file_from_checkpoint = self.current_checkpoint_data.get('temp_file')
                mapping_from_checkpoint = self.current_checkpoint_data.get('column_mapping')

                if not isinstance(mapping_from_checkpoint, dict):
                    self.log_status_worker(f"❌ Erreur checkpoint: Mapping invalide ou manquant. Abandon de la reprise.", logging.ERROR)
                    self.cleanup_temp_files(self.current_job_id); self.current_checkpoint_data = {}
                    error_msg_map = "Mapping des colonnes manquant ou invalide dans le checkpoint.\nLe checkpoint a été supprimé."
                    self.signals.show_message.emit("error", "Erreur Reprise (Mapping)", error_msg_map)
                    raise ValueError("Mapping invalide dans checkpoint.")

                if temp_file_from_checkpoint and os.path.exists(temp_file_from_checkpoint):
                    try:
                        self.log_status_worker(f"📄 Chargement état depuis: {os.path.basename(temp_file_from_checkpoint)}", logging.INFO)
                        df_mapped_for_processing = pd.read_excel(temp_file_from_checkpoint, engine='openpyxl', dtype=object)

                        for col_name_num_chkpt in ['lat', 'lng']:
                            if col_name_num_chkpt in df_mapped_for_processing.columns:
                                df_mapped_for_processing[col_name_num_chkpt] = pd.to_numeric(df_mapped_for_processing[col_name_num_chkpt], errors='coerce')

                        starting_index = self.current_checkpoint_data.get('processed', 0)
                        total_rows = self.current_checkpoint_data.get('total', len(df_mapped_for_processing))
                        self.user_column_mapping = mapping_from_checkpoint
                        self.log_status_worker(f" Mappage chargé du checkpoint: {self.user_column_mapping}", logging.DEBUG)

                        if total_rows != len(df_mapped_for_processing):
                            self.log_status_worker(f"⚠️ Le nombre total de lignes du checkpoint ({total_rows}) diffère ({len(df_mapped_for_processing)}). Utilisation de {len(df_mapped_for_processing)}.", logging.WARNING)
                            self.current_checkpoint_data['total'] = len(df_mapped_for_processing)
                            total_rows = len(df_mapped_for_processing)

                        if starting_index >= total_rows and total_rows > 0:
                             self.log_status_worker(f"⚠️ Index de reprise ({starting_index}) >= total ({total_rows}). Redémarrage à 0.", logging.WARNING)
                             starting_index = 0; self.current_checkpoint_data['processed'] = 0

                        if self.selected_geocoder == "geocodeEtabs" and self.current_checkpoint_data.get("geocode_etabs_stage") == "etabs_pending":
                            self.log_status_worker(f"🔄 Reprise geocodeEtabs: Étape Fyre terminée, passage à Etabs.", logging.INFO)
                            self._geocode_etabs_fyre_completed = True
                            resuming_geocode_etabs_etabs_stage = True
                        else:
                            self.log_status_worker(f"⏩ Reprise à la ligne {starting_index + 1} / {total_rows}.", logging.INFO)

                        checkpoint_geocoder = self.current_checkpoint_data.get('geocoder')
                        status_label_msg = "Non géocodés: 0"

                        if checkpoint_geocoder not in ["Etabs", "GmapScraper", "geocodeEtabs"] and starting_index > 0:
                            if 'lat' in df_mapped_for_processing.columns and 'lng' in df_mapped_for_processing.columns:
                                try:
                                    valid_coords = pd.notna(df_mapped_for_processing.loc[:starting_index-1, 'lat']) & pd.notna(df_mapped_for_processing.loc[:starting_index-1, 'lng'])
                                    not_geocoded_count = starting_index - valid_coords.sum()
                                    status_label_msg = f"❌ Non géocodés ({checkpoint_geocoder}): {not_geocoded_count}"
                                except Exception as e_ngc: self.log_status_worker(f"⚠️ Erreur calcul non-géocodés (reprise): {e_ngc}", logging.WARNING)
                        elif checkpoint_geocoder == "GmapScraper" and starting_index > 0:
                            if 'gmap_error' in df_mapped_for_processing.columns: successful_scrapes = df_mapped_for_processing.loc[:starting_index-1, 'gmap_error'].isna().sum()
                            elif 'gmap_lat' in df_mapped_for_processing.columns: successful_scrapes = df_mapped_for_processing.loc[:starting_index-1, 'gmap_lat'].notna().sum()
                            else: successful_scrapes = 0
                            status_label_msg = f"✅ GmapScraper: {successful_scrapes} succès (reprise)"
                        elif checkpoint_geocoder == "Etabs":
                             status_label_msg = "✅ Résultats Etabs: (Reprise)"

                        progress_percent = (starting_index / total_rows * 100) if total_rows > 0 else 0
                        eta_str = "⏱️ ETA: Prêt (Reprise)"

                        if self.selected_geocoder == "geocodeEtabs" and resuming_geocode_etabs_etabs_stage:
                            progress_percent_display = 50 + (progress_percent * 0.5)
                            status_label_msg = "✅ Résultats Etabs: (Reprise étape Etabs)"
                        else:
                            progress_percent_display = progress_percent

                        self.signals.progress_update.emit((progress_percent_display, f"⏳ {starting_index} / {total_rows} traités [{int(progress_percent_display)}%]", eta_str, status_label_msg))

                        if checkpoint_geocoder != self.selected_geocoder:
                            warn_msg = (f"⚠️ AVERTISSEMENT: Le géocodeur sélectionné ({self.selected_geocoder}) diffère "
                                        f"de celui du checkpoint ({checkpoint_geocoder}).\n"
                                        f"La reprise continuera avec '{self.selected_geocoder}', mais les résultats précédents "
                                        f"(avant {starting_index + 1}) proviennent de '{checkpoint_geocoder}'.")
                            self.log_status_worker(warn_msg, logging.CRITICAL)
                            self.signals.show_message.emit("warning", "Incohérence de Géocodeur", warn_msg)

                    except Exception as e:
                        self.log_status_worker(f"❌ Erreur chargement fichier Excel temporaire: {e}. Abandon de la reprise.", logging.ERROR)
                        df_mapped_for_processing = None; self.current_checkpoint_data = {}
                        self.cleanup_temp_files(self.current_job_id)
                        error_msg = f"Impossible de lire le fichier de reprise:\n{e}\n\nLe checkpoint a été supprimé."
                        self.signals.show_message.emit("error", "Erreur de Reprise", error_msg)
                        raise
                else:
                    self.log_status_worker(f"⚠️ Fichier temporaire Excel '{temp_file_from_checkpoint}' manquant (mentionné dans checkpoint). Abandon de la reprise.", logging.WARNING)
                    df_mapped_for_processing = None; self.current_checkpoint_data = {}
                    self.cleanup_temp_files(self.current_job_id)
                    error_msg = "Fichier de reprise Excel manquant.\nLe checkpoint a été supprimé."
                    self.signals.show_message.emit("warning", "Erreur de Reprise", error_msg)
                    raise ValueError("Fichier Excel de reprise manquant.")

            if df_mapped_for_processing is None:
                starting_index = 0; not_geocoded_count = 0
                if not self.user_column_mapping:
                     raise ValueError("Erreur interne: Tentative de démarrage sans mapping utilisateur défini.")

                log_msg = f"📄 Chargement initial depuis: {self.input_file_path}"
                is_excel_load = str(self.input_file_path).lower().endswith(('.xlsx', '.xls'))
                if is_excel_load and self.selected_sheet_name is not None:
                    log_msg += f" (Onglet: '{self.selected_sheet_name}')"
                log_msg += "..."
                self.log_status_worker(log_msg, logging.INFO)

                try:
                    file_ext = Path(self.input_file_path).suffix.lower()
                    read_opts = {'dtype': object, 'engine': 'openpyxl'}
                    if is_excel_load and self.selected_sheet_name is not None:
                        read_opts['sheet_name'] = self.selected_sheet_name
                    csv_read_opts = {'dtype': object, 'low_memory': False}

                    if file_ext == '.csv':
                        try: df_source = pd.read_csv(self.input_file_path, sep=None, engine='python', encoding='utf-8-sig', **csv_read_opts)
                        except Exception:
                           try: df_source = pd.read_csv(self.input_file_path, sep=';', engine='python', encoding='utf-8-sig', **csv_read_opts)
                           except Exception:
                              try: df_source = pd.read_csv(self.input_file_path, sep=';', engine='python', encoding='iso-8859-1', **csv_read_opts)
                              except Exception:
                                  try: df_source = pd.read_csv(self.input_file_path, sep=';', engine='python', encoding='cp1252', **csv_read_opts)
                                  except Exception as e_last_csv: raise ValueError(f"Impossible lire CSV avec encodages/séparateurs communs: {e_last_csv}") from e_last_csv
                    elif file_ext in ['.xlsx', '.xls']:
                        df_source = pd.read_excel(self.input_file_path, **read_opts)
                    else:
                        raise ValueError(f"Format de fichier non supporté: {file_ext}")

                    if df_source is None or df_source.empty:
                         raise ValueError("Le fichier source chargé est vide.")

                    total_rows = len(df_source)
                    self.log_status_worker(f"📄 Fichier source chargé ({total_rows} lignes). Application du mapping...", logging.INFO)
                    df_mapped_for_processing = self.apply_mapping_to_df(df_source, self.user_column_mapping)
                    
                    # --- NOUVELLE LOGIQUE DE VALIDATION (Ligne par Ligne) ---
                    self.log_status_worker("⚙️ Validation des données essentielles...", logging.INFO)

                    # Condition A: La ligne a un 'hexa' valide (si la colonne existe).
                    if 'hexa' in df_mapped_for_processing.columns:
                        has_valid_hexa = df_mapped_for_processing['hexa'].fillna('').astype(str).str.strip().ne('')
                    else:
                        # Si 'hexa' n'est pas mappé, aucune ligne ne peut être validée par ce critère.
                        has_valid_hexa = pd.Series([False] * len(df_mapped_for_processing), index=df_mapped_for_processing.index)

                    # Condition B: La ligne a un ensemble d'adresse complet.
                    has_valid_address = df_mapped_for_processing['address'].fillna('').astype(str).str.strip().ne('')
                    has_valid_zip = df_mapped_for_processing['zipcode'].fillna('').astype(str).str.strip().ne('')
                    has_valid_city = df_mapped_for_processing['city'].fillna('').astype(str).str.strip().ne('')
                    has_complete_address = has_valid_address & has_valid_zip & has_valid_city
                    has_complete_address = True

                    # Une ligne est valide si elle remplit la condition A OU la condition B.
                    is_row_valid = has_valid_hexa | has_complete_address

                    if not is_row_valid.all():
                        first_invalid_index = is_row_valid[~is_row_valid].index[0]
                        error_msg_map_val = (
                            f"Données invalides dans le fichier à la ligne ~{first_invalid_index + 2}.\n\n"
                            "Chaque ligne doit contenir soit une colonne 'hexa' valide, "
                            "soit un ensemble complet 'address', 'zipcode' et 'city'."
                        )
                        raise ValueError(error_msg_map_val)
                    
                    if self.selected_geocoder in ["GmapScraper", "Etabs", "geocodeEtabs"]:
                         if 'name' not in df_mapped_for_processing.columns or df_mapped_for_processing['name'].astype(str).str.strip().eq("").all():
                              raise ValueError(f"La colonne 'name' est requise et semble entièrement vide pour le géocodeur '{self.selected_geocoder}'.")

                    self.log_status_worker("✅ Validation des données réussie.", logging.INFO)
                    # --- FIN DE LA NOUVELLE LOGIQUE DE VALIDATION ---

                    self.log_status_worker("✅ Mapping appliqué avec succès.", logging.INFO)

                    status_label_init = "🔍 Statut: Prêt"
                    if self.selected_geocoder == "Etabs": status_label_init = "✅ Résultats Etabs: 0"
                    elif self.selected_geocoder == "geocodeEtabs": status_label_init = "⚙️ geocodeEtabs: Étape Fyre 0%"
                    elif self.selected_geocoder == "GmapScraper": status_label_init = "✅ GmapScraper: 0 succès"
                    else: status_label_init = f"❌ Non géocodés ({self.selected_geocoder}): 0"
                    self.signals.progress_update.emit((0.0, f"⏳ 0 / {total_rows} traités [0%]", "⏱️ ETA: Calcul...", status_label_init))

                except Exception as e:
                    self.log_status_worker(f"❌ Erreur lors du chargement/mapping initial: {e}", logging.ERROR)
                    error_msg = f"Impossible de lire/mapper le fichier source:\n{e}"
                    self.signals.show_message.emit("error", "Erreur Chargement/Mapping", error_msg)
                    raise

            if df_mapped_for_processing is None:
                 raise ValueError("État incohérent : DataFrame mappé est None après chargement/mapping.")

            self.df_original_source = df_source
            self.df_geocoding_input = df_mapped_for_processing

            if self.selected_geocoder == "geocodeEtabs" and not self._geocode_etabs_fyre_completed and not resuming_geocode_etabs_etabs_stage:
                source_df_for_etabs_stage2 = self.df_geocoding_input.copy()
                self.log_status_worker("💾 Copie du DataFrame mappé sauvegardée pour geocodeEtabs (étape 2).", logging.DEBUG)

            if self.selected_geocoder == "Nominatim":
                for col in NOMINATIM_OUTPUT_COLS:
                    if col not in self.df_geocoding_input.columns or starting_index == 0: self.df_geocoding_input[col] = pd.NA
                    self.df_geocoding_input[col] = self.df_geocoding_input[col].astype(object)

            self.geocode_func = None; self.gmaps_client = None
            current_geocoder_for_init = self.selected_geocoder
            if self.selected_geocoder == "geocodeEtabs":
                current_geocoder_for_init = "Fyre" if not self._geocode_etabs_fyre_completed else "Etabs"
            self.log_status_worker(f"🗺️ Initialisation du géocodeur: {current_geocoder_for_init}...", logging.INFO)
            if current_geocoder_for_init == "Nominatim":
                try: geolocator = Nominatim(user_agent=f"CleanMatchGeocoderPyqt/1.0 ({sys.platform})")
                except Exception as e_nom_init: raise ValueError(f"Erreur Nominatim init: {e_nom_init}")
                self.geocode_func = RateLimiter(geolocator.geocode, min_delay_seconds=1.1, error_wait_seconds=5.0, max_retries=3, swallow_exceptions=False)
                self.log_status_worker("✅ Nominatim initialisé avec limite de taux.", logging.INFO)
            elif current_geocoder_for_init == "Google Places":
                if not GOOGLE_API_KEY: raise ValueError("❌ Clé API Google (GOOGLE_PLACE_API_KEY) non configurée.")
                try: self.gmaps_client = googlemaps.Client(key=GOOGLE_API_KEY, requests_timeout=20)
                except Exception as e_gmaps_init: raise ValueError(f"Erreur Google Places init: {e_gmaps_init}")
                self.geocode_func = RateLimiter(self.gmaps_client.geocode, min_delay_seconds=1/45, error_wait_seconds=3.0, max_retries=4, swallow_exceptions=False)
                self.log_status_worker("✅ Google Places initialisé avec limite de taux.", logging.INFO)
            elif current_geocoder_for_init in ["Fyre", "Here", "Etabs"]:
                if not FYRE_API_KEY: raise ValueError(f"❌ Clé API Fyre/Here/Etabs (FYRE_API_KEY) non configurée pour {current_geocoder_for_init}.")
                self.log_status_worker(f"✅ {current_geocoder_for_init} sélectionné (Batch API). Initialisation OK.", logging.INFO); self.geocode_func = None
            elif current_geocoder_for_init == "GmapScraper":
                if not GMAP_SCRAPER_AVAILABLE: raise ImportError("❌ GmapScraper sélectionné mais module geoclass.py non chargé.")
                if not callable(gmap_get_data_function): raise TypeError("❌ GmapScraper: fonction importée non callable.")
                self.log_status_worker("✅ GmapScraper initialisé (utilise gmap_get_data_function).", logging.INFO); self.geocode_func = None
            else: raise ValueError(f"❌ Géocodeur non supporté sélectionné: {current_geocoder_for_init}")


            if not (self.selected_geocoder == "geocodeEtabs" and self._geocode_etabs_fyre_completed):
                self.log_status_worker("✍️ Préparation de la colonne 'full_address'...", logging.INFO)
                try:
                    for req_col in ESSENTIAL_COLS_GEOCODER[1:]:
                         if req_col not in self.df_geocoding_input.columns: raise KeyError(f"Colonne essentielle '{req_col}' manquante après mapping.")

                    addr_s = self.df_geocoding_input['address'].fillna('').astype(str)
                    zipc_s = self.df_geocoding_input['zipcode'].apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and x == x else str(x)).fillna('').astype(str)
                    city_s = self.df_geocoding_input['city'].fillna('').astype(str)

                    address_parts_list = []
                    use_country = bool(self.country_input)

                    for i in range(len(self.df_geocoding_input)):
                        parts = [addr_s.iloc[i].strip(), zipc_s.iloc[i].strip(), city_s.iloc[i].strip()]
                        if use_country: parts.append(self.country_input.strip())
                        address_parts_list.append(', '.join(p for p in parts if p))

                    self.df_geocoding_input['full_address'] = address_parts_list
                    self.log_status_worker("✅ Colonne 'full_address' préparée.", logging.INFO)

                except KeyError as e: raise ValueError(f"❌ Erreur préparation 'full_address': {e}")
                except Exception as e:
                    self.log_status_worker(f"❌ Erreur inattendue préparation 'full_address': {e}", logging.ERROR)
                    logging.exception("Traceback Erreur 'full_address'")
                    raise ValueError(f"Erreur inattendue préparation 'full_address': {e}")

            processed_count = starting_index
            current_row_index = starting_index
            start_time = time.time()
            time_last_checkpoint = start_time
            etabs_results_list = []

            geocoder_loop_instance = self.selected_geocoder
            if self.selected_geocoder == "geocodeEtabs":
                geocoder_loop_instance = "Fyre" if not self._geocode_etabs_fyre_completed else "Etabs"

            self.log_status_worker(f"🏃 Début boucle principale ({geocoder_loop_instance} / {self.selected_geocoder}) (Index {current_row_index + 1} à {total_rows})...", logging.INFO)

            while current_row_index < total_rows:
                if self._cancel_requested:
                    self.log_status_worker("⏹️ Annulation détectée. Sauvegarde checkpoint...", logging.WARNING)
                    self.save_checkpoint(self.df_geocoding_input, current_row_index, total_rows)
                    break

                while self._is_paused:
                    if self._cancel_requested: break
                    if time.time() - time_last_checkpoint > 45:
                        self.log_status_worker("⏸️ Pause: Sauvegarde périodique checkpoint...", logging.DEBUG)
                        if self.save_checkpoint(self.df_geocoding_input, current_row_index, total_rows):
                             time_last_checkpoint = time.time()
                    QThread.msleep(200)

                if self._cancel_requested:
                    self.log_status_worker("⏹️ Annulation (post-pause). Sauvegarde checkpoint...", logging.WARNING)
                    self.save_checkpoint(self.df_geocoding_input, current_row_index, total_rows)
                    break

                current_processing_geocoder = self.selected_geocoder
                if self.selected_geocoder == "geocodeEtabs":
                    current_processing_geocoder = "Fyre" if not self._geocode_etabs_fyre_completed else "Etabs"

                rows_processed_in_iteration = 0

                if current_processing_geocoder == "GmapScraper":
                    i = current_row_index
                    row_input_data = self.df_geocoding_input.loc[i]
                    gmap_row_results = {}
                    try:
                        addr_val = str(row_input_data.get('address', '')).strip()
                        zipc_val = str(row_input_data.get('zipcode', '')).strip()
                        city_val = str(row_input_data.get('city', '')).strip()
                        name_val = str(row_input_data.get('name', '')).strip()
                        hexa_val = str(row_input_data.get('hexa', '')).strip()
                        search_label = None
                        
                        if hexa_val:
                            # Priorité à la colonne 'hexa' si elle est remplie
                            search_label = hexa_val
                            self.log_status_worker(f"API (GmapScraper) Ligne {i+1} [via hexa]: '{search_label[:80]}'", logging.DEBUG)
                        elif addr_val and zipc_val and city_val:
                            # Fallback sur l'adresse complète
                            label_parts = [part for part in [name_val, addr_val, zipc_val, city_val] if part]
                            search_label = " ".join(label_parts)
                            self.log_status_worker(f"API (GmapScraper) Ligne {i+1} [via adresse]: '{search_label[:80]}'", logging.DEBUG)
                        else:
                            # Si ni hexa, ni adresse complète, on passe
                            self.log_status_worker(f"⏭️ Ligne {i+1} (GmapScraper): Données (hexa ou adresse/CP/ville) manquantes.", logging.DEBUG)
                            gmap_row_results['gmap_error'] = 'Missing hexa or full address data'

                        if search_label:
                            if not callable(gmap_get_data_function): raise RuntimeError("GmapScraper function non callable.")
                            is_etab_heuristic = search_label.startswith('0x')
                            scraped_data = gmap_get_data_function(hexa=search_label, hl='fr', is_etab=is_etab_heuristic)
                            if isinstance(scraped_data, dict) and not scraped_data.get('error'):
                                temp_processed = {}
                                for key, value in scraped_data.items():
                                    if not isinstance(value, (dict, list)): temp_processed[f'gmap_{key}'] = value
                                if not temp_processed: gmap_row_results['gmap_info'] = 'No scalar data from scraper'
                                else: gmap_row_results.update(temp_processed)
                            elif isinstance(scraped_data, dict) and scraped_data.get('error'):
                                gmap_row_results['gmap_error'] = scraped_data.get('error')
                            else: gmap_row_results['gmap_error'] = 'Invalid/empty data from GmapScraper'

                        for col_name_gmap, val_gmap in gmap_row_results.items():
                            if col_name_gmap not in self.df_geocoding_input.columns:
                                self.df_geocoding_input[col_name_gmap] = pd.NA
                                self.df_geocoding_input[col_name_gmap] = self.df_geocoding_input[col_name_gmap].astype(object)
                            self.df_geocoding_input.loc[i, col_name_gmap] = val_gmap
                    except Exception as e_gmap_scrape_loop:
                        self.log_status_worker(f"❌ Erreur GmapScraper Ligne {i+1}: {e_gmap_scrape_loop}", logging.ERROR)
                        logging.exception(f"Traceback GmapScraper Ligne {i+1}")
                        err_col = 'gmap_error';
                        if err_col not in self.df_geocoding_input.columns: self.df_geocoding_input[err_col] = pd.NA; self.df_geocoding_input[err_col] = self.df_geocoding_input[err_col].astype(object)
                        self.df_geocoding_input.loc[i, err_col] = f'Row processing error: {e_gmap_scrape_loop}'
                    rows_processed_in_iteration = 1
                    current_row_index += 1

                elif current_processing_geocoder in ["Fyre", "Here", "Etabs"]:
                    api_endpoint = None
                    if current_processing_geocoder == "Fyre": api_endpoint = FYRE_API_ENDPOINT
                    elif current_processing_geocoder == "Here": api_endpoint = HERE_API_ENDPOINT
                    elif current_processing_geocoder == "Etabs": api_endpoint = ETABS_API_ENDPOINT
                    else: main_loop_error = ValueError(f"Géocodeur batch inconnu: {current_processing_geocoder}"); break

                    batch_start_index = current_row_index
                    batch_end_index = min(batch_start_index + BATCH_API_BATCH_SIZE, total_rows)
                    tasks_to_submit = []
                    indices_in_batch = list(range(batch_start_index, batch_end_index))

                    for i in indices_in_batch:
                        address = self.df_geocoding_input.loc[i, 'full_address']
                        if current_processing_geocoder in ["Fyre", "Here"] and \
                           (not address or pd.isna(address) or str(address).strip() == ""):
                            self.df_geocoding_input.loc[i, ['lat', 'lng']] = pd.NA, pd.NA
                            self.log_status_worker(f"⏭️ Ligne {i+1} ({current_processing_geocoder}): Adresse vide, non soumise à l'API.", logging.DEBUG)
                            continue

                        address_hash = hashlib.sha1(slugify(str(address).lower()).encode('utf-8')).hexdigest() if address and pd.notna(address) else None
                        try:
                            payload_cols = TARGET_COLS_GEOCODER + ['full_address']
                            cols_to_include = [col for col in payload_cols if col in self.df_geocoding_input.columns]
                            payload = self.df_geocoding_input.loc[i, cols_to_include].astype(object).where(
                                pd.notna(self.df_geocoding_input.loc[i, cols_to_include]), None
                            ).to_dict()
                            if 'lat' in payload and payload['lat'] is not None:
                                try: payload['lat'] = float(payload['lat'])
                                except (ValueError, TypeError): payload.pop('lat', None)
                            if 'lng' in payload and payload['lng'] is not None:
                                try: payload['lng'] = float(payload['lng'])
                                except (ValueError, TypeError): payload.pop('lng', None)
                            if self.selected_geocoder == "geocodeEtabs" and current_processing_geocoder == "Etabs":
                                if 'name' not in payload or payload['name'] is None: payload['name'] = ""
                            if self.country_input: payload['country'] = self.country_input
                        except Exception as e_payload:
                            self.log_status_worker(f"❌ Erreur création payload ligne {i+1}: {e_payload}", logging.ERROR)
                            if current_processing_geocoder != "Etabs":
                                self.df_geocoding_input.loc[i, ['lat', 'lng']] = pd.NA, pd.NA
                            continue
                        tasks_to_submit.append((i, payload, address_hash))

                    current_row_index = batch_end_index
                    rows_processed_in_iteration = batch_end_index - batch_start_index

                    if tasks_to_submit:
                        self.log_status_worker(f"📦 Batch {batch_start_index+1}-{batch_end_index}: Soumission {len(tasks_to_submit)} tâches {current_processing_geocoder}...", logging.INFO)
                        with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_API_MAX_WORKERS, thread_name_prefix=f'{current_processing_geocoder}BatchWorker') as executor:
                            futures = {executor.submit(self.geocode_batch_api_worker, task_idx, task_payload, task_hash, api_endpoint): task_idx
                                       for task_idx, task_payload, task_hash in tasks_to_submit}
                            for future in concurrent.futures.as_completed(futures):
                                if self._cancel_requested: break
                                while self._is_paused:
                                    if self._cancel_requested: break
                                    QThread.msleep(100)
                                if self._cancel_requested: break
                                result_df_index = futures[future]
                                try:
                                    _, _, result_api_data, status_msg_api, status_lvl_api = future.result()
                                    if status_msg_api: self.log_status_worker(status_msg_api, level=status_lvl_api)
                                    if current_processing_geocoder == "Etabs":
                                        if isinstance(result_api_data, list):
                                            if result_api_data:
                                                master_id_val = self.df_geocoding_input.loc[result_df_index, 'id']
                                                master_name_val = self.df_geocoding_input.loc[result_df_index, 'name'] if 'name' in self.df_geocoding_input.columns else ""
                                                found_count_this_addr = 0
                                                for estab_dict in result_api_data:
                                                    if isinstance(estab_dict, dict):
                                                        estab_dict['master_id'] = master_id_val
                                                        estab_dict['master_name'] = master_name_val
                                                        etabs_results_list.append(estab_dict)
                                                        found_count_this_addr += 1
                                                if found_count_this_addr == 0 and result_api_data: self.log_status_worker(f"⚠️ Ligne {result_df_index+1}: Liste Etabs non vide mais sans dict. valides.", logging.WARNING)
                                        elif result_api_data is not None: self.log_status_worker(f"⚠️ Ligne {result_df_index+1}: Format réponse Etabs inattendu (attendu: liste).", logging.WARNING)
                                    elif current_processing_geocoder in ["Fyre", "Here"]:
                                        if result_api_data and isinstance(result_api_data, dict):
                                            for key, value in result_api_data.items():
                                                if key not in self.df_geocoding_input.columns:
                                                    self.df_geocoding_input[key] = pd.NA;
                                                    if key in ['lat', 'lng'] or 'score' in key:
                                                        self.df_geocoding_input[key] = pd.to_numeric(self.df_geocoding_input[key], errors='coerce')
                                                    else:
                                                        self.df_geocoding_input[key] = self.df_geocoding_input[key].astype(object)
                                                if key in ['lat', 'lng']:
                                                    self.df_geocoding_input.loc[result_df_index, key] = pd.to_numeric(value, errors='coerce')
                                                else:
                                                    self.df_geocoding_input.loc[result_df_index, key] = pd.NA if value == "" else value
                                            res_lat_api = result_api_data.get('lat'); res_lng_api = result_api_data.get('lng')
                                            lat_num_api = pd.to_numeric(res_lat_api, errors='coerce'); lng_num_api = pd.to_numeric(res_lng_api, errors='coerce')
                                            self.df_geocoding_input.loc[result_df_index, ['lat', 'lng']] = lat_num_api, lng_num_api
                                        else:
                                            self.df_geocoding_input.loc[result_df_index, ['lat', 'lng']] = pd.NA, pd.NA
                                            if result_api_data is not None: self.log_status_worker(f"ℹ️ Ligne {result_df_index+1}: Réponse {current_processing_geocoder} vide ou format inattendu.", logging.INFO)
                                except Exception as exc_future:
                                    self.log_status_worker(f"❌ Erreur traitement future ligne {result_df_index+1}: {exc_future}", logging.ERROR)
                                    logging.exception(f"Traceback erreur traitement future {result_df_index+1}:")
                                    if current_processing_geocoder != "Etabs":
                                        self.df_geocoding_input.loc[result_df_index, ['lat', 'lng']] = pd.NA, pd.NA
                        if self._cancel_requested: break
                else:
                    i = current_row_index
                    skip_row_single = False
                    try:
                        lat_val_s = self.df_geocoding_input.loc[i, 'lat']
                        lng_val_s = self.df_geocoding_input.loc[i, 'lng']
                        if pd.notna(lat_val_s) and pd.notna(lng_val_s):
                            self.log_status_worker(f"⏭️ Ligne {i+1} ({current_processing_geocoder}): Coords source valides, API non appelée.", logging.DEBUG)
                            skip_row_single = True
                    except KeyError: pass
                    except Exception as check_err_s: self.log_status_worker(f"⚠️ Ligne {i+1}: Erreur vérif coords (single): {check_err_s}. Traitement...", logging.WARNING)

                    if not skip_row_single:
                        address_single = self.df_geocoding_input.loc[i, 'full_address']
                        s_lat_val_hint, s_lng_val_hint = None, None
                        try:
                            s_lat_val_hint = self.df_geocoding_input.loc[i, 'lat']
                            s_lng_val_hint = self.df_geocoding_input.loc[i, 'lng']
                        except KeyError: pass
                        if not address_single or pd.isna(address_single) or str(address_single).strip() == "":
                            self.df_geocoding_input.loc[i, ['lat', 'lng']] = pd.NA, pd.NA
                            if self.selected_geocoder == "Nominatim":
                                for col_nom in NOMINATIM_OUTPUT_COLS:
                                    if col_nom in self.df_geocoding_input.columns: self.df_geocoding_input.loc[i, col_nom] = pd.NA
                        else:
                            geocoding_result_single = self.geocode_single_address(address_single, source_lat=s_lat_val_hint, source_lng=s_lng_val_hint)
                            new_lat_s = geocoding_result_single.get('lat'); new_lng_s = geocoding_result_single.get('lng')
                            lat_num_s = pd.to_numeric(new_lat_s, errors='coerce'); lng_num_s = pd.to_numeric(new_lng_s, errors='coerce')
                            self.df_geocoding_input.loc[i, 'lat'] = lat_num_s; self.df_geocoding_input.loc[i, 'lng'] = lng_num_s
                            if self.selected_geocoder == "Nominatim":
                                for col_nom in NOMINATIM_OUTPUT_COLS:
                                    if col_nom not in self.df_geocoding_input.columns:
                                        self.df_geocoding_input[col_nom] = pd.NA; self.df_geocoding_input[col_nom] = self.df_geocoding_input[col_nom].astype(object)
                                    self.df_geocoding_input.loc[i, col_nom] = geocoding_result_single.get(col_nom, pd.NA)
                    rows_processed_in_iteration = 1
                    current_row_index += 1

                processed_count = current_row_index
                status_label_msg = ""
                current_progress_geocoder_display = self.selected_geocoder
                if self.selected_geocoder == "geocodeEtabs": current_progress_geocoder_display = "Fyre" if not self._geocode_etabs_fyre_completed else "Etabs"

                if current_progress_geocoder_display == "GmapScraper":
                    successful_scrapes_count = 0
                    if processed_count > 0:
                        if 'gmap_error' in self.df_geocoding_input.columns: successful_scrapes_count = self.df_geocoding_input.loc[:processed_count-1, 'gmap_error'].isna().sum()
                        elif 'gmap_lat' in self.df_geocoding_input.columns: successful_scrapes_count = self.df_geocoding_input.loc[:processed_count-1, 'gmap_lat'].notna().sum()
                    status_label_msg = f"✅ GmapScraper: {successful_scrapes_count} succès"
                elif current_progress_geocoder_display != "Etabs":
                    try:
                        if processed_count > 0 and 'lat' in self.df_geocoding_input.columns and 'lng' in self.df_geocoding_input.columns:
                            valid_coords_so_far = pd.notna(self.df_geocoding_input.loc[:processed_count-1, 'lat']) & pd.notna(self.df_geocoding_input.loc[:processed_count-1, 'lng'])
                            not_geocoded_count = processed_count - valid_coords_so_far.sum()
                            status_label_msg = f"❌ Non géocodés ({current_progress_geocoder_display}): {not_geocoded_count}"
                        else: status_label_msg = f"❌ Non géocodés ({current_progress_geocoder_display}): 0"
                    except Exception: status_label_msg = f"❌ Non géocodés ({current_progress_geocoder_display}): Erreur"
                else:
                    status_label_msg = f"✅ Résultats Etabs: {len(etabs_results_list)}"

                progress_percent_current_stage = (processed_count / total_rows) * 100 if total_rows > 0 else 0
                progress_percent_overall_display = progress_percent_current_stage; stage_prefix_display = ""
                if self.selected_geocoder == "geocodeEtabs":
                    if not self._geocode_etabs_fyre_completed:
                        progress_percent_overall_display = progress_percent_current_stage * 0.5; stage_prefix_display = "⚙️ geocodeEtabs (Fyre) "
                    else:
                        progress_percent_overall_display = 50 + (progress_percent_current_stage * 0.5); stage_prefix_display = "⚙️ geocodeEtabs (Etabs) "

                eta_str_display = "Calcul..."
                elapsed_time_total = time.time() - start_time
                rows_done_this_run_or_stage = processed_count - (starting_index if not (self.selected_geocoder == "geocodeEtabs" and self._geocode_etabs_fyre_completed and not resuming_geocode_etabs_etabs_stage) else 0)

                if rows_done_this_run_or_stage > 5 and elapsed_time_total > 1:
                    time_per_row_current_stage = elapsed_time_total / rows_done_this_run_or_stage
                    remaining_rows_current_stage = total_rows - processed_count; remaining_rows_current_stage = max(0, remaining_rows_current_stage)
                    if time_per_row_current_stage > 1e-9:
                        eta_seconds_current_stage = time_per_row_current_stage * remaining_rows_current_stage
                        if self.selected_geocoder == "geocodeEtabs" and not self._geocode_etabs_fyre_completed:
                            estimated_stage2_time = total_rows * time_per_row_current_stage
                            eta_seconds_current_stage += estimated_stage2_time
                        try: eta_str_display = f"~{str(timedelta(seconds=int(eta_seconds_current_stage)))}"
                        except OverflowError: eta_str_display = "> 100 jours"
                    elif remaining_rows_current_stage > 0 : eta_str_display = "< 1s"
                    else: eta_str_display = "Terminé"
                elif processed_count >= total_rows: eta_str_display = "Terminé"

                processed_str_display = f"⏳ {stage_prefix_display}{processed_count} / {total_rows} traités [{int(progress_percent_overall_display)}%]"
                final_eta_str_display = f"⏱️ ETA: {eta_str_display}"
                self.signals.progress_update.emit((progress_percent_overall_display, processed_str_display, final_eta_str_display, status_label_msg))

                current_time_loop = time.time()
                last_saved_processed_count = self.current_checkpoint_data.get('processed', 0) if self.current_checkpoint_data else 0
                rows_since_last_chkpt = processed_count - last_saved_processed_count
                save_interval_seconds_val = 30
                save_interval_rows_val = BATCH_API_BATCH_SIZE * 2 if current_processing_geocoder in ["Fyre", "Here", "Etabs"] else 100

                if (current_time_loop - time_last_checkpoint > save_interval_seconds_val) or (rows_since_last_chkpt >= save_interval_rows_val):
                    if self.save_checkpoint(self.df_geocoding_input, processed_count, total_rows):
                        time_last_checkpoint = current_time_loop
                    else: self.log_status_worker("⚠️ Échec sauvegarde checkpoint périodique.", logging.WARNING)

                if self.selected_geocoder == "geocodeEtabs" and not self._geocode_etabs_fyre_completed and current_row_index >= total_rows:
                    self.log_status_worker("🎉 geocodeEtabs: Étape Fyre terminée. Préparation étape Etabs...", logging.INFO)
                    self._geocode_etabs_fyre_completed = True

                    if source_df_for_etabs_stage2 is None:
                        raise ValueError("État incohérent: Données pour étape Etabs non sauvegardées.")

                    df_fyre_results_copy = self.df_geocoding_input.copy()
                    self.log_status_worker("⚙️ geocodeEtabs: Transformation des données pour Etabs...", logging.INFO)

                    df_for_etabs_input_list = []
                    for idx_original, row_original_mapped in source_df_for_etabs_stage2.iterrows():
                        row_from_fyre = df_fyre_results_copy.loc[idx_original] if idx_original in df_fyre_results_copy.index else pd.Series(dtype='object')
                        etabs_payload_row = {}
                        etabs_payload_row['id'] = row_original_mapped.get('id', pd.NA)
                        if 'hexa' in row_original_mapped and pd.notna(row_original_mapped['hexa']):
                            etabs_payload_row['hexa'] = row_original_mapped['hexa']
                        etabs_payload_row['name'] = row_original_mapped.get('name', "")
                        addr_for_etabs = row_from_fyre.get('gmap_address', row_original_mapped.get('address', pd.NA))
                        zipc_for_etabs = row_from_fyre.get('gmap_zipcode', row_original_mapped.get('zipcode', pd.NA))
                        city_for_etabs = row_from_fyre.get('gmap_city', row_original_mapped.get('city', pd.NA))
                        etabs_payload_row['address'] = addr_for_etabs
                        etabs_payload_row['zipcode'] = zipc_for_etabs
                        etabs_payload_row['city'] = city_for_etabs
                        if 'lat' in row_original_mapped and pd.notna(row_original_mapped['lat']):
                            etabs_payload_row['lat'] = row_original_mapped['lat']
                        if 'lng' in row_original_mapped and pd.notna(row_original_mapped['lng']):
                            etabs_payload_row['lng'] = row_original_mapped['lng']
                        df_for_etabs_input_list.append(etabs_payload_row)

                    self.df_geocoding_input = pd.DataFrame(df_for_etabs_input_list)
                    for col_etabs_ll in ['lat', 'lng']:
                        if col_etabs_ll in self.df_geocoding_input.columns:
                            self.df_geocoding_input[col_etabs_ll] = pd.to_numeric(self.df_geocoding_input[col_etabs_ll], errors='coerce')

                    if self.df_geocoding_input.empty:
                        main_loop_error = ValueError("Aucune donnée à traiter pour étape Etabs après transformation."); break

                    total_rows = len(self.df_geocoding_input); current_row_index = 0; starting_index = 0; processed_count = 0
                    time_last_checkpoint = time.time(); start_time = time.time(); etabs_results_list = []
                    self.log_status_worker(f"📄 geocodeEtabs: {total_rows} lignes à traiter pour étape Etabs.", logging.INFO)

                    self.log_status_worker("🗺️ geocodeEtabs: Réinitialisation géocodeur pour Etabs...", logging.INFO)
                    if not FYRE_API_KEY: raise ValueError(f"❌ Clé API Fyre/Here/Etabs manquante pour Etabs.")
                    self.geocode_func = None;
                    self.log_status_worker("✅ Etabs (pour geocodeEtabs) initialisé.", logging.INFO)

                    self.log_status_worker("✍️ geocodeEtabs: Préparation 'full_address' pour étape Etabs...", logging.INFO)
                    try:
                        for req_col in ESSENTIAL_COLS_GEOCODER[1:]:
                            if req_col not in self.df_geocoding_input.columns: raise KeyError(f"Colonne essentielle '{req_col}' manquante (Etabs stage).")
                        addr_s_etabs = self.df_geocoding_input['address'].fillna('').astype(str)
                        zipc_s_etabs = self.df_geocoding_input['zipcode'].apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and x == x else str(x)).fillna('').astype(str)
                        city_s_etabs = self.df_geocoding_input['city'].fillna('').astype(str)
                        address_parts_list_etabs = []; use_country_etabs = bool(self.country_input)
                        for i_etabs in range(len(self.df_geocoding_input)):
                            parts_etabs = [addr_s_etabs.iloc[i_etabs].strip(), zipc_s_etabs.iloc[i_etabs].strip(), city_s_etabs.iloc[i_etabs].strip()]
                            if use_country_etabs: parts_etabs.append(self.country_input.strip())
                            address_parts_list_etabs.append(', '.join(p for p in parts_etabs if p))
                        self.df_geocoding_input['full_address'] = address_parts_list_etabs
                        self.log_status_worker("✅ geocodeEtabs: Colonne 'full_address' préparée pour Etabs.", logging.INFO)
                    except KeyError as e_fa_etabs: raise ValueError(f"❌ Erreur préparation 'full_address' (Etabs stage): {e_fa_etabs}")
                    except Exception as e_fa_etabs_unk: raise ValueError(f"Erreur 'full_address' (Etabs stage): {e_fa_etabs_unk}")

                    self.save_checkpoint(self.df_geocoding_input, 0, total_rows)
                    self.signals.progress_update.emit((50.0, f"⚙️ geocodeEtabs (Etabs) 0 / {total_rows} traités [50%]", "⏱️ ETA: Calcul...", f"✅ Résultats Etabs: 0"))
                    continue

            if self._cancel_requested:
                self.log_status_worker("⏹️ Traitement annulé (fin de boucle). Checkpoint sauvegardé.", logging.WARNING)
            elif (self.selected_geocoder == "Etabs" or (self.selected_geocoder == "geocodeEtabs" and self._geocode_etabs_fyre_completed)) and current_row_index >= total_rows:
                final_geocoder_name_display = "Etabs" if self.selected_geocoder == "Etabs" else "geocodeEtabs (Etabs)"
                self.log_status_worker(f"🎉 Traitement {final_geocoder_name_display} terminé. Sauvegarde...", logging.INFO)
                final_etabs_count = len(etabs_results_list)
                final_status_label_etabs = f"✅ Résultats Etabs: {final_etabs_count}"
                progress_overall_final_etabs = 100.0
                processed_str_final_etabs = f"⏳ {total_rows} / {total_rows} ({final_geocoder_name_display}) traités [100%]"
                if self.selected_geocoder == "geocodeEtabs": processed_str_final_etabs = f"⚙️ geocodeEtabs (Etabs) {total_rows} / {total_rows} traités [100%]"
                self.signals.progress_update.emit((progress_overall_final_etabs, processed_str_final_etabs, "⏱️ ETA: Terminé", final_status_label_etabs))
                original_total_rows_source = total_rows
                if self.selected_geocoder == "geocodeEtabs" and source_df_for_etabs_stage2 is not None:
                    original_total_rows_source = len(source_df_for_etabs_stage2)
                elif self.selected_geocoder == "geocodeEtabs" and self.df_original_source is not None:
                    original_total_rows_source = len(self.df_original_source)
                self.log_status_worker(f"📊 Total final {final_geocoder_name_display} trouvés: {final_etabs_count} pour {original_total_rows_source} adresses sources initiales.", logging.INFO)
                if final_etabs_count == 0:
                    success_msg_etabs = (f"Traitement {final_geocoder_name_display} terminé !\n{original_total_rows_source} lignes sources traitées.\n\nAucun établissement correspondant trouvé.")
                    self.signals.show_message.emit("info", f"🏁 Terminé ({final_geocoder_name_display})", success_msg_etabs)
                    self.cleanup_temp_files(self.current_job_id); self.current_checkpoint_data = {}
                else:
                    self.log_status_worker("✍️ Création DataFrame final Etabs...", logging.INFO)
                    try:
                        df_final_etabs_output = pd.DataFrame(etabs_results_list)
                        if not df_final_etabs_output.empty:
                            cols_etabs = df_final_etabs_output.columns.tolist()
                            master_cols_order = ['master_id', 'master_name'];
                            actual_master_cols_present = [mc for mc in master_cols_order if mc in cols_etabs]
                            if actual_master_cols_present:
                                for col_master in actual_master_cols_present: cols_etabs.remove(col_master)
                                for col_master in reversed(['master_name', 'master_id']):
                                    if col_master in actual_master_cols_present: cols_etabs.insert(0, col_master)
                                cols_etabs = list(dict.fromkeys(cols_etabs));
                                df_final_etabs_output = df_final_etabs_output[cols_etabs]
                            sort_by_etabs = []; ascending_order_etabs = []
                            if 'master_id' in df_final_etabs_output.columns: sort_by_etabs.append('master_id'); ascending_order_etabs.append(True)
                            if 'score_name' in df_final_etabs_output.columns:
                                try:
                                    df_final_etabs_output['score_name_numeric'] = pd.to_numeric(df_final_etabs_output['score_name'], errors='coerce')
                                    sort_by_etabs.append('score_name_numeric'); ascending_order_etabs.append(False)
                                    df_final_etabs_output = df_final_etabs_output.sort_values(by=sort_by_etabs, ascending=ascending_order_etabs, na_position='last')
                                    df_final_etabs_output.drop(columns=['score_name_numeric'], inplace=True, errors='ignore')
                                except Exception as e_sort_score:
                                     self.log_status_worker(f"⚠️ Erreur tri Etabs par score_name: {e_sort_score}. Tri par ID seulement.", logging.WARNING)
                                     if 'master_id' in sort_by_etabs: df_final_etabs_output = df_final_etabs_output.sort_values(by='master_id', ascending=True, na_position='last')
                            elif 'master_id' in sort_by_etabs:
                                df_final_etabs_output = df_final_etabs_output.sort_values(by='master_id', ascending=True, na_position='last')
                        base_name_etabs = Path(self.input_file_path).stem
                        safe_base_name_etabs = slugify(base_name_etabs, separator="_", max_length=80)
                        output_filename_etabs = f"{safe_base_name_etabs}_etabs_results.xlsx"
                        output_path_etabs = Path(self.output_dir_path) if self.output_dir_path else Path(self.input_file_path).parent if self.input_file_path else script_dir
                        if not output_path_etabs.exists():
                            try: output_path_etabs.mkdir(parents=True, exist_ok=True); self.log_status_worker(f"✅ Répertoire de sortie créé: {output_path_etabs}", logging.INFO)
                            except OSError as e_mkdir:
                                output_path_etabs = script_dir
                                self.signals.show_message.emit("error", "Erreur Répertoire Sortie", f"Impossible créer sortie:\n{output_path_etabs}\nSauvegarde dans {script_dir}.\nErreur: {e_mkdir}")
                        output_file_full_path_etabs = output_path_etabs / output_filename_etabs
                        self.log_status_worker(f"💾 Sauvegarde fichier final {final_geocoder_name_display}: {output_file_full_path_etabs} ({final_etabs_count} lignes)...", logging.INFO)
                        try:
                            df_final_etabs_output.to_excel(str(output_file_full_path_etabs), index=False, engine='openpyxl', na_rep='')
                            success_msg_etabs_final = (f"Traitement {final_geocoder_name_display} terminé !\n{original_total_rows_source} lignes sources traitées.\n{final_etabs_count} établissements trouvés.\n\nFichier:\n{output_file_full_path_etabs}")
                            self.signals.show_message.emit("info", f"🎉 Terminé ({final_geocoder_name_display})", success_msg_etabs_final)
                            self.cleanup_temp_files(self.current_job_id); self.current_checkpoint_data = {}
                        except Exception as e_save_etabs: main_loop_error = e_save_etabs; self.signals.show_message.emit("error", "Erreur Sauvegarde Finale", f"Impossible sauvegarder Excel {final_geocoder_name_display}:\n{e_save_etabs}")
                    except Exception as e_df_etabs_prep: main_loop_error = e_df_etabs_prep; self.signals.show_message.emit("error", "Erreur Préparation Données", f"Erreur préparation données {final_geocoder_name_display}:\n{e_df_etabs_prep}")

            elif self.selected_geocoder not in ["Etabs", "geocodeEtabs"] and current_row_index >= total_rows:
                self.log_status_worker(f"🎉 Géocodage standard ({self.selected_geocoder}) terminé. Sauvegarde...", logging.INFO)
                final_not_geocoded_count_std = 0; successful_scrapes_final = 0
                if self.selected_geocoder == "GmapScraper":
                    if 'gmap_error' in self.df_geocoding_input.columns: successful_scrapes_final = self.df_geocoding_input['gmap_error'].isna().sum()
                    elif 'gmap_lat' in self.df_geocoding_input.columns: successful_scrapes_final = self.df_geocoding_input['gmap_lat'].notna().sum()
                    final_status_label_std = f"✅ GmapScraper: {successful_scrapes_final} succès"; success_msg_std_details = f"{successful_scrapes_final} succès."
                else:
                    if 'lat' in self.df_geocoding_input.columns and 'lng' in self.df_geocoding_input.columns:
                        try:
                            valid_coords_final_std = pd.notna(self.df_geocoding_input['lat']) & pd.notna(self.df_geocoding_input['lng'])
                            final_not_geocoded_count_std = len(self.df_geocoding_input) - valid_coords_final_std.sum()
                        except Exception: final_not_geocoded_count_std = -1
                    else: final_not_geocoded_count_std = len(self.df_geocoding_input)
                    final_status_label_std = f"❌ Non géocodés: {final_not_geocoded_count_std}" if final_not_geocoded_count_std >= 0 else "❌ Non géocodés: Erreur"
                    success_msg_std_details = f"{final_not_geocoded_count_std if final_not_geocoded_count_std >= 0 else 'Erreur'} non trouvée(s)."
                self.signals.progress_update.emit((100.0, f"⏳ {total_rows} / {total_rows} traités [100%]", "⏱️ ETA: Terminé", final_status_label_std))
                df_final_std = self.df_geocoding_input.copy()
                cols_to_drop_std = ['full_address'];
                cols_exist_to_drop_std = [c for c in cols_to_drop_std if c in df_final_std.columns]
                if cols_exist_to_drop_std: df_final_std.drop(columns=cols_exist_to_drop_std, inplace=True)
                if self.selected_geocoder in ["Fyre", "Here"]:
                    sort_columns_fh = ['gmap_score_name', 'gmap_score_address', 'gmap_automatch']
                    available_sort_cols_fh = [col for col in sort_columns_fh if col in df_final_std.columns]
                    if available_sort_cols_fh:
                        try:
                            for col_sort_fh in available_sort_cols_fh: df_final_std[col_sort_fh] = pd.to_numeric(df_final_std[col_sort_fh], errors='coerce')
                            df_final_std = df_final_std.sort_values(by=available_sort_cols_fh, ascending=[False]*len(available_sort_cols_fh), na_position='last')
                        except Exception as e_sort_fh: self.log_status_worker(f"⚠️ Erreur tri résultats Fyre/Here par score: {e_sort_fh}", logging.WARNING)
                base_name_std = Path(self.input_file_path).stem
                safe_base_name_std = slugify(base_name_std, separator="_", max_length=80)
                safe_geocoder_name_std = slugify(self.selected_geocoder.lower().replace(' ', '_'), separator="_")
                output_filename_std = f"{safe_base_name_std}_enriched.xlsx"
                output_path_std = Path(self.output_dir_path) if self.output_dir_path else Path(self.input_file_path).parent if self.input_file_path else script_dir
                if not output_path_std.exists():
                   try: output_path_std.mkdir(parents=True, exist_ok=True); self.log_status_worker(f"✅ Répertoire de sortie créé: {output_path_std}", logging.INFO)
                   except OSError as e_mkdir_std:
                       output_path_std = script_dir
                       self.signals.show_message.emit("error", "Erreur Répertoire Sortie", f"Impossible créer sortie:\n{output_path_std}\nSauvegarde dans {script_dir}.\nErreur: {e_mkdir_std}")
                output_file_full_path_std = output_path_std / output_filename_std
                self.log_status_worker(f"💾 Sauvegarde fichier final géocodé: {output_file_full_path_std}...", logging.INFO)
                try:
                    df_final_std.to_excel(str(output_file_full_path_std), index=False, engine='openpyxl', na_rep='')
                    success_msg_std_final = (f"Géocodage ({self.selected_geocoder}) terminé !\n{total_rows}/{total_rows} lignes traitées.\n{success_msg_std_details}\n\nFichier:\n{output_file_full_path_std}")
                    self.signals.show_message.emit("info", f"🎉 Terminé ({self.selected_geocoder})", success_msg_std_final)
                    self.cleanup_temp_files(self.current_job_id); self.current_checkpoint_data = {}
                except Exception as e_save_std: main_loop_error = e_save_std; self.signals.show_message.emit("error", "Erreur Sauvegarde Finale", f"Impossible sauvegarder Excel final:\n{e_save_std}")

            if main_loop_error: raise main_loop_error
        except OperationCanceledError:
             self.log_status_worker("🛑 Traitement annulé par l'utilisateur (intercepté worker).", logging.WARNING)
        except Exception as e_worker_main:
            tb_str_worker = traceback.format_exc()
            self.log_status_worker(f"💥 Erreur majeure inattendue worker: {e_worker_main}", logging.CRITICAL)
            logging.critical(f"Traceback Erreur Majeure Worker:\n{tb_str_worker}")
            worker_error_data = (type(e_worker_main), e_worker_main, tb_str_worker)
            can_save_chkpt_on_err = (self.df_geocoding_input is not None and self.current_job_id and
                                     total_rows is not None and total_rows > 0 and
                                     current_row_index is not None and current_row_index <= total_rows)
            if can_save_chkpt_on_err:
                self.log_status_worker(f"💾 Tentative sauvegarde checkpoint après erreur (Index étape: {current_row_index})...", logging.WARNING)
                try:
                    processed_idx_err = min(max(0, int(current_row_index)), int(total_rows)); total_idx_err = int(total_rows)
                    if self.save_checkpoint(self.df_geocoding_input, processed_idx_err, total_idx_err): self.log_status_worker("✅ Checkpoint sauvegardé après erreur.", logging.INFO)
                    else: self.log_status_worker("❌ ÉCHEC sauvegarde checkpoint après erreur.", logging.ERROR)
                except Exception as save_err_fatal: self.log_status_worker(f"❌ Impossible sauvegarde checkpoint après erreur (erreur sauvegarde: {save_err_fatal})", logging.ERROR)
            else: self.log_status_worker("⚠️ Impossible de sauvegarder checkpoint après erreur (données/état invalide).", logging.ERROR)
            self.signals.error.emit(worker_error_data)
        finally:
            self.log_status_worker(f"🏁 Fin exécution worker pour {self.selected_geocoder}. Nettoyage...", logging.INFO)
            self.df_original_source = None; self.df_geocoding_input = None
            self.gmaps_client = None; self.geocode_func = None
            source_df_for_etabs_stage2 = None; etabs_results_list = []
            self.signals.reset_ui_request.emit(self.current_checkpoint_data if self.current_checkpoint_data else {})
            self.signals.finished.emit()
            self.log_status_worker("✨ Signal finished émis par worker.", logging.INFO)

# ==============================================================================
# MainWindow Class
# ==============================================================================
class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CleanMatch - Géocodage d'Adresses 🌍")
        self.current_theme_name = self.get_system_theme()
        logging.info(f"Detected system theme: {self.current_theme_name}")
        self.input_file_path: Path | None = None
        self.selected_sheet_name: str | None = None
        self.output_dir_path: Path | None = None
        self.current_job_id: str | None = None
        self.checkpoint_data: dict = {}
        self.user_column_mapping: dict[str, str] | None = None
        self.is_processing = False
        self.is_paused = False
        self.cancel_requested = False
        self.thread = None
        self.worker = None
        self.init_cache_db_main()
        self._create_widgets()
        self._create_layouts()
        self._connect_signals()
        self.apply_theme()
        self.center_window()
        self._update_ui_state()
        self.log_status_gui("✨ Application CleanMatch Geocoder démarrée.", logging.INFO)
        self.log_status_gui(f"ℹ️ Logs enregistrés dans : {LOG_FILENAME}", logging.INFO)
        self.log_status_gui(f"ℹ️ Cache DB (Google Only) : {CACHE_DB_FILE}", logging.INFO)
        if not GOOGLE_API_KEY: self.log_status_gui("⚠️ Clé API Google (GOOGLE_PLACE_API_KEY) non trouvée.", logging.WARNING)
        else: self.log_status_gui("✅ Clé API Google trouvée.", logging.INFO)
        if not FYRE_API_KEY: self.log_status_gui("⚠️ Clé API Fyre/Here/Etabs (FYRE_API_KEY) non trouvée.", logging.WARNING)
        else: self.log_status_gui("✅ Clé API Fyre/Here/Etabs trouvée.", logging.INFO)
        icon_filename = 'icon.png'; icon_path = script_dir / icon_filename
        if icon_path.is_file():
            try: self.setWindowIcon(QIcon(str(icon_path))); self.log_status_gui(f"ℹ️ Icône '{icon_filename}' chargée.", logging.INFO)
            except Exception as e_icon: logging.warning(f"Impossible charger icône '{icon_filename}': {e_icon}")
        else: logging.warning(f"Fichier icône '{icon_filename}' non trouvé dans {script_dir}")

    @staticmethod
    def get_system_theme() -> str:
        if QApplication.instance():
            try:
                color_scheme = QApplication.styleHints().colorScheme()
                if color_scheme == Qt.ColorScheme.Dark: return "dark"
            except Exception as e_theme: logging.warning(f"Could not detect system theme: {e_theme}")
        return "light"

    def apply_theme(self):
        theme_qss = DARK_MODE_QSS if self.current_theme_name == "dark" else LIGHT_MODE_QSS
        try:
             self.setStyleSheet(theme_qss)
             self.theme_button.setText("☀️ Mode Clair" if self.current_theme_name == "dark" else "🌙 Mode Sombre")
             for label in [getattr(self, 'input_label', None), getattr(self, 'output_label', None), getattr(self, 'file_status_label', None)]:
                 if label: label.style().unpolish(label); label.style().polish(label)
             logging.debug(f"Applied {self.current_theme_name} theme.")
        except Exception as e_apply_theme: logging.error(f"Failed to apply theme: {e_apply_theme}")

    def toggle_theme(self):
        self.current_theme_name = "light" if self.current_theme_name == "dark" else "dark"
        self.apply_theme()

    def center_window(self):
        try:
            self.adjustSize()
            primary_screen = QApplication.primaryScreen()
            if primary_screen:
                center_point = primary_screen.availableGeometry().center()
                frame_geometry = self.frameGeometry(); frame_geometry.moveCenter(center_point)
                self.move(frame_geometry.topLeft())
            else: logging.warning("No primary screen found for centering."); self.resize(QSize(650, 750))
        except Exception as e_center: logging.error(f"Error centering window: {e_center}"); self.resize(QSize(650, 750))

    def _create_widgets(self):
        self.file_group = QGroupBox("📁 Fichiers Source et Destination")
        self.input_button = QPushButton("📂 1. Choisir Fichier Source (.xlsx/.csv)")
        self.input_label = QLabel("📄 Fichier Source : Aucun fichier sélectionné")
        self.input_label.setObjectName("inputLabel"); self.input_label.setWordWrap(True)
        self.file_status_label = QLabel("")
        self.file_status_label.setObjectName("fileStatusLabel")
        self.output_button = QPushButton("📦 2. Choisir Répertoire de Sortie (Optionnel)")
        self.output_label = QLabel("📤 Répertoire Sortie : (par défaut: dossier du fichier source)")
        self.output_label.setObjectName("outputLabel"); self.output_label.setWordWrap(True)
        self.options_group = QGroupBox("⚙️ Options de Géocodage")
        self.geocoder_label = QLabel("🗺️ 3. Service de Géocodage :")
        self.geocoder_combo = QComboBox()
        geocoder_options = ["Fyre", "Here", "Etabs", "geocodeEtabs", "Google Places", "Nominatim"]
        if GMAP_SCRAPER_AVAILABLE: geocoder_options.insert(4, "GmapScraper")
        self.geocoder_combo.addItems(geocoder_options)
        gmap_index = self.geocoder_combo.findText("GmapScraper")
        if gmap_index != -1:
             item = self.geocoder_combo.model().item(gmap_index)
             if item: item.setEnabled(GMAP_SCRAPER_AVAILABLE)
        self.country_label = QLabel("🌍 Pays (optionnel):")
        self.country_entry = QLineEdit(); self.country_entry.setPlaceholderText("Ex: France"); self.country_entry.setFixedWidth(120)
        self.clear_cache_button = QPushButton("🧹 Vider Cache Google")
        self.clear_cache_button.setToolTip("Supprime le cache local pour Google Places API uniquement.")
        self.theme_button = QPushButton()
        self.start_button = QPushButton("▶️ 4. Lancer le Traitement")
        self.pause_button = QPushButton("⏸️ Pause")
        self.cancel_button = QPushButton("⏹️ Annuler")
        self.progress_group = QGroupBox("📊 Progression du Traitement")
        self.progress_bar = QProgressBar(); self.progress_bar.setValue(0); self.progress_bar.setTextVisible(False)
        self.processed_label = QLabel("⏳ 0 / 0 traités [0%]")
        self.eta_label = QLabel("⏱️ ETA: N/A")
        self.status_third_label = QLabel("🔍 Statut: Prêt")
        self.status_group = QGroupBox("📝 Logs")
        self.status_text = QTextEdit(); self.status_text.setReadOnly(True); self.status_text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)

    def _create_layouts(self):
        main_layout = QVBoxLayout(self); main_layout.setSpacing(10)
        file_layout = QVBoxLayout(self.file_group)
        input_line_layout = QHBoxLayout()
        input_line_layout.addWidget(self.input_button, 1)
        input_line_layout.addWidget(self.file_status_label, 0)
        file_layout.addLayout(input_line_layout)
        file_layout.addWidget(self.input_label)
        file_layout.addWidget(self.output_button)
        file_layout.addWidget(self.output_label)
        main_layout.addWidget(self.file_group)
        options_outer_layout = QVBoxLayout(self.options_group)
        options_top_layout = QHBoxLayout()
        options_top_layout.addWidget(self.geocoder_label); options_top_layout.addWidget(self.geocoder_combo, 1); options_top_layout.addSpacing(20)
        options_top_layout.addWidget(self.country_label); options_top_layout.addWidget(self.country_entry)
        options_outer_layout.addLayout(options_top_layout)
        cache_button_layout = QHBoxLayout(); cache_button_layout.addStretch(1); cache_button_layout.addWidget(self.clear_cache_button)
        options_outer_layout.addLayout(cache_button_layout)
        main_layout.addWidget(self.options_group)
        control_layout = QHBoxLayout()
        control_layout.addWidget(self.theme_button, 0, Qt.AlignmentFlag.AlignLeft); control_layout.addStretch(1)
        control_layout.addWidget(self.start_button, 2); control_layout.addWidget(self.pause_button, 1); control_layout.addWidget(self.cancel_button, 1)
        main_layout.addLayout(control_layout)
        progress_layout = QVBoxLayout(self.progress_group)
        progress_layout.addWidget(self.progress_bar)
        progress_labels_layout = QGridLayout()
        progress_labels_layout.addWidget(self.processed_label, 0, 0); progress_labels_layout.addWidget(self.eta_label, 0, 1, alignment=Qt.AlignmentFlag.AlignCenter)
        progress_labels_layout.addWidget(self.status_third_label, 0, 2, alignment=Qt.AlignmentFlag.AlignRight)
        progress_labels_layout.setColumnStretch(0, 1); progress_labels_layout.setColumnStretch(1, 1); progress_labels_layout.setColumnStretch(2, 1)
        progress_layout.addLayout(progress_labels_layout)
        main_layout.addWidget(self.progress_group)
        status_layout = QVBoxLayout(self.status_group)
        status_layout.addWidget(self.status_text)
        main_layout.addWidget(self.status_group, 1)

    def _connect_signals(self):
        self.input_button.clicked.connect(self.choose_input_file)
        self.output_button.clicked.connect(self.choose_output_dir)
        self.theme_button.clicked.connect(self.toggle_theme)
        self.start_button.clicked.connect(self.start_geocoding_process)
        self.cancel_button.clicked.connect(self.handle_cancel_request)
        self.clear_cache_button.clicked.connect(self.clear_cache_db)
        self.geocoder_combo.currentTextChanged.connect(self._handle_geocoder_selection_change)

    @pyqtSlot(str, int)
    def log_status_gui(self, message, level):
        if QThread.currentThread() is not QApplication.instance().thread():
            QMetaObject.invokeMethod(self, "log_status_gui", Qt.ConnectionType.QueuedConnection, Q_ARG(str, message), Q_ARG(int, level))
            return
        try:
            timestamp = time.strftime("%H:%M:%S")
            prefix_map = {logging.DEBUG: "🐛 ", logging.INFO: "✨ ", logging.WARNING: "⚠️ ", logging.ERROR: "❌ ", logging.CRITICAL: "💥 "}
            prefix = prefix_map.get(level, "ℹ️ ")
            color_map = {logging.DEBUG: "#7f8c8d", logging.INFO: "#2ecc71" if self.current_theme_name == "dark" else "#16a085",
                         logging.WARNING: "#f39c12", logging.ERROR: "#e74c3c", logging.CRITICAL: "#c0392b"}
            text_color = color_map.get(level, "#3498db" if self.current_theme_name == "dark" else "#2980b9")
            self.status_text.append(f"<span style='color:{text_color};'>[{timestamp}] {prefix}{message}</span>")
            self.status_text.moveCursor(QTextCursor.MoveOperation.End)
        except Exception as e_log_gui: logging.error(f"Failed to log message to GUI: {e_log_gui} (Original: {message})")

    @pyqtSlot(tuple)
    def _update_progress(self, progress_data):
        try:
            percentage, processed_str, eta_str, status_label_str = progress_data
            self.progress_bar.setValue(int(percentage))
            self.processed_label.setText(processed_str)
            self.eta_label.setText(eta_str)
            self.status_third_label.setText(status_label_str)
        except Exception as e_prog: self.log_status_gui(f"Error updating progress UI: {e_prog}", logging.ERROR)

    @pyqtSlot(str, str, str)
    def _show_message_box(self, msg_type, title, message):
        try:
            prefix_map = {"info": "ℹ️ ", "warning": "⚠️ ", "error": "❌ "}
            prefix = prefix_map.get(msg_type, "ℹ️ ")
            full_message = f"{prefix}{message}"
            if msg_type == "info": QMessageBox.information(self, title, full_message)
            elif msg_type == "warning": QMessageBox.warning(self, title, full_message)
            elif msg_type == "error": QMessageBox.critical(self, title, full_message)
            else: QMessageBox.information(self, title, full_message)
        except Exception as e_msgbox: self.log_status_gui(f"Error showing message box '{title}': {e_msgbox}", logging.ERROR)

    @pyqtSlot(bool, bool, bool)
    def _update_worker_state(self, processing, paused, cancelled):
        self.is_paused = paused
        self.cancel_requested = cancelled
        self._update_ui_state()

    def _set_file_status_label(self, text: str, status: str):
        self.file_status_label.setText(text)
        self.file_status_label.setProperty("status", status)
        self.file_status_label.style().unpolish(self.file_status_label)
        self.file_status_label.style().polish(self.file_status_label)

    def _update_ui_state(self):
        try:
            processing = self.is_processing; paused = self.is_paused; cancelled = self.cancel_requested
            main_controls_enabled = not processing
            action_buttons_enabled = processing
            pause_button_text = "⏯️ Reprendre" if paused else "⏸️ Pause"
            cancel_button_enabled = processing and not cancelled
            mapping_ok_or_not_needed = (self.user_column_mapping is not None or
                                         self.file_status_label.property("status") == "ok")
            start_enabled = main_controls_enabled and bool(self.input_file_path) and mapping_ok_or_not_needed

            self.input_button.setEnabled(main_controls_enabled)
            self.output_button.setEnabled(main_controls_enabled)
            self.start_button.setEnabled(start_enabled)
            self.geocoder_combo.setEnabled(main_controls_enabled)
            self.clear_cache_button.setEnabled(main_controls_enabled)
            self.country_entry.setEnabled(main_controls_enabled)
            self.theme_button.setEnabled(main_controls_enabled)
            self.pause_button.setEnabled(action_buttons_enabled); self.pause_button.setText(pause_button_text)
            self.cancel_button.setEnabled(cancel_button_enabled)

            job_matches_file = False
            if self.current_job_id and self.input_file_path and self.checkpoint_data:
                 job_matches_file = (self.checkpoint_data.get('job_id') == self.current_job_id and
                                     self.checkpoint_data.get('file_path') == str(self.input_file_path.resolve()))
            sheet_matches = True
            is_excel = self.input_file_path and self.input_file_path.suffix.lower() in ['.xlsx', '.xls']
            if is_excel and self.checkpoint_data: sheet_matches = (self.checkpoint_data.get('sheet_name') == self.selected_sheet_name)
            geocoder_matches = self.checkpoint_data.get('geocoder') == self.geocoder_combo.currentText() if self.checkpoint_data else False
            mapping_matches = True
            if self.checkpoint_data and self.user_column_mapping:
                 mapping_matches = (self.checkpoint_data.get('column_mapping') == self.user_column_mapping)
            is_resume_possible_ui = (self.checkpoint_data and job_matches_file and sheet_matches and geocoder_matches and mapping_matches and
                                     self.checkpoint_data.get('total', 0) > 0 and
                                     self.checkpoint_data.get('processed', 0) < self.checkpoint_data.get('total', 0))

            if is_resume_possible_ui and not processing:
                self.start_button.setText("▶️ Reprendre")
            else:
                self.start_button.setText("▶️ 4. Lancer le Traitement")

            if not processing and not is_resume_possible_ui:
                 self.status_third_label.setText("🔍 Statut: Prêt")
            elif processing and paused: self.status_third_label.setText("⏸️ Statut: En Pause")
            elif processing and cancelled: self.status_third_label.setText("⏹️ Statut: Annulation...")
        except Exception as e_ui_state: self.log_status_gui(f"Error updating UI state: {e_ui_state}", logging.ERROR)

    @pyqtSlot(dict)
    def _reset_ui_after_processing(self, final_checkpoint_data_from_worker):
        self.log_status_gui("🏁 Fin du traitement détectée par l'UI. Réinitialisation...", logging.INFO)
        was_processing = self.is_processing
        self.is_processing = False; self.is_paused = False; self.cancel_requested = False
        self.checkpoint_data = final_checkpoint_data_from_worker if final_checkpoint_data_from_worker else {}
        try:
            if self.worker: self.pause_button.clicked.disconnect(self.worker.toggle_pause)
            else: self.pause_button.clicked.disconnect()
            self.log_status_gui("ℹ️ Signal pause déconnecté.", logging.DEBUG)
        except TypeError: pass
        except Exception as e_disconnect_pause: self.log_status_gui(f"⚠️ Erreur déconnexion signal pause: {e_disconnect_pause}", logging.WARNING)

        if self.thread is not None:
            if self.thread.isRunning():
                self.log_status_gui("⏳ Attente arrêt thread worker (reset UI)...", logging.DEBUG)
                self.thread.quit()
                if not self.thread.wait(5000):
                    self.log_status_gui("⚠️ Thread non arrêté proprement. Terminaison forcée.", logging.WARNING)
                    self.thread.terminate(); self.thread.wait(2000)
            self.log_status_gui("🧹 Nettoyage thread et worker (reset UI)...", logging.DEBUG)
            self.thread = None; self.worker = None

        is_resume_possible_now = False
        if self.current_job_id and self.input_file_path and self.checkpoint_data:
            job_matches = (self.checkpoint_data.get('job_id') == self.current_job_id and
                           self.checkpoint_data.get('file_path') == str(self.input_file_path.resolve()))
            sheet_matches = True
            is_excel_file = self.input_file_path.suffix.lower() in ['.xlsx', '.xls']
            if is_excel_file: sheet_matches = (self.checkpoint_data.get('sheet_name') == self.selected_sheet_name)
            geocoder_matches = (self.checkpoint_data.get('geocoder') == self.geocoder_combo.currentText())
            mapping_matches = (self.checkpoint_data.get('column_mapping') == self.user_column_mapping)
            total_chk = self.checkpoint_data.get('total', 0); processed_chk = self.checkpoint_data.get('processed', 0)
            is_resume_possible_now = (job_matches and sheet_matches and geocoder_matches and mapping_matches and
                                     total_chk > 0 and processed_chk < total_chk)

        if is_resume_possible_now:
            processed_ui = self.checkpoint_data.get('processed', 0); total_ui = self.checkpoint_data.get('total', 0)
            chkpt_geocoder_ui = self.checkpoint_data.get('geocoder')
            eta_txt_ui = "⏱️ ETA: Prêt (Reprise)"; status_txt_ui = f"🔍 Statut: Prêt (Reprise {chkpt_geocoder_ui})"
            processed_txt_ui = f"⏳ {processed_ui} / {total_ui} traités [???%]"
            if chkpt_geocoder_ui == "geocodeEtabs":
                if self.checkpoint_data.get("geocode_etabs_stage") == "etabs_pending":
                    progress_overall_ui = 50 + (processed_ui / total_ui * 50) if total_ui > 0 else 50
                    status_txt_ui = "🔍 Statut: Prêt (Reprise geocodeEtabs - Étape Etabs)"
                    processed_txt_ui = f"⚙️ geocodeEtabs (Etabs) {processed_ui} / {total_ui} traités [{int(progress_overall_ui)}%]"
                else:
                    progress_overall_ui = (processed_ui / total_ui * 50) if total_ui > 0 else 0
                    status_txt_ui = "🔍 Statut: Prêt (Reprise geocodeEtabs - Étape Fyre)"
                    processed_txt_ui = f"⚙️ geocodeEtabs (Fyre) {processed_ui} / {total_ui} traités [{int(progress_overall_ui)}%]"
            elif chkpt_geocoder_ui == "GmapScraper":
                status_txt_ui = "🔍 Statut: Prêt (Reprise GmapScraper)"; progress_overall_ui = (processed_ui / total_ui * 100) if total_ui > 0 else 0; processed_txt_ui = f"⏳ {processed_ui} / {total_ui} traités [{int(progress_overall_ui)}%]"
            else: progress_overall_ui = (processed_ui / total_ui * 100) if total_ui > 0 else 0; processed_txt_ui = f"⏳ {processed_ui} / {total_ui} traités [{int(progress_overall_ui)}%]"
            self.progress_bar.setValue(int(progress_overall_ui)); self.processed_label.setText(processed_txt_ui)
            self.eta_label.setText(eta_txt_ui); self.status_third_label.setText(status_txt_ui)
            self.log_status_gui(f"✅ UI réinitialisée. Checkpoint valide pour reprise ({processed_ui}/{total_ui}).", logging.INFO)
        else:
            self.progress_bar.setValue(0); self.processed_label.setText("⏳ 0 / 0 traités [0%]");
            self.eta_label.setText("⏱️ ETA: N/A");
            if was_processing and self.checkpoint_data:
                reason_inv = "(voir logs précédents)"
                self.log_status_gui(f"ℹ️ Reset UI: Checkpoint ignoré car invalide/incompatible {reason_inv}.", logging.INFO)
            elif was_processing: self.log_status_gui("ℹ️ Reset UI: Aucun checkpoint valide ou traitement terminé.", logging.INFO)
        self._update_ui_state()
        self.log_status_gui("✅ Réinitialisation UI terminée.", logging.DEBUG)

    @pyqtSlot(tuple)
    def _handle_worker_error(self, error_data):
        try:
            exception_type, exception_value, tb_str = error_data
            error_msg_display = (f"Erreur critique pendant le traitement :\n\n"
                                 f"{exception_type.__name__}: {exception_value}\n\n"
                                 f"Consultez les logs pour plus de détails techniques:\n{LOG_FILENAME}")
            self.log_status_gui(f"💥 ERREUR CRITIQUE WORKER: {exception_type.__name__}: {exception_value}", logging.CRITICAL)
            logging.critical(f"--- Worker Error Traceback ---\n{tb_str}\n--- End Traceback ---")
            QMessageBox.critical(self, "❌ Erreur Critique Worker", error_msg_display)
        except Exception as e_handle_err: logging.critical(f"CRITICAL: Erreur gestion erreur worker: {e_handle_err}")
        if self.is_processing: self._reset_ui_after_processing(self.checkpoint_data)

    def choose_input_file(self):
        if self.is_processing:
            QMessageBox.warning(self, "⚠️ Action Impossible", "Fichier source non modifiable pendant traitement.")
            return
        current_input_path_val = self.input_file_path; current_job_id_val = self.current_job_id
        current_checkpoint_val = self.checkpoint_data.copy(); current_output_path_val = self.output_dir_path
        current_sheet_name_val = self.selected_sheet_name
        self.user_column_mapping = None
        self._set_file_status_label("", "")
        file_path_selected_str, _ = QFileDialog.getOpenFileName(
            self, "📂 Choisir fichier source", "", "Fichiers Excel/CSV (*.xlsx *.xls *.csv);;Tous les fichiers (*)"
        )
        if file_path_selected_str:
            selected_path_obj = Path(file_path_selected_str)
            selected_sheet_from_dialog = None
            is_excel_file_selected = selected_path_obj.suffix.lower() in ['.xlsx', '.xls']
            if is_excel_file_selected:
                try:
                    xls_file = pd.ExcelFile(selected_path_obj)
                    sheet_names_list = xls_file.sheet_names
                    if len(sheet_names_list) > 1:
                        item_sheet, ok_sheet = QInputDialog.getItem(self, "Choisir l'Onglet", "Choisir l'onglet Excel à traiter:", sheet_names_list, 0, False)
                        if ok_sheet and item_sheet: selected_sheet_from_dialog = item_sheet
                        else: self.log_status_gui("❌ Sélection onglet annulée.", logging.WARNING); return
                    elif len(sheet_names_list) == 1: selected_sheet_from_dialog = sheet_names_list[0]
                    else: QMessageBox.warning(self, "⚠️ Fichier Excel Vide", "Aucun onglet trouvé."); return
                except Exception as e_excel_sheets: QMessageBox.critical(self, "❌ Erreur Lecture Excel", f"Impossible lire onglets:\n{e_excel_sheets}"); return
            self.input_file_path = selected_path_obj
            self.selected_sheet_name = selected_sheet_from_dialog
            base_name_ui = self.input_file_path.name
            label_text_ui = f"📄 Fichier Source : {base_name_ui}"
            if self.selected_sheet_name and is_excel_file_selected: label_text_ui += f" (Onglet: '{self.selected_sheet_name}')"
            self.input_label.setText(label_text_ui)
            log_message_ui_file = f"📂 Fichier source sélectionné: {self.input_file_path}"
            if self.selected_sheet_name and is_excel_file_selected: log_message_ui_file += f", Onglet: '{self.selected_sheet_name}'"
            self.log_status_gui(log_message_ui_file, logging.INFO)
            try:
                input_dir_new = self.input_file_path.parent
                if not self.output_dir_path or self.output_dir_path == script_dir:
                    self.output_dir_path = input_dir_new
                    self.output_label.setText(f"📤 Répertoire Sortie : {self.output_dir_path}")
                    self.log_status_gui(f"✅ Répertoire sortie par défaut mis à jour: {self.output_dir_path}", logging.INFO)
            except Exception as e_out_dir:
                self.log_status_gui(f"⚠️ Erreur définition répertoire sortie par défaut: {e_out_dir}", logging.WARNING)
                self.output_dir_path = None; self.output_label.setText("📤 Répertoire Sortie : Erreur défaut")
            try:
                new_job_id_gen = generate_job_id(str(self.input_file_path))
                if new_job_id_gen != self.current_job_id or (is_excel_file_selected and self.selected_sheet_name != current_sheet_name_val):
                    self.current_job_id = new_job_id_gen
                    self.checkpoint_data = {}
                    mapping_successful = self._pre_check_and_map_columns()
                    if mapping_successful:
                        self._check_for_checkpoint()
                else:
                    mapping_successful = self._pre_check_and_map_columns()
                    if mapping_successful: self._check_for_checkpoint()
            except Exception as e_job_id_fatal:
                 QMessageBox.critical(self, "❌ Erreur Critique Job ID", f"Impossible générer ID unique:\n{e_job_id_fatal}")
                 return
        else: self.log_status_gui("❌ Sélection fichier source annulée.", logging.INFO)
        self._update_ui_state()

    def _pre_check_and_map_columns(self) -> bool:
        if not self.input_file_path or not self.input_file_path.is_file():
            self._set_file_status_label("Fichier Invalide", "error")
            return False
        self._set_file_status_label("Vérification en-têtes...", "warning")
        QApplication.processEvents()
        try:
            file_ext = self.input_file_path.suffix.lower()
            df_preview = None
            temp_available_columns: list[str] = []
            assume_headers = True
            read_opts_preview = {'dtype': object, 'nrows': 5}
            csv_read_opts_preview = {'dtype': object, 'low_memory': False, 'nrows': 5}
            if file_ext == '.csv':
                try: df_preview = pd.read_csv(self.input_file_path, sep=None, engine='python', encoding='utf-8-sig', **csv_read_opts_preview)
                except Exception:
                    try: df_preview = pd.read_csv(self.input_file_path, sep=';', engine='python', encoding='utf-8-sig', **csv_read_opts_preview)
                    except Exception: pass
                if df_preview is None or df_preview.empty:
                    self.log_status_gui("CSV aperçu vide/échec (avec en-tête), tentative sans...", logging.DEBUG)
                    csv_read_opts_preview['header'] = None
                    try: df_preview = pd.read_csv(self.input_file_path, sep=None, engine='python', encoding='utf-8-sig', **csv_read_opts_preview)
                    except Exception:
                       try: df_preview = pd.read_csv(self.input_file_path, sep=';', engine='python', encoding='utf-8-sig', **csv_read_opts_preview)
                       except Exception: pass
                    if df_preview is not None and not df_preview.empty:
                         assume_headers = False
                         temp_available_columns = [f"col_{i}" for i in range(len(df_preview.columns))]
                         self.log_status_gui("CSV lu sans en-têtes (supposé).", logging.DEBUG)
            elif file_ext in ['.xlsx', '.xls']:
                sheet_to_load_preview: str | int = self.selected_sheet_name if self.selected_sheet_name else 0
                try:
                    df_preview = pd.read_excel(self.input_file_path, sheet_name=sheet_to_load_preview, **read_opts_preview)
                except Exception as e_excel_prev:
                    self.log_status_gui(f"Erreur lecture aperçu Excel: {e_excel_prev}", logging.ERROR)
                    self._set_file_status_label("Erreur Lecture Excel", "error")
                    return False
            if assume_headers and df_preview is not None and not df_preview.empty:
                 temp_available_columns = [str(col) for col in df_preview.columns]
            if not temp_available_columns:
                 if df_preview is not None and df_preview.empty:
                     self._set_file_status_label("Fichier Vide", "error")
                     QMessageBox.warning(self, "Fichier Vide", "Le fichier sélectionné semble être vide.")
                 else:
                     self._set_file_status_label("Erreur En-têtes", "error")
                     QMessageBox.warning(self, "Erreur En-têtes", "Impossible de lire les colonnes/en-têtes du fichier.")
                 return False
            needs_mapping_dialog = False
            if not assume_headers:
                needs_mapping_dialog = True
                self.log_status_gui("Mapping requis car aucun en-tête CSV détecté.", logging.INFO)
            else:
                present_source_cols_lower = {sc.lower() for sc in temp_available_columns}
                missing_essential_targets_auto = []
                for req_target_col in MANDATORY_COLS_FOR_MAPPING_DIALOG_GEO:
                    if req_target_col.lower() not in present_source_cols_lower:
                        missing_essential_targets_auto.append(req_target_col)
                if missing_essential_targets_auto:
                    needs_mapping_dialog = True
                    self.log_status_gui(f"Mapping requis. Manquants (auto): {missing_essential_targets_auto}", logging.INFO)
                else:
                    current_geocoder = self.geocoder_combo.currentText()
                    if current_geocoder in ["GmapScraper", "Etabs", "geocodeEtabs"]:
                        if 'name'.lower() not in present_source_cols_lower:
                            needs_mapping_dialog = True
                            self.log_status_gui(f"Mapping requis car 'name' manque pour {current_geocoder}.", logging.INFO)
            if needs_mapping_dialog:
                self._set_file_status_label("⚠️ Mapping Requis", "warning")
                dialog = GeocoderColumnMappingDialog(self, temp_available_columns, MANDATORY_COLS_FOR_MAPPING_DIALOG_GEO, TARGET_COLS_GEOCODER)
                mapping_result = dialog.get_mapping()
                if mapping_result:
                    self.user_column_mapping = mapping_result
                    self._set_file_status_label("✔️ Mappé", "ok")
                    self.log_status_gui(f"Mapping manuel réussi: {self.user_column_mapping}", logging.INFO)
                    return True
                else:
                    self.user_column_mapping = None
                    self._set_file_status_label("Mapping Annulé", "error")
                    self.log_status_gui("Mapping manuel annulé.", logging.WARNING)
                    return False
            else:
                self.user_column_mapping = {}
                source_cols_dict_lower = {sc.lower(): sc for sc in temp_available_columns}
                for target_col in TARGET_COLS_GEOCODER:
                    target_col_lower = target_col.lower()
                    if target_col_lower in source_cols_dict_lower:
                        self.user_column_mapping[target_col] = source_cols_dict_lower[target_col_lower]
                    elif target_col_lower == 'lat' and 'latitude' in source_cols_dict_lower:
                        self.user_column_mapping[target_col] = source_cols_dict_lower['latitude']
                    elif target_col_lower == 'lng' and 'longitude' in source_cols_dict_lower:
                        self.user_column_mapping[target_col] = source_cols_dict_lower['longitude']
                auto_mapped_targets = self.user_column_mapping.keys()
                missing_essential_targets_automap = [
                    req_col for req_col in MANDATORY_COLS_FOR_MAPPING_DIALOG_GEO
                    if req_col not in auto_mapped_targets
                ]
                if missing_essential_targets_automap:
                     self._set_file_status_label("⚠️ Auto-map Incomplet!", "error")
                     self.log_status_gui(f"Erreur auto-mapping: Essentiels manquants {missing_essential_targets_automap}", logging.ERROR)
                     self.user_column_mapping = None
                     return False
                else:
                     self._set_file_status_label("En-têtes OK (Auto)", "ok")
                     self.log_status_gui(f"Auto-mapping réussi: {self.user_column_mapping}", logging.INFO)
                     return True
        except pd.errors.EmptyDataError:
            self._set_file_status_label("❌ Fichier Vide", "error")
            QMessageBox.warning(self, "Fichier Vide", "Le fichier sélectionné est vide.")
            return False
        except Exception as e:
            self._set_file_status_label("❌ Erreur En-têtes", "error")
            self.log_status_gui(f"Erreur vérification en-têtes: {e}", logging.ERROR)
            QMessageBox.critical(self, "Erreur Lecture En-têtes", f"Impossible lire/vérifier les en-têtes:\n{e}")
            return False
        finally:
             self._update_ui_state()

    def choose_output_dir(self):
        if self.is_processing:
            QMessageBox.warning(self, "⚠️ Action Impossible", "Répertoire sortie non modifiable pendant traitement.")
            return
        start_dir_dialog = str(self.output_dir_path) if self.output_dir_path else str(script_dir)
        dir_path_selected = QFileDialog.getExistingDirectory(self, "📦 Choisir le répertoire de sortie", start_dir_dialog)
        if dir_path_selected:
            self.output_dir_path = Path(dir_path_selected)
            self.output_label.setText(f"📤 Répertoire Sortie : {dir_path_selected}")
            self.log_status_gui(f"📦 Répertoire sortie MANUELLEMENT sélectionné : {dir_path_selected}", logging.INFO)
        else: self.log_status_gui("❌ Sélection répertoire sortie annulée.", logging.INFO)

    def clear_cache_db(self):
        if self.is_processing:
            QMessageBox.warning(self, "⚠️ Action Impossible", "Cache non modifiable pendant traitement.")
            return
        reply = QMessageBox.question(self, "❓ Confirmation Requise",
                                     "Vider TOUT le cache local de <b>Google Places</b> ?\n\nAction irréversible (forcera nouveaux appels API payants).",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                if self.thread and self.thread.isRunning():
                    QMessageBox.critical(self, "❌ Erreur", "Impossible vider cache pendant worker actif."); return
                cache_file_str = str(CACHE_DB_FILE)
                if os.path.exists(cache_file_str): os.remove(cache_file_str)
                self.init_cache_db_main()
                self.log_status_gui("🧹 Cache Google vidé et réinitialisé.", logging.INFO)
                QMessageBox.information(self, "✅ Succès", "Cache Google Places vidé.")
            except PermissionError:
                 QMessageBox.critical(self, "❌ Erreur Permission", f"Impossible supprimer cache:\n{CACHE_DB_FILE}\nVérifiez permissions/utilisation."); self.log_status_gui(f"❌ Erreur permission suppression cache: {CACHE_DB_FILE}", logging.ERROR)
            except Exception as e_unknown_cache:
                 self.log_status_gui(f"💥 Erreur inconnue vidage cache: {e_unknown_cache}", logging.ERROR); QMessageBox.critical(self, "❌ Erreur Inconnue", f"Erreur vidage cache:\n{e_unknown_cache}")

    def init_cache_db_main(self):
        try:
            conn = sqlite3.connect(str(CACHE_DB_FILE))
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS geocode_cache (address_hash TEXT PRIMARY KEY, lat REAL, lng REAL, geocoder_used TEXT, raw_response TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
            try: c.execute("ALTER TABLE geocode_cache ADD COLUMN raw_response TEXT")
            except sqlite3.OperationalError: pass
            c.execute("CREATE INDEX IF NOT EXISTS idx_address_hash ON geocode_cache (address_hash)")
            conn.commit(); conn.close();
            logging.debug("ℹ️ Cache DB initialisée/vérifiée (main thread).")
        except Exception as e_unknown_init_cache:
            logging.critical(f"💥 Erreur init cache DB (main): {e_unknown_init_cache}"); QMessageBox.critical(self, "Erreur Cache DB", f"Impossible initialiser cache DB:\n{e_unknown_init_cache}")

    def _check_for_checkpoint(self):
        self.checkpoint_data = {}
        if not self.current_job_id or not self.input_file_path or not self.user_column_mapping:
             self._update_ui_state()
             return
        checkpoint_file_path = get_checkpoint_file(self.current_job_id)
        self.log_status_gui(f"🔍 Vérification checkpoint: {os.path.basename(checkpoint_file_path)}", logging.DEBUG)
        if os.path.exists(checkpoint_file_path):
            try:
                with open(checkpoint_file_path, 'r', encoding='utf-8') as f_chk: loaded_chkpt_data = json.load(f_chk)
                is_chkpt_valid = True; chkpt_error_messages = []
                abs_current_input_path_chk = str(self.input_file_path.resolve())
                is_excel_for_chkpt = self.input_file_path.suffix.lower() in ['.xlsx', '.xls']
                if loaded_chkpt_data.get('job_id') != self.current_job_id: chkpt_error_messages.append("Job ID"); is_chkpt_valid = False
                if loaded_chkpt_data.get('file_path') != abs_current_input_path_chk: chkpt_error_messages.append("Chemin fichier"); is_chkpt_valid = False
                chkpt_sheet_name = loaded_chkpt_data.get('sheet_name')
                if is_excel_for_chkpt and is_chkpt_valid and chkpt_sheet_name != self.selected_sheet_name: chkpt_error_messages.append(f"Onglet ('{chkpt_sheet_name}' vs '{self.selected_sheet_name}')"); is_chkpt_valid = False
                chkpt_geocoder_val = loaded_chkpt_data.get('geocoder'); current_ui_geocoder = self.geocoder_combo.currentText()
                if is_chkpt_valid and chkpt_geocoder_val != current_ui_geocoder: chkpt_error_messages.append(f"Géocodeur ('{chkpt_geocoder_val}' vs '{current_ui_geocoder}')"); is_chkpt_valid = False
                chkpt_mapping = loaded_chkpt_data.get('column_mapping')
                if is_chkpt_valid and chkpt_mapping != self.user_column_mapping: chkpt_error_messages.append("Mapping différent"); is_chkpt_valid = False
                temp_excel_file_chkpt = loaded_chkpt_data.get('temp_file')
                if is_chkpt_valid and (not temp_excel_file_chkpt or not os.path.exists(temp_excel_file_chkpt)):
                    chkpt_error_messages.append("Fichier temp manquant"); is_chkpt_valid = False; cleanup_temp_files(self.current_job_id)
                processed_chkpt = loaded_chkpt_data.get('processed', 0); total_chkpt = loaded_chkpt_data.get('total', 0)
                if is_chkpt_valid and total_chkpt > 0 and processed_chkpt >= total_chkpt:
                    self.log_status_gui(f"ℹ️ Checkpoint trouvé mais terminé ({processed_chkpt}/{total_chkpt}). Nettoyage.", logging.INFO); is_chkpt_valid = False; cleanup_temp_files(self.current_job_id)
                if is_chkpt_valid:
                    timestamp_f_chkpt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(loaded_chkpt_data.get('timestamp', 0)))
                    geocoder_chkpt_display = loaded_chkpt_data.get('geocoder', 'N/A')
                    resume_msg_base_chkpt = f"Traitement précédent interrompu.\nFichier: {self.input_file_path.name}"
                    if is_excel_for_chkpt and chkpt_sheet_name: resume_msg_base_chkpt += f"\nOnglet: '{chkpt_sheet_name}'"
                    prog_msg_chkpt = f"\nProgression: {processed_chkpt} / {total_chkpt} lignes"
                    if geocoder_chkpt_display == "geocodeEtabs" and loaded_chkpt_data.get("geocode_etabs_stage") == "etabs_pending":
                        prog_msg_chkpt = f"\nProgression: Étape Fyre terminée, reprise Etabs ({processed_chkpt}/{total_chkpt})."
                    resume_msg_full = (resume_msg_base_chkpt + prog_msg_chkpt + f"\nGéocodeur: {geocoder_chkpt_display}" +
                                       f"\nDernière sauvegarde: {timestamp_f_chkpt}\n\nVoulez-vous reprendre ?")
                    reply_resume = QMessageBox.question(self, "❓ Reprendre Traitement ?", resume_msg_full, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.Yes)
                    if reply_resume == QMessageBox.StandardButton.Yes:
                        self.checkpoint_data = loaded_chkpt_data
                        self.log_status_gui(f"✅ Reprise acceptée pour {os.path.basename(checkpoint_file_path)}.", logging.INFO)
                        eta_txt_resume = "⏱️ ETA: Prêt (Reprise)"; status_txt_resume = f"🔍 Statut: Prêt (Reprise {geocoder_chkpt_display})";
                        processed_txt_resume = f"⏳ {processed_chkpt} / {total_chkpt} traités [???%]"
                        if geocoder_chkpt_display == "geocodeEtabs":
                            if loaded_chkpt_data.get("geocode_etabs_stage") == "etabs_pending":
                                progress_overall_resume = 50 + (processed_chkpt / total_chkpt * 50) if total_chkpt > 0 else 50
                                status_txt_resume = "🔍 Statut: Prêt (Reprise geocodeEtabs - Étape Etabs)"
                                processed_txt_resume = f"⚙️ geocodeEtabs (Etabs) {processed_chkpt} / {total_chkpt} traités [{int(progress_overall_resume)}%]"
                            else:
                                progress_overall_resume = (processed_chkpt / total_chkpt * 50) if total_chkpt > 0 else 0
                                status_txt_resume = "🔍 Statut: Prêt (Reprise geocodeEtabs - Étape Fyre)"
                                processed_txt_resume = f"⚙️ geocodeEtabs (Fyre) {processed_chkpt} / {total_chkpt} traités [{int(progress_overall_resume)}%]"
                        elif geocoder_chkpt_display == "GmapScraper":
                            status_txt_resume = "🔍 Statut: Prêt (Reprise GmapScraper)"; progress_overall_resume = (processed_chkpt / total_chkpt * 100) if total_chkpt > 0 else 0; processed_txt_resume = f"⏳ {processed_chkpt} / {total_chkpt} traités [{int(progress_overall_resume)}%]"
                        else: progress_overall_resume = (processed_chkpt / total_chkpt * 100) if total_chkpt > 0 else 0; processed_txt_resume = f"⏳ {processed_chkpt} / {total_chkpt} traités [{int(progress_overall_resume)}%]"
                        self.progress_bar.setValue(int(progress_overall_resume)); self.processed_label.setText(processed_txt_resume)
                        self.eta_label.setText(eta_txt_resume); self.status_third_label.setText(status_txt_resume)
                        if geocoder_chkpt_display in [self.geocoder_combo.itemText(i) for i in range(self.geocoder_combo.count())]: self.geocoder_combo.setCurrentText(geocoder_chkpt_display)
                    else:
                        self.log_status_gui("❌ Reprise refusée. Nettoyage checkpoint.", logging.INFO)
                        cleanup_temp_files(self.current_job_id); self.checkpoint_data = {}
                elif chkpt_error_messages:
                    self.log_status_gui(f"⚠️ Checkpoint ({os.path.basename(checkpoint_file_path)}) invalide/incompatible : {'; '.join(chkpt_error_messages)}. Ignoré.", logging.WARNING)
                    self.checkpoint_data = {}
            except json.JSONDecodeError as e_json:
                self.log_status_gui(f"❌ Erreur lecture JSON checkpoint: {e_json}. Nettoyage.", logging.ERROR); self.checkpoint_data = {}; cleanup_temp_files(self.current_job_id)
            except Exception as e_chkpt_read:
                self.log_status_gui(f"❌ Erreur lecture/validation checkpoint: {e_chkpt_read}", logging.ERROR); self.checkpoint_data = {}; cleanup_temp_files(self.current_job_id)
        else: self.checkpoint_data = {}
        self._update_ui_state()

    @pyqtSlot(str)
    def _handle_geocoder_selection_change(self, selected_geocoder_text):
        self.user_column_mapping = None
        self._set_file_status_label("", "")
        self.checkpoint_data = {}
        if selected_geocoder_text == "Google Places" and not self.is_processing:
            QMessageBox.warning(self, "⚠️ Avertissement Coût API Google", "<b>Google Places API</b>: Service <b>payant</b> (~19 EUR / 1000 req).\nVérifiez facturation Google Cloud. Cache local utilisé.")
        if not self.is_processing and self.input_file_path:
            mapping_ok = self._pre_check_and_map_columns()
            if mapping_ok:
                 self._check_for_checkpoint()
        self._update_ui_state()

    def start_geocoding_process(self):
        if not self.input_file_path or not self.input_file_path.is_file():
            QMessageBox.critical(self, "❌ Fichier Source Manquant", "Sélectionnez un fichier source valide."); return
        if self.is_processing:
            QMessageBox.warning(self, "⚠️ Traitement en Cours", "Un traitement est déjà en cours."); return
        is_excel_start = self.input_file_path.suffix.lower() in ['.xlsx', '.xls']
        if is_excel_start and not self.selected_sheet_name:
            QMessageBox.critical(self, "❌ Onglet Manquant", "Sélectionnez un fichier Excel et choisir un onglet."); return
        if self.user_column_mapping is None:
            map_ok = self._pre_check_and_map_columns()
            if not map_ok or self.user_column_mapping is None:
                 QMessageBox.critical(self, "❌ Mapping Requis", "Le mappage des colonnes est requis ou a échoué/été annulé. Veuillez re-sélectionner le fichier.")
                 self._set_file_status_label("Mapping Requis!", "error")
                 return
        selected_geocoder_start = self.geocoder_combo.currentText()
        if selected_geocoder_start == "Google Places" and not GOOGLE_API_KEY: QMessageBox.critical(self, "❌ Clé API Manquante", "Clé API Google requise."); return
        if selected_geocoder_start in ["Fyre", "Here", "Etabs", "geocodeEtabs"] and not FYRE_API_KEY: QMessageBox.critical(self, "❌ Clé API Manquante", f"Clé API Fyre/Here/Etabs requise pour {selected_geocoder_start}."); return
        if selected_geocoder_start == "GmapScraper" and not GMAP_SCRAPER_AVAILABLE: QMessageBox.critical(self, "❌ Module Manquant", "Module GmapScraper (geoclass.py) non chargé."); return
        if not FYRE_API_KEY and selected_geocoder_start not in ["Fyre", "Here", "Etabs", "geocodeEtabs", "GmapScraper"]: self.log_status_gui("⚠️ Clé API Fyre/Here manquante, fallback désactivé.", logging.WARNING)
        try:
            if not self.current_job_id: self.current_job_id = generate_job_id(str(self.input_file_path))
        except Exception as e_job_start: QMessageBox.critical(self, "❌ Erreur Job ID", f"Impossible générer ID unique:\n{e_job_start}"); return
        checkpoint_to_pass_to_worker = {}
        if (self.checkpoint_data and self.current_job_id and self.input_file_path and
            self.checkpoint_data.get('job_id') == self.current_job_id and
            self.checkpoint_data.get('file_path') == str(self.input_file_path.resolve()) and
            (not is_excel_start or self.checkpoint_data.get('sheet_name') == self.selected_sheet_name) and
            self.checkpoint_data.get('geocoder') == selected_geocoder_start and
            self.checkpoint_data.get('column_mapping') == self.user_column_mapping):
            checkpoint_to_pass_to_worker = self.checkpoint_data.copy();
            self.log_status_gui(f"✅ Utilisation checkpoint valide ({selected_geocoder_start}) pour reprise.", logging.INFO)
        else:
            if self.checkpoint_data: self.log_status_gui(f"ℹ️ Ancien checkpoint incompatible ignoré. Nouveau traitement '{selected_geocoder_start}'.", logging.INFO)
            self.checkpoint_data = {};
            log_start_msg = f"▶️ Nouveau traitement (Job ID {self.current_job_id}) avec {selected_geocoder_start}"
            if is_excel_start: log_start_msg += f", Onglet: '{self.selected_sheet_name}'"
            self.log_status_gui(log_start_msg + ".", logging.INFO)
            self.progress_bar.setValue(0); self.processed_label.setText("⏳ 0 / 0 traités [0%]"); self.eta_label.setText("⏱️ ETA: N/A");
            status_label_init_start = "🔍 Statut: Démarrage...";
            if selected_geocoder_start == "geocodeEtabs": status_label_init_start = "⚙️ geocodeEtabs: Étape Fyre 0%"
            elif selected_geocoder_start == "Etabs": status_label_init_start = "✅ Résultats Etabs: 0"
            elif selected_geocoder_start == "GmapScraper": status_label_init_start = "✅ GmapScraper: 0 succès"
            else: status_label_init_start = f"❌ Non géocodés ({selected_geocoder_start}): 0"
            self.status_third_label.setText(status_label_init_start)
        self.is_processing = True; self.is_paused = False; self.cancel_requested = False
        self._update_ui_state()
        self.log_status_gui(f"🚀 Lancement worker pour {selected_geocoder_start}...", logging.INFO)
        self.thread = QThread()
        self.worker = Worker(
            input_file_path=str(self.input_file_path),
            selected_sheet_name=self.selected_sheet_name,
            output_dir_path=str(self.output_dir_path) if self.output_dir_path else None,
            geocoder_choice=selected_geocoder_start,
            country=self.country_entry.text().strip(),
            initial_checkpoint_data=checkpoint_to_pass_to_worker,
            job_id=self.current_job_id,
            user_column_mapping=self.user_column_mapping
        )
        self.worker.moveToThread(self.thread)
        self.worker.signals.log_message.connect(self.log_status_gui)
        self.worker.signals.progress_update.connect(self._update_progress)
        self.worker.signals.error.connect(self._handle_worker_error)
        self.worker.signals.show_message.connect(self._show_message_box)
        self.worker.signals.state_change.connect(self._update_worker_state)
        self.worker.signals.reset_ui_request.connect(self._reset_ui_after_processing)
        self.worker.signals.finished.connect(self.thread.quit)
        self.thread.started.connect(self.worker.run_geocoding_task)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self.worker.deleteLater)
        try: self.pause_button.clicked.disconnect()
        except TypeError: pass
        self.pause_button.clicked.connect(self.worker.toggle_pause)
        self.thread.start()
        self.log_status_gui("✨ Thread worker démarré.", logging.INFO)

    def handle_cancel_request(self):
        if not self.is_processing or self.cancel_requested:
            self.log_status_gui("⚠️ Demande annulation ignorée.", logging.WARNING); return
        reply_cancel = QMessageBox.question(self, "❓ Confirmer Annulation", "Annuler traitement en cours ?\nProgression sauvegardée si possible.", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply_cancel == QMessageBox.StandardButton.Yes:
            self.log_status_gui("⏹️ Annulation demandée...", logging.WARNING)
            self.cancel_requested = True; self.cancel_button.setEnabled(False); self.status_third_label.setText("🔍 Statut: Annulation...")
            if self.worker: QMetaObject.invokeMethod(self.worker, "request_cancel", Qt.ConnectionType.QueuedConnection); self.log_status_gui("ℹ️ Signal annulation envoyé.", logging.INFO)
            else: self.log_status_gui("⚠️ Worker non trouvé pour annulation.", logging.WARNING); self._reset_ui_after_processing({})
        else: self.log_status_gui("❌ Annulation annulée.", logging.INFO)

    def closeEvent(self, event):
        if self.is_processing and self.thread and self.thread.isRunning():
            reply_close = QMessageBox.question(self, "❓ Quitter l'Application", "Traitement en cours.\nQuitter l'annulera (sauvegarde tentée).\n\nQuitter quand même ?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
            if reply_close == QMessageBox.StandardButton.Yes:
                self.log_status_gui("⏹️ Fermeture demandée pendant traitement...", logging.WARNING)
                self.cancel_requested = True
                if self.worker: QMetaObject.invokeMethod(self.worker, "request_cancel", Qt.ConnectionType.QueuedConnection); self.log_status_gui("ℹ️ Signal annulation envoyé avant fermeture.", logging.INFO)
                event.accept()
            else: self.log_status_gui("❌ Fermeture annulée.", logging.INFO); event.ignore()
        else:
            self.log_status_gui("👋 Fermeture application.", logging.INFO)
            logging.info("ℹ️ Arrêt logging..."); logging.shutdown()
            event.accept()

# ==============================================================================
# Utility Functions
# ==============================================================================
def generate_job_id(file_path_for_id):
    try:
        abs_path_id = str(Path(file_path_for_id).resolve())
        base_name_id = Path(file_path_for_id).stem
        safe_base_name_id = slugify(base_name_id, separator="_", max_length=50, lowercase=True)
        path_hash_id = hashlib.sha1(abs_path_id.encode('utf-8')).hexdigest()[:8]
        job_id_gen = f"{safe_base_name_id}_{path_hash_id}_job"
        logging.debug(f"Generated Job ID: '{job_id_gen}' for file: '{abs_path_id}'")
        return job_id_gen
    except Exception as e_gen_id:
        logging.error(f"❌ Erreur génération Job ID pour '{file_path_for_id}': {e_gen_id}")
        raise ValueError(f"Impossible de générer un Job ID unique: {e_gen_id}") from e_gen_id

def get_checkpoint_file(job_id_param):
    if not job_id_param: return None
    return str(script_dir / f"{job_id_param}{CHECKPOINT_SUFFIX}")

def get_temp_file(job_id_param):
    if not job_id_param: return None
    return str(script_dir / f"{job_id_param}{TEMP_SUFFIX}")

def cleanup_temp_files(job_id_to_clean_param):
    if not job_id_to_clean_param: logging.debug("Nettoyage annulé: Job ID manquant."); return
    checkpoint_file_clean = get_checkpoint_file(job_id_to_clean_param)
    temp_excel_file_clean = get_temp_file(job_id_to_clean_param)
    temp_json_file_clean = checkpoint_file_clean + ".tmp" if checkpoint_file_clean else None
    files_to_remove_clean = [f for f in [checkpoint_file_clean, temp_excel_file_clean, temp_json_file_clean] if f and os.path.exists(f)]
    if not files_to_remove_clean: logging.debug(f"Aucun fichier temporaire trouvé pour Job ID: {job_id_to_clean_param}"); return
    logging.info(f"🧹 Nettoyage fichiers temporaires pour Job ID: {job_id_to_clean_param}")
    for f_to_remove in files_to_remove_clean:
        try: os.remove(f_to_remove); logging.debug(f"🗑️ Fichier temporaire supprimé : {os.path.basename(f_to_remove)}")
        except PermissionError as e_perm: logging.error(f"❌ Erreur permission suppression fichier {os.path.basename(f_to_remove)}: {e_perm}")
        except Exception as e_unknown_clean: logging.error(f"💥 Erreur inconnue suppression fichier {os.path.basename(f_to_remove)}: {e_unknown_clean}")

# ==============================================================================
# Main Execution Block
# ==============================================================================
if __name__ == "__main__":
    if hasattr(Qt.ApplicationAttribute, 'AA_EnableHighDpiScaling'): QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    if hasattr(Qt.ApplicationAttribute, 'AA_UseHighDpiPixmaps'): QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec())
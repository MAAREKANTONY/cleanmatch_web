# --- START OF FILE speed2.py (FINAL VERSION - MULTI-PHONE & TOKEN_SORT) ---

import sys
import os
import re
import pandas as pd
import numpy as np
from unidecode import unidecode
from rapidfuzz import fuzz
from functools import lru_cache

from concurrent.futures import ProcessPoolExecutor, as_completed
try:
    from geopy.distance import geodesic
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False

try:
    import qdarkstyle
except ImportError:
    qdarkstyle = None

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QFileDialog, QProgressBar, QSpinBox,
    QFormLayout, QMessageBox, QComboBox, QInputDialog
)

from PyQt6.QtCore import QObject, QThread, pyqtSignal, Qt

FRENCH_STOP_WORDS = [
    'le', 'la', 'les', 'du', 'des', 'au', 'aux', 'de', 'et', 'a', 'l', 'd', 'un', 'une', 'en', 'dans', 'sur', 'pour', 'par', 'avec', 'ce', 'ces', 'cette', 'cet', 'sans', 'ne', 'pas', 'plus', 'que', 'qui', 'quoi', 'où', 'comment', 'quand', 'pourquoi', 'si', 'mais', 'ou', 'donc', 'car',
    'bar', 'restaurant', 'hotel', 'brasserie', 'camping', 'cafe', 'boulangerie',
    'patisserie', 'pizzeria', 'tabac', 'presse', 'boucherie', 'charcuterie', 'epicerie',
    'pharmacie', 'garage', 'salon', 'coiffure', 'karaoke', 'discotheque', 'cinema', 'cine', 'disco',
    'creperie', 'pub', 'hotels', 'bistrot', 'pizza', 'crepe', 'sandwich', 'bowling', 'billard', 'club', 
    'sarl', 'eurl', 'sas', 'sasu', 'sa', 'snc', 'sci', 'gaec', 'entreprise',
    'societe', 'etablissements', 'ets', 'cie', 'groupe', 'maison', 'chez', 'association'
]

@lru_cache(maxsize=None)
def slugify(text):
    if not isinstance(text, str):
        return ""
    text = unidecode(text).lower()
    text = re.sub(r'[\s\W_]+', ' ', text).strip()
    return text

@lru_cache(maxsize=None)
def clean_zipcode(zipcode):
    if pd.isna(zipcode):
        return ""
    zip_str = str(zipcode).split('.')[0]
    cleaned_zip = re.sub(r'[^a-zA-Z0-9]', '', zip_str)
    return cleaned_zip.lower()

@lru_cache(maxsize=None)
def clean_phone_number(phone):
    """
    Nettoie un numéro pour ne garder que les chiffres.
    Gère le bug des float Excel (.0) qui ajoutent un zéro fantôme.
    """
    if pd.isna(phone) or phone == "":
        return ""
    
    # Convertir en string
    s_phone = str(phone)
    
    # CORRECTION : Si c'est un float (ex: "33491252864.0"), on coupe au point
    if '.' in s_phone:
        s_phone = s_phone.split('.')[0]
        
    # Ne garder que les chiffres
    clean = re.sub(r'\D', '', s_phone)
    
    # Optionnel: normalisation française pour améliorer encore le match
    # Si le numéro commence par 33 (format intl sans +), on le remplace par un 0
    # Cela permet de matcher 33491... avec 0491... à 100%
    if clean.startswith('33') and len(clean) == 11:
        clean = '0' + clean[2:]
        
    if len(clean) < 6: 
        return ""
        
    return clean

def clean_name_for_matching(name, city, stop_words_set):
    name_slug = slugify(name)
    city_slug = slugify(city)
    name_words = name_slug.split()
    city_words = city_slug.split()
    words_to_remove = stop_words_set.union(city_words)
    filtered_words = [word for word in name_words if word not in words_to_remove]
    final_name = " ".join(filtered_words).strip()
    return final_name if final_name else name_slug

def process_chunk(row_master_dict, df_slave_group, occurrences_master_set, occurrences_slave_set, threshold_name, threshold_voie):
    chunk_results = []
    
    master_id = row_master_dict.get('id')
    master_name = str(row_master_dict.get('name', ''))
    master_city = str(row_master_dict.get('city', ''))
    master_name_dedup = clean_name_for_matching(master_name, master_city, occurrences_master_set)
    master_zipcode = str(row_master_dict.get('zipcode', ''))

    # --- RECUPERATION SECURISEE DES COORDONNEES MASTER ---
    master_lat = row_master_dict.get('lat')
    # On cherche 'lng' en priorité (car initialisé dans le Worker), sinon 'lon'
    master_lng = row_master_dict.get('lng') if 'lng' in row_master_dict else row_master_dict.get('lon')

    # --- PRÉPARATION TELEPHONES MASTER ---
    m_phone_raw = row_master_dict.get('phone', '')
    m_cell_raw = row_master_dict.get('cellular', '')
    
    m_phone_clean = clean_phone_number(m_phone_raw)
    m_cell_clean = clean_phone_number(m_cell_raw)
    # -------------------------------------

    for _, row_slave in df_slave_group.iterrows():
        slave_id = row_slave.get('id')
        slave_name = str(row_slave.get('name', ''))
        slave_city = str(row_slave.get('city', ''))
        slave_name_dedup = clean_name_for_matching(slave_name, slave_city, occurrences_slave_set)
        slave_zipcode = str(row_slave.get('zipcode', ''))

        # --- RECUPERATION SECURISEE DES COORDONNEES SLAVE ---
        slave_lat = row_slave.get('lat')
        slave_lng = row_slave.get('lng') if 'lng' in row_slave else row_slave.get('lon')

        score_zipcode = fuzz.token_set_ratio(master_zipcode, slave_zipcode)

        # --- CALCUL DISTANCE SECURISE (CORRECTION DU BUG) ---
        distance_in_meters = float('inf') # Par défaut infini si pas de coordonnées
        
        if GEOPY_AVAILABLE:
            try:
                # On vérifie que TOUTES les coordonnées sont des nombres valides (pas NaN, pas None, pas vide)
                if (pd.notna(master_lat) and pd.notna(master_lng) and 
                    pd.notna(slave_lat) and pd.notna(slave_lng) and 
                    master_lat != '' and master_lng != '' and 
                    slave_lat != '' and slave_lng != ''):
                    
                    distance_in_meters = geodesic((master_lat, master_lng), (slave_lat, slave_lng)).meters
            except Exception:
                # Si erreur de conversion ou format, on ignore le calcul de distance
                distance_in_meters = float('inf')
        # ----------------------------------------------------
        
        # 1. Score Name (avec Token Sort pour "Boston Pizza" == "Pizza Boston")
        score_name = fuzz.token_sort_ratio(master_name_dedup, slave_name_dedup)
        
        # --- CALCUL SCORE PHONE (MATRICE) ---
        s_phone_raw = row_slave.get('phone', '')
        s_cell_raw = row_slave.get('cellular', '')
        
        s_phone_clean = clean_phone_number(s_phone_raw)
        s_cell_clean = clean_phone_number(s_cell_raw)
        
        phone_scores = []
        
        # On teste toutes les combinaisons possibles si les données existent
        if m_phone_clean:
            if s_phone_clean: phone_scores.append(fuzz.ratio(m_phone_clean, s_phone_clean))
            if s_cell_clean:  phone_scores.append(fuzz.ratio(m_phone_clean, s_cell_clean))
            
        if m_cell_clean:
            if s_phone_clean: phone_scores.append(fuzz.ratio(m_cell_clean, s_phone_clean))
            if s_cell_clean:  phone_scores.append(fuzz.ratio(m_cell_clean, s_cell_clean))
            
        # On prend le max, ou 0 si aucune comparaison n'a été possible
        score_phone = max(phone_scores) if phone_scores else 0
        # ------------------------------------

        # Logique de filtrage : On garde si nom proche OU téléphone identique
        proceed = False
        if score_name >= 50:
            proceed = True
        elif score_phone == 100:
            proceed = True
            
        if proceed:
            master_voie_slug = slugify(str(row_master_dict.get('voie', '')))
            slave_voie_slug = slugify(str(row_slave.get('voie', '')))
            master_city_slug = slugify(master_city)
            slave_city_slug = slugify(slave_city)

            score_voie = fuzz.token_set_ratio(master_voie_slug, slave_voie_slug)
            score_city = fuzz.token_set_ratio(master_city_slug, slave_city_slug)

            master_mc = row_master_dict.get('matchcode')
            slave_mc = row_slave.get('matchcode')
            same_matchcode = False

            if pd.notna(master_mc) and pd.notna(slave_mc) and master_mc and slave_mc:
                same_matchcode = (master_mc == slave_mc)
            
            same_hexa = False
            master_hexa = row_master_dict.get('hexa')
            slave_hexa = row_slave.get('hexa')

            if pd.notna(master_hexa) and pd.notna(slave_hexa) and master_hexa != '' and slave_hexa != '':
                same_hexa = (master_hexa == slave_hexa)

            same_siret = False
            master_siret = row_master_dict.get('siret')
            slave_siret = row_slave.get('siret')
            if pd.notna(master_siret) and pd.notna(slave_siret) and master_siret != '' and slave_siret != '':
                same_siret = (str(master_siret).strip() == str(slave_siret).strip())

            automatch = 0
            match_method = ''
            
            # --- Règles d'Automatch ---
            # 1. Matchcode + Nom fort
            if same_matchcode and score_name >= threshold_name:
                automatch = 1
                match_method = 'matchcode_name'
            
            # 2. Nom parfait + Voie correcte
            elif score_name == 100 and score_voie >= threshold_voie:
                automatch = 1
                match_method = 'algo_score'
            
            # 3. Nom fort + Voie forte
            elif score_name >= threshold_name and score_voie >= 80:
                automatch = 1
                match_method = 'algo_score'
            
            # 4. Hexacle (Coordonnées/Adresse pré-calculée)
            if automatch == 0 and master_hexa and slave_hexa and same_hexa:
                automatch = 1
                match_method = 'hexa_match'
            
            # 5. Téléphone Identique (Match fort)
            # Utilisation de distance_in_meters sécurisée
            if automatch == 0 and score_phone == 100 and (score_zipcode == 100 or distance_in_meters <= 50):
                automatch = 1
                match_method = 'phone_match'
            
            # Pour l'affichage final, on remet la distance (si infinie, on met Nan)
            final_distance = distance_in_meters if distance_in_meters != float('inf') else np.nan
            
            chunk_results.append({
                'master_id': master_id,
                'master_name': master_name, 'master_name_dedup': master_name_dedup,
                'master_address': row_master_dict.get('address', ''), 'master_zipcode': row_master_dict.get('zipcode_clean', ''),
                'master_city': master_city, 'master_voie': row_master_dict.get('voie', ''),
                'master_num_voie': row_master_dict.get('num_voie', ''), 'master_matchcode': master_mc,
                'master_hexa': master_hexa,
                'master_siret': master_siret,
                'master_phone': m_phone_raw, 
                'master_cellular': m_cell_raw,
                
                'slave_id': slave_id,
                'slave_name': slave_name, 'slave_name_dedup': slave_name_dedup,
                'slave_address': row_slave.get('address', ''), 'slave_zipcode': row_slave.get('zipcode_clean', ''),
                'slave_city': slave_city, 'slave_voie': row_slave.get('voie', ''),
                'slave_num_voie': row_slave.get('num_voie', ''), 'slave_matchcode': slave_mc,
                'slave_hexa': slave_hexa,
                'slave_siret': slave_siret,
                'slave_phone': s_phone_raw,
                'slave_cellular': s_cell_raw,
                
                'same_siret': same_siret,
                'same_hexa': same_hexa,
                'distance': final_distance,
                'score_name': score_name, 'score_voie': score_voie, 'score_city': score_city, 
                'score_phone': score_phone,
                'same_matchcode': same_matchcode, 'automatch': automatch, 'match_method': match_method
            })
    return chunk_results
    
    chunk_results = []
    
    master_id = row_master_dict.get('id')
    master_name = str(row_master_dict.get('name', ''))
    master_city = str(row_master_dict.get('city', ''))
    master_name_dedup = clean_name_for_matching(master_name, master_city, occurrences_master_set)
    master_zipcode = str(row_master_dict.get('zipcode', ''))

    master_lat = row_master_dict.get('lat', '')
    master_lon = row_master_dict.get('lon', '')

    # --- PRÉPARATION TELEPHONES MASTER ---
    m_phone_raw = row_master_dict.get('phone', '')
    m_cell_raw = row_master_dict.get('cellular', '')
    
    m_phone_clean = clean_phone_number(m_phone_raw)
    m_cell_clean = clean_phone_number(m_cell_raw)
    # -------------------------------------

    for _, row_slave in df_slave_group.iterrows():
        slave_id = row_slave.get('id')
        slave_name = str(row_slave.get('name', ''))
        slave_city = str(row_slave.get('city', ''))
        slave_name_dedup = clean_name_for_matching(slave_name, slave_city, occurrences_slave_set)
        slave_zipcode = str(row_slave.get('zipcode', ''))

        slave_lat = row_slave.get('lat', '')
        slave_lon = row_slave.get('lon', '')

        score_zipcode = fuzz.token_set_ratio(master_zipcode, slave_zipcode)

        distance_in_meters = geodesic((master_lat, master_lon), (slave_lat, slave_lon)).meters
        
        # 1. Score Name (avec Token Sort pour "Boston Pizza" == "Pizza Boston")
        score_name = fuzz.token_sort_ratio(master_name_dedup, slave_name_dedup)
        
        # --- CALCUL SCORE PHONE (MATRICE) ---
        s_phone_raw = row_slave.get('phone', '')
        s_cell_raw = row_slave.get('cellular', '')
        
        s_phone_clean = clean_phone_number(s_phone_raw)
        s_cell_clean = clean_phone_number(s_cell_raw)
        
        phone_scores = []
        
        # On teste toutes les combinaisons possibles si les données existent
        if m_phone_clean:
            if s_phone_clean: phone_scores.append(fuzz.ratio(m_phone_clean, s_phone_clean))
            if s_cell_clean:  phone_scores.append(fuzz.ratio(m_phone_clean, s_cell_clean))
            
        if m_cell_clean:
            if s_phone_clean: phone_scores.append(fuzz.ratio(m_cell_clean, s_phone_clean))
            if s_cell_clean:  phone_scores.append(fuzz.ratio(m_cell_clean, s_cell_clean))
            
        # On prend le max, ou 0 si aucune comparaison n'a été possible
        score_phone = max(phone_scores) if phone_scores else 0
        # ------------------------------------

        # Logique de filtrage : On garde si nom proche OU téléphone identique
        proceed = False
        if score_name >= 50:
            proceed = True
        elif score_phone == 100:
            proceed = True
            
        if proceed:
            master_voie_slug = slugify(str(row_master_dict.get('voie', '')))
            slave_voie_slug = slugify(str(row_slave.get('voie', '')))
            master_city_slug = slugify(master_city)
            slave_city_slug = slugify(slave_city)

            score_voie = fuzz.token_set_ratio(master_voie_slug, slave_voie_slug)
            score_city = fuzz.token_set_ratio(master_city_slug, slave_city_slug)

            master_mc = row_master_dict.get('matchcode')
            slave_mc = row_slave.get('matchcode')
            same_matchcode = False

            if pd.notna(master_mc) and pd.notna(slave_mc) and master_mc and slave_mc:
                same_matchcode = (master_mc == slave_mc)
            
            same_hexa = False
            master_hexa = row_master_dict.get('hexa')
            slave_hexa = row_slave.get('hexa')

            if pd.notna(master_hexa) and pd.notna(slave_hexa) and master_hexa != '' and slave_hexa != '':
                same_hexa = (master_hexa == slave_hexa)

            same_siret = False
            master_siret = row_master_dict.get('siret')
            slave_siret = row_slave.get('siret')
            if pd.notna(master_siret) and pd.notna(slave_siret) and master_siret != '' and slave_siret != '':
                same_siret = (str(master_siret).strip() == str(slave_siret).strip())

            automatch = 0
            match_method = ''
            
            # --- Règles d'Automatch ---
            # 1. Matchcode + Nom fort
            if same_matchcode and score_name >= threshold_name:
                automatch = 1
                match_method = 'matchcode_name'
            
            # 2. Nom parfait + Voie correcte
            elif score_name == 100 and score_voie >= threshold_voie:
                automatch = 1
                match_method = 'algo_score'
            
            # 3. Nom fort + Voie forte
            elif score_name >= threshold_name and score_voie >= 80:
                automatch = 1
                match_method = 'algo_score'
            
            # 4. Hexacle (Coordonnées/Adresse pré-calculée)
            if automatch == 0 and master_hexa and slave_hexa and same_hexa:
                automatch = 1
                match_method = 'hexa_match'
            
            # 5. Téléphone Identique (Match fort)
            if automatch == 0 and score_phone == 100 and (score_zipcode == 100 or distance_in_meters <= 50):
                automatch = 1
                match_method = 'phone_match'

            distance_meters = np.nan
            if GEOPY_AVAILABLE:
                try:
                    master_lat = row_master_dict.get('lat')
                    master_lng = row_master_dict.get('lng')
                    slave_lat = row_slave.get('lat')
                    slave_lng = row_slave.get('lng')

                    if pd.notna(master_lat) and pd.notna(master_lng) and pd.notna(slave_lat) and pd.notna(slave_lng):
                        distance_meters = geodesic((master_lat, master_lng), (slave_lat, slave_lng)).meters
                except Exception:
                    pass
            
            chunk_results.append({
                'master_id': master_id,
                'master_name': master_name, 'master_name_dedup': master_name_dedup,
                'master_address': row_master_dict.get('address', ''), 'master_zipcode': row_master_dict.get('zipcode_clean', ''),
                'master_city': master_city, 'master_voie': row_master_dict.get('voie', ''),
                'master_num_voie': row_master_dict.get('num_voie', ''), 'master_matchcode': master_mc,
                'master_hexa': master_hexa,
                'master_siret': master_siret,
                'master_phone': m_phone_raw, 
                'master_cellular': m_cell_raw, # AJOUT
                
                'slave_id': slave_id,
                'slave_name': slave_name, 'slave_name_dedup': slave_name_dedup,
                'slave_address': row_slave.get('address', ''), 'slave_zipcode': row_slave.get('zipcode_clean', ''),
                'slave_city': slave_city, 'slave_voie': row_slave.get('voie', ''),
                'slave_num_voie': row_slave.get('num_voie', ''), 'slave_matchcode': slave_mc,
                'slave_hexa': slave_hexa,
                'slave_siret': slave_siret,
                'slave_phone': s_phone_raw,
                'slave_cellular': s_cell_raw, # AJOUT
                
                'same_siret': same_siret,
                'same_hexa': same_hexa,
                'distance': distance_meters,
                'score_name': score_name, 'score_voie': score_voie, 'score_city': score_city, 
                'score_phone': score_phone, # Résultat max des combinaisons
                'same_matchcode': same_matchcode, 'automatch': automatch, 'match_method': match_method
            })
    return chunk_results

class Worker(QObject):
    progress = pyqtSignal(int)
    status = pyqtSignal(str)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)
    
    def __init__(self, master_path, slave_path, master_sheet, slave_sheet, threshold_name, threshold_voie):
        super().__init__()
        self.master_path = master_path
        self.slave_path = slave_path
        self.master_sheet = master_sheet
        self.slave_sheet = slave_sheet
        self.threshold_name = threshold_name
        self.threshold_voie = threshold_voie
        self.is_running = True

    def stop(self):
        self.is_running = False

    def run(self):
        try:
            # Colonnes obligatoires pour que le script tourne
            required_cols = ['id', 'name', 'address', 'city', 'voie', 'num_voie', 'matchcode', 'zipcode']
            
            if not GEOPY_AVAILABLE:
                self.status.emit("Avertissement: Librairie 'geopy' non installée. La distance ne sera pas calculée.")

            # --- MASTER ---
            self.status.emit("Chargement du fichier master...")
            df_master = pd.read_excel(self.master_path, sheet_name=self.master_sheet)
            missing_master_cols = [col for col in required_cols if col not in df_master.columns]
            if missing_master_cols:
                self.error.emit(f"Le fichier master doit contenir les colonnes : {', '.join(missing_master_cols)}")
                return
            
            # Initialisation des colonnes optionnelles si absentes
            if 'phone' not in df_master.columns: df_master['phone'] = ""
            if 'cellular' not in df_master.columns: df_master['cellular'] = ""
            if 'siret' not in df_master.columns: df_master['siret'] = ""
            if 'hexa' not in df_master.columns: df_master['hexa'] = ""
            if 'lat' not in df_master.columns: df_master['lat'] = np.nan
            if 'lng' not in df_master.columns: df_master['lng'] = np.nan

            master_rows_before = len(df_master)
            df_master.drop_duplicates(subset=['id'], keep='first', inplace=True)
            master_rows_after = len(df_master)
            if master_rows_before > master_rows_after:
                self.status.emit(f"Master : {master_rows_before - master_rows_after} doublon(s) sur l'ID supprimé(s).")

            # --- SLAVE ---
            self.status.emit("Chargement du fichier slave...")
            df_slave = pd.read_excel(self.slave_path, sheet_name=self.slave_sheet)
            missing_slave_cols = [col for col in required_cols if col not in df_slave.columns]
            if missing_slave_cols:
                self.error.emit(f"Le fichier slave doit contenir les colonnes : {', '.join(missing_slave_cols)}")
                return
            
            # Initialisation des colonnes optionnelles si absentes
            if 'phone' not in df_slave.columns: df_slave['phone'] = ""
            if 'cellular' not in df_slave.columns: df_slave['cellular'] = ""
            if 'siret' not in df_slave.columns: df_slave['siret'] = ""
            if 'hexa' not in df_slave.columns: df_slave['hexa'] = ""
            if 'lat' not in df_slave.columns: df_slave['lat'] = np.nan
            if 'lng' not in df_slave.columns: df_slave['lng'] = np.nan
            
            slave_rows_before = len(df_slave)
            df_slave.drop_duplicates(subset=['id'], keep='first', inplace=True)
            slave_rows_after = len(df_slave)
            if slave_rows_before > slave_rows_after:
                self.status.emit(f"Slave : {slave_rows_before - slave_rows_after} doublon(s) sur l'ID supprimé(s).")
            
            self.progress.emit(5)

            self.status.emit("Calcul des occurrences de noms (master)...")
            master_name_slug = df_master['name'].dropna().apply(slugify)
            top_50_master = master_name_slug.value_counts().nlargest(50).index.tolist()
            occurrences_master_set = set(top_50_master + FRENCH_STOP_WORDS)
            
            self.status.emit("Calcul des occurrences de noms (slave)...")
            slave_name_slug = df_slave['name'].dropna().apply(slugify)
            top_50_slave = slave_name_slug.value_counts().nlargest(50).index.tolist()
            occurrences_slave_set = set(top_50_slave + FRENCH_STOP_WORDS)
            self.progress.emit(10)

            self.status.emit("Préparation des données...")
            
            df_master['zipcode_clean'] = df_master['zipcode'].apply(clean_zipcode)
            df_master['zip_group'] = df_master['zipcode_clean'].str[:3]
            
            df_slave['zipcode_clean'] = df_slave['zipcode'].apply(clean_zipcode)
            df_slave['zip_group'] = df_slave['zipcode_clean'].str[:3]

            master_groups = dict(list(df_master.groupby('zip_group')))
            slave_groups = dict(list(df_slave.groupby('zip_group')))
            
            self.progress.emit(15)

            results = []
            futures = []
            
            with ProcessPoolExecutor() as executor:
                self.status.emit("Préparation des tâches de comparaison...")
                
                for zip_key, df_master_group in master_groups.items():
                    if not self.is_running: break
                    if zip_key in slave_groups:
                        df_slave_group = slave_groups[zip_key]
                        for _, row_master in df_master_group.iterrows():
                            future = executor.submit(
                                process_chunk,
                                row_master.to_dict(),
                                df_slave_group,
                                occurrences_master_set,
                                occurrences_slave_set,
                                self.threshold_name,
                                self.threshold_voie
                            )
                            futures.append(future)

                if not futures:
                    self.status.emit("Terminé. Aucun groupe de code postal en commun trouvé.")
                    self.progress.emit(100)
                    self.finished.emit("Aucun groupe de code postal en commun, aucun fichier de sortie n'a été créé.")
                    return

                self.status.emit(f"Traitement de {len(futures)} tâches en parallèle...")

                total_tasks = len(futures)
                for i, future in enumerate(as_completed(futures)):
                    if not self.is_running:
                        executor.shutdown(wait=False, cancel_futures=True)
                        self.status.emit("Traitement annulé.")
                        return

                    try:
                        chunk_results = future.result()
                        if chunk_results:
                            results.extend(chunk_results)
                    except Exception as e:
                        self.error.emit(f"Une erreur est survenue dans un processus de travail : {e}")
                        executor.shutdown(wait=False, cancel_futures=True)
                        return

                    progress_percent = 15 + int(((i + 1) / total_tasks) * 80)
                    self.progress.emit(progress_percent)

            if not self.is_running: return

            self.status.emit("Génération du fichier de sortie...")
            self.progress.emit(95)

            if not results:
                self.status.emit("Terminé. Aucune correspondance trouvée après comparaison.")
                self.progress.emit(100)
                self.finished.emit("Aucune correspondance n'a été trouvée, aucun fichier de sortie n'a été créé.")
                return

            df_output = pd.DataFrame(results)

            # --- Réorganisation esthétique des colonnes ---
            cols = df_output.columns.tolist()
            # On veut regrouper les scores et drapeaux ensemble
            desired_order_flags = ['same_siret', 'same_hexa', 'distance', 'score_phone']
            
            if 'score_name' in cols:
                target_idx = cols.index('score_name')
                for col_name in reversed(desired_order_flags):
                    if col_name in cols:
                        cols.remove(col_name)
                        cols.insert(target_idx, col_name)
            
            # Optionnel: Placer cellular à côté de phone
            # ... (Laissez tel quel pour l'instant, c'est déjà propre)
            
            df_output = df_output[cols]
            # ----------------------------------------------

            self.status.emit("Tri des résultats...")
            # Tri par automatch, puis score tel, puis nom, puis voie
            df_output = df_output.sort_values(
                by=['automatch', 'score_phone', 'score_name', 'score_voie'], 
                ascending=[False, False, False, False]
            )

            master_basename = os.path.splitext(os.path.basename(self.master_path))[0]
            slave_basename = os.path.splitext(os.path.basename(self.slave_path))[0]
            output_filename = f"{master_basename}_VS_{slave_basename}.xlsx"
            output_path = os.path.join(os.path.dirname(self.master_path), output_filename)
            
            df_output.to_excel(output_path, index=False)
            self.progress.emit(100)
            self.status.emit(f"Terminé ! Fichier de sortie : {output_path}")
            self.finished.emit(output_path)

        except Exception as e:
            self.error.emit(f"Une erreur est survenue : {str(e)}")

class AppMatcher(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Outil de Matching (Phone Matrix & TokenSort)")
        self.setGeometry(100, 100, 700, 480)

        self.master_path = None
        self.slave_path = None
        self.master_sheet = None
        self.slave_sheet = None
        
        self.thread = None
        self.worker = None

        self.initUI()
        self.setup_theme()

    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        theme_layout = QHBoxLayout()
        theme_label = QLabel("Thème:")
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Système", "Clair", "Sombre"])
        self.theme_combo.currentTextChanged.connect(self.setup_theme)
        theme_layout.addStretch()
        theme_layout.addWidget(theme_label)
        theme_layout.addWidget(self.theme_combo)
        main_layout.addLayout(theme_layout)

        file_layout = QFormLayout()
        
        self.master_label = QLabel("Aucun fichier sélectionné")
        self.master_label.setStyleSheet("font-style: italic; color: grey;")
        btn_master = QPushButton("Choisir Fichier Master (N°1)")
        btn_master.clicked.connect(self.choose_master_file)
        file_layout.addRow(btn_master, self.master_label)

        self.slave_label = QLabel("Aucun fichier sélectionné")
        self.slave_label.setStyleSheet("font-style: italic; color: grey;")
        btn_slave = QPushButton("Choisir Fichier Slave (N°2)")
        btn_slave.clicked.connect(self.choose_slave_file)
        file_layout.addRow(btn_slave, self.slave_label)
        
        main_layout.addLayout(file_layout)
        main_layout.addSpacing(20)

        threshold_layout = QFormLayout()
        self.threshold_name_spin = QSpinBox()
        self.threshold_name_spin.setRange(0, 100)
        self.threshold_name_spin.setValue(80)
        self.threshold_name_spin.setSuffix(" %")
        threshold_layout.addRow("Seuil de similarité 'name':", self.threshold_name_spin)
        
        self.threshold_voie_spin = QSpinBox()
        self.threshold_voie_spin.setRange(0, 100)
        self.threshold_voie_spin.setValue(73)
        self.threshold_voie_spin.setSuffix(" %")
        threshold_layout.addRow("Seuil de similarité 'voie':", self.threshold_voie_spin)
        
        main_layout.addLayout(threshold_layout)
        main_layout.addStretch()

        self.start_button = QPushButton("Démarrer le Traitement")
        self.start_button.setFixedHeight(40)
        self.start_button.setStyleSheet("font-size: 16px; font-weight: bold;")
        self.start_button.clicked.connect(self.start_processing)
        main_layout.addWidget(self.start_button)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        main_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("Prêt. Veuillez sélectionner les fichiers.")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(self.status_label)
        
    def choose_master_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Choisir le fichier Master", "", "Fichiers Excel (*.xlsx *.xls)")
        if path:
            self.master_path = path
            self.master_sheet = self.ask_for_sheet(path)
            if self.master_sheet:
                 self.master_label.setText(f"{os.path.basename(path)} (Onglet: {self.master_sheet})")
                 self.update_label_colors()

    def choose_slave_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Choisir le fichier Slave", "", "Fichiers Excel (*.xlsx *.xls)")
        if path:
            self.slave_path = path
            self.slave_sheet = self.ask_for_sheet(path)
            if self.slave_sheet:
                self.slave_label.setText(f"{os.path.basename(path)} (Onglet: {self.slave_sheet})")
                self.update_label_colors()
    
    def ask_for_sheet(self, file_path):
        try:
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names
            if len(sheet_names) > 1:
                sheet_name, ok = QInputDialog.getItem(self, "Choisir un onglet", 
                                                      f"Le fichier {os.path.basename(file_path)} a plusieurs onglets. Lequel analyser ?", 
                                                      sheet_names, 0, False)
                if ok and sheet_name: return sheet_name
                else: return None
            return sheet_names[0]
        except Exception as e:
            self.show_error(f"Impossible de lire le fichier {os.path.basename(file_path)}: {e}")
            return None

    def start_processing(self):
        if not self.master_path or not self.slave_path:
            self.show_error("Veuillez sélectionner les deux fichiers (master et slave).")
            return
        
        self.start_button.setEnabled(False)
        self.status_label.setText("Initialisation...")
        self.progress_bar.setValue(0)

        threshold_name = self.threshold_name_spin.value()
        threshold_voie = self.threshold_voie_spin.value()

        self.thread = QThread()
        self.worker = Worker(
            master_path=self.master_path, slave_path=self.slave_path,
            master_sheet=self.master_sheet, slave_sheet=self.slave_sheet,
            threshold_name=threshold_name, threshold_voie=threshold_voie
        )
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_finished)
        self.worker.error.connect(self.on_error)
        self.worker.progress.connect(self.set_progress)
        self.worker.status.connect(self.set_status)
        
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.error.connect(self.thread.quit)

        self.thread.start()

    def set_progress(self, value):
        self.progress_bar.setValue(value)

    def set_status(self, text):
        self.status_label.setText(text)

    def on_finished(self, output_path):
        QMessageBox.information(self, "Succès", f"Le traitement est terminé.\n{output_path}")
        self.start_button.setEnabled(True)
        self.progress_bar.setValue(100)

    def on_error(self, message):
        self.show_error(message)
        self.start_button.setEnabled(True)
        self.status_label.setText("Erreur. Prêt pour une nouvelle tentative.")
        self.progress_bar.setValue(0)
    
    def show_error(self, message):
        msg_box = QMessageBox(QMessageBox.Icon.Critical, "Erreur", message, QMessageBox.StandardButton.Ok, self)
        msg_box.exec()
        
    def setup_theme(self):
        choice = self.theme_combo.currentText()
        if choice == "Système":
            QApplication.instance().setStyleSheet("")
        elif choice == "Clair":
            QApplication.instance().setStyleSheet("""
                QWidget { background-color: #f0f0f0; color: #333; }
                QPushButton { background-color: #dcdcdc; border: 1px solid #c0c0c0; padding: 5px; }
                QPushButton:hover { background-color: #c8c8c8; }
                QLineEdit, QSpinBox, QComboBox { background-color: white; border: 1px solid #c0c0c0; }
                QLabel { color: #333; }
            """)
        elif choice == "Sombre":
            if qdarkstyle:
                QApplication.instance().setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt6'))
            else:
                 QApplication.instance().setStyleSheet("""
                    QWidget { background-color: #3c3c3c; color: #f0f0f0; }
                    QPushButton { background-color: #5a5a5a; border: 1px solid #777; padding: 5px; }
                    QPushButton:hover { background-color: #6a6a6a; }
                    QLineEdit, QSpinBox, QComboBox { background-color: #2a2a2a; border: 1px solid #777; }
                    QProgressBar { border: 1px solid grey; border-radius: 5px; text-align: center; color: #f0f0f0; }
                    QProgressBar::chunk { background-color: #05B8CC; }
                    QLabel { color: #f0f0f0; }
                 """)
        self.update_label_colors()
    
    def update_label_colors(self):
        is_dark = self.theme_combo.currentText() == "Sombre"
        for label in [self.master_label, self.slave_label]:
            if "Aucun fichier" in label.text():
                label.setStyleSheet("font-style: italic; color: grey;")
            else:
                color = "#f0f0f0" if is_dark else "black"
                label.setStyleSheet(f"font-style: normal; color: {color};")


    def closeEvent(self, event):
        if self.thread and self.thread.isRunning():
            self.worker.stop()
            self.thread.quit()
            self.thread.wait()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = AppMatcher()
    main_window.show()
    sys.exit(app.exec())
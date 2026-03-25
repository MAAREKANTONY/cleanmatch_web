from django import forms
from django.conf import settings

from normalizer.services.normalizer_service import CANONICAL_MAPPING_FIELDS, REQUIRED_MATCHCODE_FIELDS, EUROPE_COUNTRY_CHOICES
from matcher.services.matcher_service import MATCHER_MAPPING_FIELDS, MATCHER_REQUIRED_FIELDS
from geocoder.services.geocoder_service import GEOCODER_MAPPING_FIELDS, GEOCODER_REQUIRED_FIELDS
from geoclass.services.geoclass_service import GEOCLASS_MAPPING_FIELDS, GEOCLASS_REQUIRED_FIELDS
from marketsegmenter.services.marketsegmenter_service import MARKETSEGMENTER_MAPPING_FIELDS, MARKETSEGMENTER_REQUIRED_FIELDS
from ai_review.services.ai_review_service import (
    AI_REVIEW_MAPPING_FIELDS,
    AI_LLM_ENABLED,
    AI_LLM_PROVIDER,
    AI_LLM_MODEL,
    AI_LLM_PROVIDER_CHOICES,
    AI_LLM_MODEL_CHOICES_GROUPED,
    AI_LLM_MAX_BUDGET_EUR,
    AI_LLM_MAX_COST_PER_ROW_EUR,
    AI_LLM_MAX_CALLS_PER_ROW,
)
from ai_review.services.capability_engine import AI_REVIEW_ACTION_PROFILES, AI_REVIEW_DEFAULT_ACTION_PROFILE
from ai_review.llm import model_belongs_to_provider

from .models import Job


class JobCreateForm(forms.Form):
    job_type = forms.ChoiceField(
        label='Type de job',
        choices=[
            (Job.JobType.DEMO, 'Test pipeline'),
            (Job.JobType.NORMALIZER, 'Normalizer (moteur réel V2 checkpoint6 Matcher Multi-country)'),
            (Job.JobType.MATCHER, 'Matcher (moteur réel V4 Multi-country)'),
            (Job.JobType.GEOCODER, 'Geocoder (moteur réel V2 checkpoint)'),
            (Job.JobType.GEOCLASS, 'Geoclass (moteur réel V1 heuristique)'),
            (Job.JobType.MARKETSEGMENTER, 'Market Segmenter FYRE (Google Places -> market segments)'),
            (Job.JobType.AI_REVIEW, 'AI Review (mapping canonique + capacités agent)'),
        ],
        initial=Job.JobType.NORMALIZER,
    )
    input_file_1 = forms.FileField(label='Fichier source', required=False)
    input_file_2 = forms.FileField(label='Second fichier (optionnel)', required=False)
    normalizer_do_clean = forms.BooleanField(label='Nettoyage des colonnes', required=False, initial=True)
    normalizer_do_matchcode = forms.BooleanField(label='Génération matchcode / voie / num_voie', required=False, initial=True)
    normalizer_country_code = forms.ChoiceField(label='Pays principal', required=False, choices=EUROPE_COUNTRY_CHOICES, initial='FR')
    normalizer_sheet_name = forms.CharField(
        label='Nom de l’onglet Excel (optionnel)',
        required=False,
        help_text='Tu peux choisir un onglet après inspection du fichier. Si vide et si plusieurs onglets existent, le premier sera utilisé.',
        widget=forms.TextInput(attrs={'list': 'sheet-options', 'placeholder': 'Ex: Feuil1'}),
    )

    matcher_master_sheet_name = forms.CharField(required=False, widget=forms.HiddenInput())
    matcher_slave_sheet_name = forms.CharField(required=False, widget=forms.HiddenInput())
    matcher_threshold_name = forms.IntegerField(label='Seuil score name', required=False, initial=85, min_value=50, max_value=100)
    matcher_threshold_voie = forms.IntegerField(label='Seuil score voie', required=False, initial=70, min_value=50, max_value=100)
    matcher_top_k = forms.IntegerField(label='Top résultats / master', required=False, initial=5, min_value=1, max_value=20)


    geocoder_sheet_name = forms.CharField(required=False, widget=forms.HiddenInput())
    geocoder_provider = forms.ChoiceField(
        label='Provider geocoder',
        required=False,
        choices=[('existing_or_nominatim', 'Réutiliser coordonnées existantes puis Nominatim'), ('existing_only', 'Réutiliser uniquement les coordonnées existantes')],
        initial='existing_or_nominatim',
    )
    geocoder_country_hint = forms.CharField(label='Pays / hint (optionnel)', required=False, initial='')
    marketsegmenter_sheet_name = forms.CharField(required=False, widget=forms.HiddenInput())
    marketsegmenter_country_default = forms.CharField(label='Pays par défaut (optionnel)', required=False, initial='')

    marketsegmenter_source_mode = forms.ChoiceField(
        label='Source Market Segmenter',
        required=False,
        choices=[('uploaded', 'Fichier uploadé'), ('bigquery', 'Table BigQuery')],
        initial='uploaded',
    )
    marketsegmenter_bq_table_name = forms.CharField(label='Table BigQuery source', required=False, initial=settings.BIGQUERY_INPUT_TABLE)
    marketsegmenter_bq_country_code = forms.CharField(label='Filtre country_code', required=False, initial='')
    marketsegmenter_bq_output_table_name = forms.CharField(label='Table BigQuery de sortie', required=False, initial=settings.BIGQUERY_OUTPUT_TABLE)
    ai_review_sheet_name = forms.CharField(required=False, widget=forms.HiddenInput())
    ai_review_low_confidence_threshold = forms.FloatField(label='Seuil faible confiance AI review', required=False, initial=0.65, min_value=0.0, max_value=1.0)
    ai_review_min_confidence_threshold = forms.FloatField(label='Seuil minimum AI review', required=False, initial=0.15, min_value=0.0, max_value=1.0)
    ai_review_action_profile = forms.ChoiceField(label='Profil d’action AI review', required=False, choices=[(k, k) for k in sorted(AI_REVIEW_ACTION_PROFILES.keys())], initial=AI_REVIEW_DEFAULT_ACTION_PROFILE)
    ai_review_llm_enabled = forms.BooleanField(label='Activer le LLM réel', required=False, initial=AI_LLM_ENABLED)
    ai_review_llm_provider = forms.ChoiceField(label='Provider LLM', required=False, choices=AI_LLM_PROVIDER_CHOICES, initial=AI_LLM_PROVIDER)
    ai_review_llm_model = forms.ChoiceField(label='Modèle LLM', required=False, choices=AI_LLM_MODEL_CHOICES_GROUPED, initial=AI_LLM_MODEL)
    ai_review_llm_max_budget_eur = forms.FloatField(label='Budget max LLM par job (€)', required=False, initial=AI_LLM_MAX_BUDGET_EUR, min_value=0.0)
    ai_review_llm_max_cost_per_row_eur = forms.FloatField(label='Budget max LLM par ligne (€)', required=False, initial=AI_LLM_MAX_COST_PER_ROW_EUR, min_value=0.0)
    ai_review_llm_max_calls_per_row = forms.IntegerField(label='Nb max d’appels LLM par ligne', required=False, initial=AI_LLM_MAX_CALLS_PER_ROW, min_value=1, max_value=10)

    # Normalizer hidden mappings
    for field in CANONICAL_MAPPING_FIELDS:
        locals()[f'mapping_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())

    # Matcher hidden mappings
    for field in MATCHER_MAPPING_FIELDS:
        locals()[f'matcher_master_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())
        locals()[f'matcher_slave_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())

    # Geocoder hidden mappings
    for field in GEOCODER_MAPPING_FIELDS:
        locals()[f'geocoder_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())

    # Market Segmenter hidden mappings
    for field in MARKETSEGMENTER_MAPPING_FIELDS:
        locals()[f'marketsegmenter_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())

    # AI review hidden mappings
    for field in AI_REVIEW_MAPPING_FIELDS:
        locals()[f'ai_review_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())

    # Geoclass hidden mappings
    for field in GEOCLASS_MAPPING_FIELDS:
        locals()[f'geoclass_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())
    del field

    def clean_input_file_1(self):
        uploaded = self.cleaned_data.get('input_file_1')
        if not uploaded:
            return uploaded
        if uploaded.size <= 0:
            raise forms.ValidationError('Le fichier principal est vide.')
        return uploaded

    def clean(self):
        cleaned = super().clean()
        job_type = cleaned.get('job_type')
        input_file_1 = cleaned.get('input_file_1')
        input_file_2 = cleaned.get('input_file_2')
        marketsegmenter_source_mode = (cleaned.get('marketsegmenter_source_mode') or 'uploaded').strip()

        requires_primary_file = True
        if job_type == Job.JobType.MARKETSEGMENTER and marketsegmenter_source_mode == 'bigquery':
            requires_primary_file = False
        if job_type != Job.JobType.DEMO and requires_primary_file and not input_file_1:
            self.add_error('input_file_1', 'Ce job nécessite une source principale.')
        if job_type == Job.JobType.NORMALIZER and input_file_1:
            allowed_ext = {'.xlsx', '.xlsm', '.xltx', '.xltm'}
            filename = input_file_1.name.lower()
            if not any(filename.endswith(ext) for ext in allowed_ext):
                self.add_error('input_file_1', 'Le normalizer web supporte pour le moment uniquement les fichiers Excel .xlsx/.xlsm/.xltx/.xltm.')
            if not cleaned.get('normalizer_do_clean') and not cleaned.get('normalizer_do_matchcode'):
                self.add_error(None, 'Sélectionne au moins une opération pour le normalizer.')

            mapping = self.get_mapping_payload(cleaned)
            values = list(mapping.values())
            duplicates = [src for src in values if values.count(src) > 1]
            if duplicates:
                self.add_error(None, 'Une même colonne source ne peut pas être utilisée plusieurs fois dans le mapping.')
            if cleaned.get('normalizer_do_matchcode'):
                missing_required = [field for field in REQUIRED_MATCHCODE_FIELDS if field not in mapping]
                if missing_required:
                    self.add_error(None, 'Le matchcode nécessite un mapping explicite des colonnes : ' + ', '.join(sorted(missing_required)))

        if job_type == Job.JobType.MATCHER:
            if not input_file_2:
                self.add_error('input_file_2', 'Le matcher nécessite un fichier master et un fichier slave.')
            master_mapping = self.get_matcher_mapping_payload(cleaned, 'master')
            slave_mapping = self.get_matcher_mapping_payload(cleaned, 'slave')
            for side, mapping in [('master', master_mapping), ('slave', slave_mapping)]:
                values = list(mapping.values())
                duplicates = [src for src in values if values.count(src) > 1]
                if duplicates:
                    self.add_error(None, f'Le mapping {side} contient une colonne source utilisée plusieurs fois.')
                missing_required = [field for field in MATCHER_REQUIRED_FIELDS if field not in mapping]
                if missing_required:
                    self.add_error(None, f'Le matcher nécessite un mapping {side} des colonnes : ' + ', '.join(sorted(missing_required)))
        if job_type == Job.JobType.GEOCODER and input_file_1:
            geocoder_mapping = self.get_geocoder_mapping_payload(cleaned)
            values = list(geocoder_mapping.values())
            duplicates = [src for src in values if values.count(src) > 1]
            if duplicates:
                self.add_error(None, 'Le mapping geocoder contient une colonne source utilisée plusieurs fois.')
            missing_required = [field for field in GEOCODER_REQUIRED_FIELDS if field not in geocoder_mapping]
            if missing_required:
                self.add_error(None, 'Le geocoder nécessite un mapping des colonnes : ' + ', '.join(sorted(missing_required)))
        if job_type == Job.JobType.MARKETSEGMENTER:
            marketsegmenter_mapping = self.get_marketsegmenter_mapping_payload(cleaned)
            ai_review_mapping = self.get_ai_review_mapping_payload(cleaned)
            threshold = cleaned.get('ai_review_low_confidence_threshold')
            min_threshold = cleaned.get('ai_review_min_confidence_threshold')
            if threshold is None:
                self.add_error('ai_review_low_confidence_threshold', 'Le seuil de faible confiance est requis pour l’enchaînement Market Segmenter + AI.')
            if min_threshold is None:
                self.add_error('ai_review_min_confidence_threshold', 'Le seuil minimum AI review est requis pour l’enchaînement Market Segmenter + AI.')
            if threshold is not None and min_threshold is not None and min_threshold > threshold:
                self.add_error('ai_review_min_confidence_threshold', 'Le seuil minimum AI review doit être inférieur ou égal au seuil de faible confiance.')
            values = list(marketsegmenter_mapping.values())
            duplicates = [src for src in values if values.count(src) > 1]
            if duplicates:
                self.add_error(None, 'Le mapping market segmenter contient une colonne source utilisée plusieurs fois.')
            missing_required = [field for field in MARKETSEGMENTER_REQUIRED_FIELDS if field not in marketsegmenter_mapping]
            if missing_required:
                self.add_error(None, 'Le market segmenter nécessite un mapping des colonnes : ' + ', '.join(sorted(missing_required)))
            ai_values = [v for v in ai_review_mapping.values() if v]
            ai_duplicates = [src for src in ai_values if ai_values.count(src) > 1]
            if ai_duplicates:
                self.add_error(None, 'Le mapping AI review contient une colonne source utilisée plusieurs fois.')
            if marketsegmenter_source_mode == 'bigquery':
                if not (cleaned.get('marketsegmenter_bq_table_name') or '').strip():
                    self.add_error('marketsegmenter_bq_table_name', 'Le nom de table BigQuery source est requis.')
                if not (cleaned.get('marketsegmenter_bq_output_table_name') or '').strip():
                    self.add_error('marketsegmenter_bq_output_table_name', 'Le nom de table BigQuery de sortie est requis.')
            elif not input_file_1:
                self.add_error('input_file_1', 'Sélectionne un fichier source pour le market segmenter.')
        if job_type == Job.JobType.AI_REVIEW and input_file_1:
            threshold = cleaned.get('ai_review_low_confidence_threshold')
            min_threshold = cleaned.get('ai_review_min_confidence_threshold')
            if threshold is None:
                self.add_error('ai_review_low_confidence_threshold', 'Le seuil de faible confiance est requis pour AI Review.')
            if min_threshold is None:
                self.add_error('ai_review_min_confidence_threshold', 'Le seuil minimum AI review est requis.')
            if threshold is not None and min_threshold is not None and min_threshold > threshold:
                self.add_error('ai_review_min_confidence_threshold', 'Le seuil minimum AI review doit être inférieur ou égal au seuil de faible confiance.')
            profile = (cleaned.get('ai_review_action_profile') or '').strip()
            if profile and profile not in AI_REVIEW_ACTION_PROFILES:
                self.add_error('ai_review_action_profile', 'Le profil AI Review sélectionné est invalide.')
            provider = cleaned.get('ai_review_llm_provider')
            model = cleaned.get('ai_review_llm_model')
            budget = cleaned.get('ai_review_llm_max_budget_eur')
            row_budget = cleaned.get('ai_review_llm_max_cost_per_row_eur')
            max_calls = cleaned.get('ai_review_llm_max_calls_per_row')
            if cleaned.get('ai_review_llm_enabled') and not provider:
                self.add_error('ai_review_llm_provider', 'Le provider LLM est requis.')
            if cleaned.get('ai_review_llm_enabled') and not model:
                self.add_error('ai_review_llm_model', 'Le modèle LLM est requis.')
            if cleaned.get('ai_review_llm_enabled') and provider and model and not model_belongs_to_provider(provider, model):
                self.add_error('ai_review_llm_model', 'Le modèle choisi ne fait pas partie du provider sélectionné.')
            if budget is None:
                self.add_error('ai_review_llm_max_budget_eur', 'Le budget max LLM par job est requis.')
            if row_budget is None:
                self.add_error('ai_review_llm_max_cost_per_row_eur', 'Le budget max LLM par ligne est requis.')
            if max_calls is None:
                self.add_error('ai_review_llm_max_calls_per_row', 'Le nombre max d’appels LLM par ligne est requis.')
            if budget is not None and row_budget is not None and row_budget > budget:
                self.add_error('ai_review_llm_max_cost_per_row_eur', 'Le budget max par ligne ne peut pas dépasser le budget max du job.')
        if job_type == Job.JobType.GEOCLASS and input_file_1:
            geoclass_mapping = self.get_geoclass_mapping_payload(cleaned)
            values = list(geoclass_mapping.values())
            duplicates = [src for src in values if values.count(src) > 1]
            if duplicates:
                self.add_error(None, 'Le mapping geoclass contient une colonne source utilisée plusieurs fois.')
            missing_required = [field for field in GEOCLASS_REQUIRED_FIELDS if field not in geoclass_mapping]
            if missing_required:
                self.add_error(None, 'Le geoclass nécessite un mapping des colonnes : ' + ', '.join(sorted(missing_required)))
        return cleaned

    @staticmethod
    def get_mapping_payload(cleaned_data):
        mapping = {}
        for canonical in CANONICAL_MAPPING_FIELDS:
            value = (cleaned_data.get(f'mapping_{canonical}') or '').strip()
            if value and value != '__ignore__':
                mapping[canonical] = value
        return mapping

    @staticmethod
    def get_matcher_mapping_payload(cleaned_data, side: str):
        mapping = {}
        for canonical in MATCHER_MAPPING_FIELDS:
            value = (cleaned_data.get(f'matcher_{side}_{canonical}') or '').strip()
            if value and value != '__ignore__':
                mapping[canonical] = value
        return mapping


    @staticmethod
    def get_geocoder_mapping_payload(cleaned_data):
        mapping = {}
        for canonical in GEOCODER_MAPPING_FIELDS:
            value = (cleaned_data.get(f'geocoder_{canonical}') or '').strip()
            if value and value != '__ignore__':
                mapping[canonical] = value
        return mapping

    geoclass_sheet_name = forms.CharField(required=False, widget=forms.HiddenInput())

    @staticmethod
    def get_marketsegmenter_mapping_payload(cleaned_data):
        mapping = {}
        for canonical in MARKETSEGMENTER_MAPPING_FIELDS:
            value = (cleaned_data.get(f'marketsegmenter_{canonical}') or '').strip()
            if value and value != '__ignore__':
                mapping[canonical] = value
        return mapping

    @staticmethod
    def get_ai_review_mapping_payload(cleaned_data):
        mapping = {}
        for canonical in AI_REVIEW_MAPPING_FIELDS:
            value = (cleaned_data.get(f'ai_review_{canonical}') or '').strip()
            if value and value != '__ignore__':
                mapping[canonical] = value
        return mapping


    @staticmethod
    def get_geoclass_mapping_payload(cleaned_data):
        mapping = {}
        for canonical in GEOCLASS_MAPPING_FIELDS:
            value = (cleaned_data.get(f'geoclass_{canonical}') or '').strip()
            if value and value != '__ignore__':
                mapping[canonical] = value
        return mapping

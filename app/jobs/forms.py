from django import forms

from normalizer.services.normalizer_service import CANONICAL_MAPPING_FIELDS, REQUIRED_MATCHCODE_FIELDS
from matcher.services.matcher_service import MATCHER_MAPPING_FIELDS, MATCHER_REQUIRED_FIELDS

from .models import Job


class JobCreateForm(forms.Form):
    job_type = forms.ChoiceField(
        label='Type de job',
        choices=[
            (Job.JobType.DEMO, 'Test pipeline'),
            (Job.JobType.NORMALIZER, 'Normalizer (moteur réel V2)'),
            (Job.JobType.MATCHER, 'Matcher (moteur réel V2)'),
            (Job.JobType.GEOCODER, 'Geocoder (stub)'),
        ],
        initial=Job.JobType.NORMALIZER,
    )
    input_file_1 = forms.FileField(label='Fichier source', required=True)
    input_file_2 = forms.FileField(label='Second fichier (optionnel)', required=False)
    normalizer_do_clean = forms.BooleanField(label='Nettoyage des colonnes', required=False, initial=True)
    normalizer_do_matchcode = forms.BooleanField(label='Génération matchcode / voie / num_voie', required=False, initial=True)
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

    # Normalizer hidden mappings
    mapping_id = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_name = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_address = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_zipcode = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_city = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_lat = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_lng = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_hexa_gmap = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_phone_gmap = forms.CharField(required=False, widget=forms.HiddenInput())
    mapping_social_link_gmap = forms.CharField(required=False, widget=forms.HiddenInput())

    # Matcher hidden mappings
    for_side = ('master', 'slave')
    for field in MATCHER_MAPPING_FIELDS:
        locals()[f'matcher_master_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())
        locals()[f'matcher_slave_{field}'] = forms.CharField(required=False, widget=forms.HiddenInput())
    del for_side, field

    def clean_input_file_1(self):
        uploaded = self.cleaned_data['input_file_1']
        if uploaded.size <= 0:
            raise forms.ValidationError('Le fichier principal est vide.')
        return uploaded

    def clean(self):
        cleaned = super().clean()
        job_type = cleaned.get('job_type')
        input_file_1 = cleaned.get('input_file_1')
        input_file_2 = cleaned.get('input_file_2')
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

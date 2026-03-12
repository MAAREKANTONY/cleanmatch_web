from django import forms

from .models import Job


class JobCreateForm(forms.Form):
    job_type = forms.ChoiceField(
        label='Type de job',
        choices=[
            (Job.JobType.DEMO, 'Test pipeline'),
            (Job.JobType.NORMALIZER, 'Normalizer (moteur réel V1)'),
            (Job.JobType.MATCHER, 'Matcher (stub)'),
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

    def clean_input_file_1(self):
        uploaded = self.cleaned_data['input_file_1']
        if uploaded.size <= 0:
            raise forms.ValidationError('Le fichier principal est vide.')
        return uploaded

    def clean(self):
        cleaned = super().clean()
        job_type = cleaned.get('job_type')
        input_file_1 = cleaned.get('input_file_1')
        if job_type == Job.JobType.NORMALIZER and input_file_1:
            allowed_ext = {'.xlsx', '.xlsm', '.xltx', '.xltm'}
            filename = input_file_1.name.lower()
            if not any(filename.endswith(ext) for ext in allowed_ext):
                self.add_error('input_file_1', 'Le normalizer web supporte pour le moment uniquement les fichiers Excel .xlsx/.xlsm/.xltx/.xltm.')
            if not cleaned.get('normalizer_do_clean') and not cleaned.get('normalizer_do_matchcode'):
                self.add_error(None, 'Sélectionne au moins une opération pour le normalizer.')
        return cleaned

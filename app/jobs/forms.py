from django import forms

from .models import Job


class JobCreateForm(forms.Form):
    job_type = forms.ChoiceField(
        label='Type de job',
        choices=[
            (Job.JobType.DEMO, 'Test pipeline'),
            (Job.JobType.NORMALIZER, 'Normalizer (stub)'),
            (Job.JobType.MATCHER, 'Matcher (stub)'),
            (Job.JobType.GEOCODER, 'Geocoder (stub)'),
        ],
        initial=Job.JobType.DEMO,
    )
    input_file_1 = forms.FileField(label='Fichier source', required=True)
    input_file_2 = forms.FileField(label='Second fichier (optionnel)', required=False)

    def clean_input_file_1(self):
        uploaded = self.cleaned_data['input_file_1']
        if uploaded.size <= 0:
            raise forms.ValidationError('Le fichier principal est vide.')
        return uploaded

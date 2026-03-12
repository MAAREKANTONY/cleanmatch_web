import os
import time
from pathlib import Path

from celery import shared_task
from django.core.files.base import ContentFile

from jobs.models import Job
from jobs.services import JobService


def _read_text_preview(path: str, limit: int = 4000) -> str:
    if not path or not os.path.exists(path):
        return 'Aperçu indisponible : fichier introuvable.'
    try:
        with open(path, 'rb') as fh:
            raw = fh.read(limit)
        return raw.decode('utf-8', errors='replace')
    except Exception as exc:
        return f'Aperçu indisponible : {exc}'


@shared_task(bind=True)
def run_uploaded_job(self, job_id: str):
    job = Job.objects.get(id=job_id)
    input_path = job.input_file_1.path if job.input_file_1 else ''
    second_input_path = job.input_file_2.path if job.input_file_2 else ''

    try:
        JobService.mark_running(job, 'Initialisation du traitement')
        JobService.update_progress(job, 5, 'Vérification des fichiers uploadés')
        time.sleep(1)

        if input_path and not os.path.exists(input_path):
            raise FileNotFoundError(f'Fichier principal introuvable : {input_path}')

        primary_size = os.path.getsize(input_path) if input_path and os.path.exists(input_path) else 0
        secondary_size = os.path.getsize(second_input_path) if second_input_path and os.path.exists(second_input_path) else 0
        JobService.append_runtime_log(
            job,
            f"Fichier principal : {Path(input_path).name if input_path else '-'} ({primary_size} bytes)",
        )
        if second_input_path:
            JobService.append_runtime_log(
                job,
                f"Fichier secondaire : {Path(second_input_path).name} ({secondary_size} bytes)",
            )

        for percent, message in [
            (20, 'Lecture des métadonnées du fichier'),
            (40, 'Simulation du pré-traitement'),
            (60, 'Simulation du traitement asynchrone'),
            (80, 'Génération du livrable de sortie'),
        ]:
            time.sleep(1)
            job.refresh_from_db()
            JobService.update_progress(job, percent, message)

        preview = _read_text_preview(input_path)
        output_lines = [
            'CleanMatch Web - Iteration 2',
            f'Job ID: {job.id}',
            f'Type: {job.job_type}',
            f'Fichier principal: {Path(input_path).name if input_path else "-"}',
            f'Taille fichier principal: {primary_size} bytes',
            f'Fichier secondaire: {Path(second_input_path).name if second_input_path else "-"}',
            f'Taille fichier secondaire: {secondary_size} bytes',
            '',
            'Aperçu du fichier principal (premiers octets décodés en UTF-8 avec remplacement) :',
            preview,
            '',
            'Ce résultat reste un stub technique : la prochaine itération branchera le vrai moteur métier.',
        ]
        output_name = f'result_{job.id}.txt'
        output_content = '\n'.join(output_lines)

        job.refresh_from_db()
        job.output_file.save(output_name, ContentFile(output_content.encode('utf-8')), save=False)
        JobService.mark_success(job, message='Job terminé avec succès')
    except Exception as exc:
        job.refresh_from_db()
        JobService.mark_failed(job, str(exc))
        raise

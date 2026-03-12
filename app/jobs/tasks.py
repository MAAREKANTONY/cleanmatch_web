import os
import time
from pathlib import Path

from celery import shared_task
from django.core.files import File
from django.core.files.base import ContentFile

from jobs.models import Job
from jobs.services import JobService
from normalizer.services.normalizer_service import NormalizerOptions, NormalizerService


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
        if job.job_type == Job.JobType.NORMALIZER:
            return _run_normalizer_job(job)
        return _run_stub_job(job, input_path, second_input_path)
    except Exception as exc:
        job.refresh_from_db()
        JobService.mark_failed(job, str(exc))
        raise


def _run_normalizer_job(job: Job):
    parameters = job.parameters_json or {}
    input_path = Path(job.input_file_1.path)
    output_name = _build_normalizer_output_name(input_path, parameters)
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db()
        JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db()
        JobService.append_runtime_log(job, message)

    service = NormalizerService(progress_callback=progress, log_callback=log)
    options = NormalizerOptions(
        do_clean=bool(parameters.get('do_clean', True)),
        do_matchcode=bool(parameters.get('do_matchcode', True)),
        sheet_name=(parameters.get('sheet_name') or '').strip() or None,
    )

    log('🚀 Lancement du normalizer web V1')
    log(f'📂 Fichier source : {input_path.name}')
    result_path = service.run(input_path=input_path, output_path=output_path, options=options)

    job.refresh_from_db()
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='Normalizer terminé avec succès')
    return str(job.id)


def _build_normalizer_output_name(input_path: Path, parameters: dict) -> str:
    stem = input_path.stem
    for suffix in ['_enriched', '_cleaned', '_matchcoded', '_normalized']:
        if stem.endswith(suffix):
            stem = stem[:-len(suffix)]
            break
    do_clean = bool(parameters.get('do_clean', True))
    do_matchcode = bool(parameters.get('do_matchcode', True))
    if do_clean and do_matchcode:
        suffix = '_normalized.xlsx'
    elif do_clean:
        suffix = '_cleaned.xlsx'
    else:
        suffix = '_matchcoded.xlsx'
    return f'{stem}{suffix}'


def _run_stub_job(job: Job, input_path: str, second_input_path: str):
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
        'CleanMatch Web - Iteration 3',
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
        'Normalizer branché. Matcher et Geocoder restent encore en stub technique.',
    ]
    output_name = f'result_{job.id}.txt'
    output_content = '\n'.join(output_lines)

    job.refresh_from_db()
    job.output_file.save(output_name, ContentFile(output_content.encode('utf-8')), save=False)
    JobService.mark_success(job, message='Job terminé avec succès')
    return str(job.id)

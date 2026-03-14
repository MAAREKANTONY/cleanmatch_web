from __future__ import annotations

from dataclasses import dataclass
from shutil import disk_usage
from typing import Iterable

from celery import current_app
from django.conf import settings
from django.db.models import Q
from django.utils import timezone

from jobs.models import Job


class JobCancelledError(Exception):
    pass


@dataclass(slots=True)
class DiskSpaceStatus:
    free_bytes: int
    threshold_bytes: int

    @property
    def has_enough_space(self) -> bool:
        return self.free_bytes >= self.threshold_bytes


class JobService:
    @staticmethod
    def mark_queued(job: Job, celery_task_id: str = '') -> Job:
        job.status = Job.Status.QUEUED
        job.progress_percent = 0
        job.progress_message = "Job mis en file d'attente"
        job.cancellation_requested = False
        job.cancellation_reason = ''
        job.cancelled_at = None
        job.beat()
        if celery_task_id:
            job.celery_task_id = celery_task_id
        job.save(update_fields=[
            'status', 'progress_percent', 'progress_message', 'celery_task_id',
            'cancellation_requested', 'cancellation_reason', 'cancelled_at', 'last_heartbeat'
        ])
        return job

    @staticmethod
    def mark_running(job: Job, message: str = 'Job en cours') -> Job:
        job.status = Job.Status.RUNNING
        job.started_at = job.started_at or timezone.now()
        job.progress_message = message
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] {message}")
        job.save(update_fields=['status', 'started_at', 'progress_message', 'log_text', 'last_heartbeat'])
        return job

    @staticmethod
    def append_runtime_log(job: Job, message: str) -> Job:
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] {message}")
        job.save(update_fields=['log_text', 'last_heartbeat'])
        return job

    @staticmethod
    def update_progress(job: Job, percent: int, message: str) -> Job:
        job.progress_percent = max(0, min(100, int(percent)))
        job.progress_message = message
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] {message} ({job.progress_percent}%)")
        job.save(update_fields=['progress_percent', 'progress_message', 'log_text', 'last_heartbeat'])
        return job

    @staticmethod
    def heartbeat(job: Job, message: str | None = None) -> Job:
        job.beat()
        if message:
            job.progress_message = message
            job.append_log(f"[{timezone.now().isoformat()}] ♥ {message}")
            job.save(update_fields=['last_heartbeat', 'progress_message', 'log_text'])
        else:
            job.save(update_fields=['last_heartbeat'])
        return job

    @staticmethod
    def mark_success(job: Job, message: str = 'Job terminé') -> Job:
        job.status = Job.Status.SUCCESS
        job.progress_percent = 100
        job.progress_message = message
        job.finished_at = timezone.now()
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] {message}")
        job.save()
        return job

    @staticmethod
    def mark_failed(job: Job, error_message: str) -> Job:
        job.status = Job.Status.FAILED
        job.progress_message = 'Job en erreur'
        job.error_message = error_message
        job.finished_at = timezone.now()
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] ERROR: {error_message}")
        job.save()
        return job

    @staticmethod
    def request_cancel(job: Job, reason: str = 'Annulation demandée par l’utilisateur') -> Job:
        job.cancellation_requested = True
        job.cancellation_reason = reason[:255]
        job.cancelled_at = timezone.now()
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] CANCEL_REQUESTED: {job.cancellation_reason}")
        job.save(update_fields=['cancellation_requested', 'cancellation_reason', 'cancelled_at', 'last_heartbeat', 'log_text'])
        if job.celery_task_id:
            current_app.control.revoke(job.celery_task_id, terminate=False)
        if job.status in {Job.Status.PENDING, Job.Status.QUEUED}:
            return JobService.mark_cancelled(job, job.cancellation_reason)
        return job

    @staticmethod
    def mark_cancelled(job: Job, reason: str = 'Job annulé') -> Job:
        job.status = Job.Status.CANCELLED
        job.progress_message = 'Job annulé'
        job.error_message = reason
        job.finished_at = timezone.now()
        job.cancelled_at = job.cancelled_at or timezone.now()
        job.beat()
        job.append_log(f"[{timezone.now().isoformat()}] CANCELLED: {reason}")
        job.save()
        return job

    @staticmethod
    def enforce_not_cancelled(job: Job) -> None:
        job.refresh_from_db(fields=['status', 'cancellation_requested', 'cancellation_reason'])
        if job.status == Job.Status.CANCELLED or job.cancellation_requested:
            raise JobCancelledError(job.cancellation_reason or 'Job annulé')

    @staticmethod
    def get_disk_space_status(path: str) -> DiskSpaceStatus:
        _, _, free_bytes = disk_usage(path)
        threshold_mb = int(getattr(settings, 'JOB_MIN_FREE_DISK_MB', 1024))
        return DiskSpaceStatus(free_bytes=free_bytes, threshold_bytes=threshold_mb * 1024 * 1024)

    @staticmethod
    def ensure_disk_space(path: str) -> None:
        status = JobService.get_disk_space_status(path)
        if not status.has_enough_space:
            free_mb = round(status.free_bytes / 1024 / 1024, 1)
            threshold_mb = round(status.threshold_bytes / 1024 / 1024, 1)
            raise RuntimeError(
                f"Espace disque insuffisant pour lancer le job : {free_mb} MB libres, minimum requis {threshold_mb} MB."
            )

    @staticmethod
    def fail_stale_jobs() -> list[str]:
        now = timezone.now()
        running_timeout = getattr(settings, 'JOB_STALE_RUNNING_MINUTES', 30)
        queued_timeout = getattr(settings, 'JOB_STALE_QUEUED_MINUTES', 60)
        running_cutoff = now - timezone.timedelta(minutes=running_timeout)
        queued_cutoff = now - timezone.timedelta(minutes=queued_timeout)

        stale_jobs: Iterable[Job] = Job.objects.filter(
            Q(status=Job.Status.RUNNING, last_heartbeat__lt=running_cutoff) |
            Q(status=Job.Status.QUEUED, created_at__lt=queued_cutoff, last_heartbeat__lt=queued_cutoff)
        )
        updated = []
        for job in stale_jobs:
            reason = (
                f"Job auto-fail: heartbeat expiré. Dernier heartbeat: {job.last_heartbeat or 'jamais'}"
            )
            JobService.mark_failed(job, reason)
            updated.append(str(job.id))
        return updated


@staticmethod
def _safe_delete_field_file(field_file) -> bool:
    if not field_file:
        return False
    try:
        storage = field_file.storage
        name = field_file.name
        if name and storage.exists(name):
            storage.delete(name)
            return True
    except Exception:
        return False
    return False

@staticmethod
def delete_job_files(job: Job, delete_input: bool = False, delete_output: bool = False, delete_error: bool = False) -> dict:
    deleted = {'input_file_1': False, 'input_file_2': False, 'output_file': False, 'error_file': False}
    if delete_input:
        deleted['input_file_1'] = JobService._safe_delete_field_file(job.input_file_1)
        deleted['input_file_2'] = JobService._safe_delete_field_file(job.input_file_2)
        if job.input_file_1:
            job.input_file_1 = None
        if job.input_file_2:
            job.input_file_2 = None
    if delete_output:
        deleted['output_file'] = JobService._safe_delete_field_file(job.output_file)
        if job.output_file:
            job.output_file = None
    if delete_error:
        deleted['error_file'] = JobService._safe_delete_field_file(job.error_file)
        if job.error_file:
            job.error_file = None
    update_fields = []
    if delete_input:
        update_fields += ['input_file_1', 'input_file_2']
    if delete_output:
        update_fields += ['output_file']
    if delete_error:
        update_fields += ['error_file']
    if update_fields:
        job.append_log(f"[{timezone.now().isoformat()}] CLEANUP: delete_input={delete_input} delete_output={delete_output} delete_error={delete_error}")
        update_fields += ['log_text']
        job.save(update_fields=update_fields)
    return deleted

@staticmethod
def delete_job(job: Job, delete_files: bool = False) -> None:
    if job.status in {Job.Status.RUNNING, Job.Status.PENDING}:
        raise RuntimeError('Impossible de supprimer un job en cours.')
    if job.status == Job.Status.QUEUED and not job.cancellation_requested:
        raise RuntimeError('Annule d’abord le job avant de le supprimer.')
    if delete_files:
        JobService.delete_job_files(job, delete_input=True, delete_output=True, delete_error=True)
    job.delete()

@staticmethod
def media_storage_stats() -> dict:
    from pathlib import Path
    from django.conf import settings

    media_root = Path(settings.MEDIA_ROOT)
    buckets = {'inputs': 0, 'outputs': 0, 'errors': 0, 'temp': 0, 'other': 0, 'total': 0}
    counts = {'inputs': 0, 'outputs': 0, 'errors': 0, 'temp': 0, 'other': 0, 'total': 0}
    if not media_root.exists():
        return {'bytes': buckets, 'counts': counts}
    for file_path in media_root.rglob('*'):
        if not file_path.is_file():
            continue
        size = file_path.stat().st_size
        rel = file_path.relative_to(media_root).parts
        bucket = rel[0] if rel and rel[0] in {'inputs', 'outputs', 'errors', 'temp'} else 'other'
        buckets[bucket] += size
        buckets['total'] += size
        counts[bucket] += 1
        counts['total'] += 1
    return {'bytes': buckets, 'counts': counts}

@staticmethod
def human_bytes(value: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}" if unit != 'B' else f"{int(amount)} B"
        amount /= 1024

@staticmethod
def cleanup_old_jobs(days: int = 30, statuses: tuple[str, ...] | None = None, delete_files: bool = True) -> dict:
    statuses = statuses or (Job.Status.SUCCESS, Job.Status.FAILED, Job.Status.CANCELLED)
    cutoff = timezone.now() - timezone.timedelta(days=days)
    qs = Job.objects.filter(status__in=statuses, created_at__lt=cutoff)
    deleted_jobs = 0
    deleted_files = 0
    for job in qs.iterator():
        if delete_files:
            result = JobService.delete_job_files(job, delete_input=True, delete_output=True, delete_error=True)
            deleted_files += sum(1 for v in result.values() if v)
        job.delete()
        deleted_jobs += 1
    return {'deleted_jobs': deleted_jobs, 'deleted_files': deleted_files}

@staticmethod
def cleanup_orphan_files() -> dict:
    from pathlib import Path
    from django.conf import settings

    media_root = Path(settings.MEDIA_ROOT)
    media_root.mkdir(parents=True, exist_ok=True)
    referenced = set()
    for field_name in ['input_file_1', 'input_file_2', 'output_file', 'error_file']:
        referenced.update(
            Job.objects.exclude(**{field_name: ''}).exclude(**{f'{field_name}__isnull': True}).values_list(field_name, flat=True)
        )
    deleted = []
    for folder in ['inputs', 'outputs', 'errors', 'temp']:
        dir_path = media_root / folder
        if not dir_path.exists():
            continue
        for file_path in dir_path.rglob('*'):
            if not file_path.is_file():
                continue
            rel = file_path.relative_to(media_root).as_posix()
            if rel not in referenced:
                try:
                    file_path.unlink()
                    deleted.append(rel)
                except FileNotFoundError:
                    pass
    return {'deleted_count': len(deleted), 'deleted_files': deleted[:50]}

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

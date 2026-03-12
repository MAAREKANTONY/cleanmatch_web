from django.utils import timezone
from jobs.models import Job


class JobService:
    @staticmethod
    def mark_queued(job: Job, celery_task_id: str = '') -> Job:
        job.status = Job.Status.QUEUED
        job.progress_percent = 0
        job.progress_message = "Job mis en file d'attente"
        if celery_task_id:
            job.celery_task_id = celery_task_id
        job.save(update_fields=['status', 'progress_percent', 'progress_message', 'celery_task_id'])
        return job

    @staticmethod
    def mark_running(job: Job, message: str = 'Job en cours') -> Job:
        job.status = Job.Status.RUNNING
        job.started_at = job.started_at or timezone.now()
        job.progress_message = message
        job.append_log(f"[{timezone.now().isoformat()}] {message}")
        job.save(update_fields=['status', 'started_at', 'progress_message', 'log_text'])
        return job

    @staticmethod
    def append_runtime_log(job: Job, message: str) -> Job:
        job.append_log(f"[{timezone.now().isoformat()}] {message}")
        job.save(update_fields=['log_text'])
        return job

    @staticmethod
    def update_progress(job: Job, percent: int, message: str) -> Job:
        job.progress_percent = max(0, min(100, int(percent)))
        job.progress_message = message
        job.append_log(f"[{timezone.now().isoformat()}] {message} ({job.progress_percent}%)")
        job.save(update_fields=['progress_percent', 'progress_message', 'log_text'])
        return job

    @staticmethod
    def mark_success(job: Job, message: str = 'Job terminé') -> Job:
        job.status = Job.Status.SUCCESS
        job.progress_percent = 100
        job.progress_message = message
        job.finished_at = timezone.now()
        job.append_log(f"[{timezone.now().isoformat()}] {message}")
        job.save()
        return job

    @staticmethod
    def mark_failed(job: Job, error_message: str) -> Job:
        job.status = Job.Status.FAILED
        job.progress_message = 'Job en erreur'
        job.error_message = error_message
        job.finished_at = timezone.now()
        job.append_log(f"[{timezone.now().isoformat()}] ERROR: {error_message}")
        job.save()
        return job

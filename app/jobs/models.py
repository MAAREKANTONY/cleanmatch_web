import uuid
from django.db import models
from django.utils import timezone


class Job(models.Model):
    class JobType(models.TextChoices):
        DEMO = 'demo', 'Demo'
        NORMALIZER = 'normalizer', 'Normalizer'
        MATCHER = 'matcher', 'Matcher'
        GEOCODER = 'geocoder', 'Geocoder'

    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        QUEUED = 'queued', 'Queued'
        RUNNING = 'running', 'Running'
        SUCCESS = 'success', 'Success'
        FAILED = 'failed', 'Failed'
        CANCELLED = 'cancelled', 'Cancelled'

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job_type = models.CharField(max_length=32, choices=JobType.choices, default=JobType.DEMO)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING)
    progress_percent = models.PositiveSmallIntegerField(default=0)
    progress_message = models.CharField(max_length=255, blank=True)
    parameters_json = models.JSONField(default=dict, blank=True)
    input_file_1 = models.FileField(upload_to='inputs/', blank=True, null=True)
    input_file_2 = models.FileField(upload_to='inputs/', blank=True, null=True)
    output_file = models.FileField(upload_to='outputs/', blank=True, null=True)
    error_file = models.FileField(upload_to='errors/', blank=True, null=True)
    log_text = models.TextField(blank=True)
    error_message = models.TextField(blank=True)
    celery_task_id = models.CharField(max_length=255, blank=True)
    cancellation_requested = models.BooleanField(default=False)
    cancellation_reason = models.CharField(max_length=255, blank=True)
    cancelled_at = models.DateTimeField(blank=True, null=True)
    last_heartbeat = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.job_type} - {self.id}"

    def append_log(self, message: str):
        self.log_text = (self.log_text or '') + message + '\n'

    @property
    def is_finished(self) -> bool:
        return self.status in {self.Status.SUCCESS, self.Status.FAILED, self.Status.CANCELLED}

    def beat(self):
        self.last_heartbeat = timezone.now()

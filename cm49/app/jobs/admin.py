from django.contrib import admin
from .models import Job


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ('id', 'job_type', 'status', 'progress_percent', 'created_at', 'started_at', 'finished_at')
    list_filter = ('job_type', 'status', 'created_at')
    search_fields = ('id', 'celery_task_id', 'progress_message', 'error_message')
    readonly_fields = ('created_at', 'started_at', 'finished_at', 'log_text')

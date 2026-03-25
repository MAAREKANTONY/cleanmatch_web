from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from jobs.models import Job
from jobs.services import JobService


class JobReliabilityTests(TestCase):
    def test_request_cancel_revokes_running_task_with_terminate(self):
        job = Job.objects.create(
            job_type=Job.JobType.AI_REVIEW,
            status=Job.Status.RUNNING,
            celery_task_id='task-running-1',
            started_at=timezone.now(),
        )

        with patch('jobs.services.current_app.control.revoke') as revoke_mock:
            JobService.request_cancel(job, 'manual kill')

        revoke_mock.assert_called_once()
        _, kwargs = revoke_mock.call_args
        self.assertTrue(kwargs.get('terminate'))
        self.assertEqual(job.cancellation_requested, True)

    def test_fail_stale_jobs_marks_orphan_running_job_failed(self):
        job = Job.objects.create(
            job_type=Job.JobType.AI_REVIEW,
            status=Job.Status.RUNNING,
            celery_task_id='missing-task-1',
            started_at=timezone.now() - timezone.timedelta(minutes=20),
            last_heartbeat=timezone.now() - timezone.timedelta(minutes=6),
        )

        with patch.object(JobService, 'celery_known_task_ids', return_value=set()):
            updated = JobService.fail_stale_jobs()

        job.refresh_from_db()
        self.assertIn(str(job.id), updated)
        self.assertEqual(job.status, Job.Status.FAILED)
        self.assertIn('task Celery introuvable', job.error_message)

    def test_fail_stale_jobs_keeps_recent_running_job_when_task_missing(self):
        job = Job.objects.create(
            job_type=Job.JobType.AI_REVIEW,
            status=Job.Status.RUNNING,
            celery_task_id='missing-task-2',
            started_at=timezone.now() - timezone.timedelta(minutes=2),
            last_heartbeat=timezone.now() - timezone.timedelta(minutes=2),
        )

        with patch.object(JobService, 'celery_known_task_ids', return_value=set()):
            updated = JobService.fail_stale_jobs()

        job.refresh_from_db()
        self.assertNotIn(str(job.id), updated)
        self.assertEqual(job.status, Job.Status.RUNNING)

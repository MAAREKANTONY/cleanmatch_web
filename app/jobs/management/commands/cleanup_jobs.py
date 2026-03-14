
from django.core.management.base import BaseCommand
from jobs.services import JobService


class Command(BaseCommand):
    help = 'Supprime les jobs terminés plus vieux que X jours, avec leurs fichiers.'

    def add_arguments(self, parser):
        parser.add_argument('--days', type=int, default=30)

    def handle(self, *args, **options):
        result = JobService.cleanup_old_jobs(days=options['days'], delete_files=True)
        self.stdout.write(self.style.SUCCESS(
            f"Deleted {result['deleted_jobs']} jobs and {result['deleted_files']} files"
        ))


from django.core.management.base import BaseCommand
from jobs.services import JobService


class Command(BaseCommand):
    help = 'Supprime les fichiers orphelins du MEDIA_ROOT.'

    def handle(self, *args, **options):
        result = JobService.cleanup_orphan_files()
        self.stdout.write(self.style.SUCCESS(
            f"Deleted {result['deleted_count']} orphan files"
        ))

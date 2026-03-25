from __future__ import annotations

import shutil
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from ai_review.services.ai_review_service import AI_CACHE_SUBDIR, AI_LLM_CACHE_SUBDIR


class Command(BaseCommand):
    help = 'Delete AI Review web and LLM cache folders under MEDIA_ROOT/outputs.'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='List cache directories without deleting them.')

    def handle(self, *args, **options):
        media_root = Path(settings.MEDIA_ROOT)
        outputs_root = media_root / 'outputs'
        targets = []
        for subdir in (AI_CACHE_SUBDIR, AI_LLM_CACHE_SUBDIR):
            targets.extend(outputs_root.rglob(subdir)) if outputs_root.exists() else None

        if not targets:
            self.stdout.write(self.style.WARNING(f'No AI Review cache directories found under {outputs_root}'))
            return

        deleted = 0
        for path in sorted(set(targets)):
            if options['dry_run']:
                self.stdout.write(f'DRY RUN {path}')
                continue
            shutil.rmtree(path, ignore_errors=True)
            self.stdout.write(self.style.SUCCESS(f'Deleted {path}'))
            deleted += 1

        if options['dry_run']:
            self.stdout.write(self.style.WARNING('Dry run completed. No files were deleted.'))
        else:
            self.stdout.write(self.style.SUCCESS(f'AI Review cache cleanup completed. Deleted {deleted} directories.'))

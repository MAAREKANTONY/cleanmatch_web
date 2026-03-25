from __future__ import annotations

from django.core.management.base import BaseCommand

from core.config_loader import DEFAULT_CATALOG_DIR, OVERRIDE_CATALOG_DIR, bootstrap_config_overrides


class Command(BaseCommand):
    help = 'Initialise le dossier de configuration override persistant sans écraser les fichiers existants.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Recopie aussi les fichiers déjà présents dans le dossier override.',
        )

    def handle(self, *args, **options):
        force = bool(options.get('force'))
        copied = bootstrap_config_overrides(force=force)
        self.stdout.write(self.style.SUCCESS(f'Catalog default: {DEFAULT_CATALOG_DIR}'))
        self.stdout.write(self.style.SUCCESS(f'Catalog override: {OVERRIDE_CATALOG_DIR}'))
        if copied:
            self.stdout.write(self.style.SUCCESS(f'{len(copied)} fichier(s) copié(s) dans le dossier override.'))
            for path in copied[:20]:
                self.stdout.write(f' - {path}')
            if len(copied) > 20:
                self.stdout.write(f' ... {len(copied) - 20} autre(s) fichier(s)')
            return
        self.stdout.write('Aucun fichier copié : le dossier override était déjà initialisé.')

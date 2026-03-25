from __future__ import annotations

from django.conf import settings

from .config_loader import catalog_version


def app_meta(request):
    version_meta = catalog_version()
    default_label = str(version_meta.get('display_label') or version_meta.get('notes') or version_meta.get('catalog_version') or 'CleanMatch Web').strip()
    default_summary = str(version_meta.get('summary') or version_meta.get('notes') or '').strip()
    return {
        'app_iteration_label': settings.APP_DISPLAY_VERSION or default_label,
        'app_iteration_summary': default_summary,
    }

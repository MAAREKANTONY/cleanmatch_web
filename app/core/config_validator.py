from __future__ import annotations

from pathlib import Path

from .config_loader import CATALOG_DIR, load_yaml_config


def validate_catalog_minimal() -> list[str]:
    errors: list[str] = []
    required = [
        'version.yaml',
        'normalizer/mapping_fields.yaml',
        'matcher/mapping_fields.yaml',
        'geocoder/mapping_fields.yaml',
        'geoclass/mapping_fields.yaml',
        'marketsegmenter/mapping_fields.yaml',
    ]
    for rel in required:
        if not (CATALOG_DIR / rel).exists():
            errors.append(f'Missing config: {rel}')
    for rel in [r for r in required if (CATALOG_DIR / r).exists()]:
        try:
            load_yaml_config(rel)
        except Exception as exc:
            errors.append(f'Invalid config {rel}: {exc}')
    return errors


def validate_or_raise() -> None:
    errors = validate_catalog_minimal()
    if errors:
        raise RuntimeError('Config catalog invalid: ' + '; '.join(errors))

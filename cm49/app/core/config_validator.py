from __future__ import annotations

from core.config_loader import DEFAULT_CATALOG_DIR, OVERRIDE_CATALOG_DIR, load_yaml_config


REQUIRED_CONFIGS = [
    'version.yaml',
    'normalizer/mapping_fields.yaml',
    'matcher/mapping_fields.yaml',
    'geocoder/mapping_fields.yaml',
    'geoclass/mapping_fields.yaml',
    'marketsegmenter/mapping_fields.yaml',
    'ai_review/mapping_fields.yaml',
]



def validate_catalog_minimal() -> list[str]:
    errors: list[str] = []
    for rel in REQUIRED_CONFIGS:
        default_path = DEFAULT_CATALOG_DIR / rel
        if not default_path.exists():
            errors.append(f'Missing default config: {rel}')
            continue
        try:
            load_yaml_config(rel)
        except Exception as exc:
            errors.append(f'Invalid config {rel}: {exc}')
    if OVERRIDE_CATALOG_DIR.exists() and not OVERRIDE_CATALOG_DIR.is_dir():
        errors.append(f'Invalid override directory: {OVERRIDE_CATALOG_DIR}')
    return errors



def validate_or_raise() -> None:
    errors = validate_catalog_minimal()
    if errors:
        raise RuntimeError('Config catalog invalid: ' + '; '.join(errors))

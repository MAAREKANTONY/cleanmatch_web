from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from geocoder.services.geocoder_service import GeocoderOptions, GeocoderService


def test_geocoder_reuses_existing_coordinates_and_writes_summary():
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        src = root / 'input.csv'
        out = root / 'out.csv'
        pd.DataFrame([{'id': '1', 'address': '1 rue de Paris', 'zipcode': '75001', 'city': 'Paris', 'lat': 48.86, 'lng': 2.34}]).to_csv(src, index=False)
        service = GeocoderService()
        service.run(src, out, GeocoderOptions(provider='existing_only'))
        out_df = pd.read_csv(out)
        assert out_df.iloc[0]['geocoder_status'] == 'resolved_existing'
        assert out.with_name(out.stem + '_summary.json').exists()


def test_geocoder_checkpoint_resume_is_readable():
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        src = root / 'input.csv'
        out = root / 'out.csv'
        pd.DataFrame([{'id': '1', 'address': '1 rue de Paris', 'zipcode': '75001', 'city': 'Paris'}]).to_csv(src, index=False)
        checkpoint = out.with_suffix('.checkpoint.json')
        checkpoint.write_text('{"1": {"lat": "", "lng": "", "geocoder_status": "unresolved", "geocoder_source": "existing_only", "geocoder_query": "x", "geocoder_label": ""}}', encoding='utf-8')
        service = GeocoderService()
        service.run(src, out, GeocoderOptions(provider='existing_only'))
        out_df = pd.read_csv(out)
        assert out_df.iloc[0]['geocoder_status'] == 'unresolved'

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from geoclass.services.geoclass_service import GeoclassOptions, GeoclassService


def test_geoclass_classifies_restaurant_keyword():
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        src = root / 'input.csv'
        out = root / 'out.csv'
        pd.DataFrame([{'id': '1', 'name': 'Ristorante Roma'}]).to_csv(src, index=False)
        GeoclassService().run(src, out, GeoclassOptions())
        out_df = pd.read_csv(out)
        assert out_df.iloc[0]['geoclass_category'] == 'food_service'

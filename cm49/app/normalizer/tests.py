from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from django.test import SimpleTestCase

from normalizer.services.normalizer_service import NormalizerOptions, NormalizerService


class NormalizerServiceStabilizationTests(SimpleTestCase):
    def _run_case(self, rows, country_code='FR'):
        with TemporaryDirectory() as tmp:
            src = Path(tmp) / 'input.xlsx'
            out = Path(tmp) / 'output.csv'
            pd.DataFrame(rows).to_excel(src, index=False)
            service = NormalizerService()
            service.run(src, out, NormalizerOptions(country_code=country_code))
            return pd.read_csv(out)

    def test_fr_keeps_matchcode_and_siret(self):
        df = self._run_case([
            {
                'name': 'Cafe Paris',
                'address': '12 rue de Rivoli',
                'zipcode': '75001',
                'city': 'Paris',
                'country': 'France',
                'legal_id': 'FR 123 456 789 00012',
            }
        ])
        self.assertEqual(df.loc[0, 'country'], 'FR')
        self.assertEqual(str(df.loc[0, 'legal_id']), '12345678900012')
        self.assertEqual(df.loc[0, 'legal_id_type'], 'siret')
        self.assertTrue(str(df.loc[0, 'matchcode']).startswith('FR-75001-'))

    def test_it_alias_columns_are_auto_applied(self):
        df = self._run_case([
            {
                'ragione sociale': 'Bar Roma',
                'final_address': 'Via Roma 12',
                'legalZipCode': '00100',
                'legalCity': 'Roma',
                'country': 'Italia',
                'partita_iva': 'IT12345678901',
            }
        ], country_code='IT')
        self.assertEqual(df.loc[0, 'country'], 'IT')
        self.assertEqual(df.loc[0, 'address'], 'Via Roma 12')
        self.assertEqual(str(df.loc[0, 'zipcode']), '00100')
        self.assertEqual(df.loc[0, 'city'], 'Roma')
        self.assertEqual(str(df.loc[0, 'legal_id']), '12345678901')
        self.assertEqual(df.loc[0, 'legal_id_type'], 'partita_iva')
        self.assertTrue(str(df.loc[0, 'matchcode']).startswith('IT-00100-'))

    def test_es_alias_columns_are_auto_applied(self):
        df = self._run_case([
            {
                'outlet_name': 'Casa Sol',
                'street': 'Calle Mayor 14',
                'postal_code': '28013',
                'locality': 'Madrid',
                'country_code': 'ES',
                'cif': 'B12345678',
            }
        ], country_code='ES')
        self.assertEqual(df.loc[0, 'country'], 'ES')
        self.assertEqual(df.loc[0, 'city'], 'Madrid')
        self.assertEqual(str(df.loc[0, 'legal_id']), 'B12345678')
        self.assertEqual(df.loc[0, 'legal_id_type'], 'nif_cif')
        self.assertTrue(str(df.loc[0, 'matchcode']).startswith('ES-28013-'))

    def test_de_duplicate_country_columns_do_not_break_str_operations(self):
        rows = [
            ['name', 'address', 'zipcode', 'city', 'country', 'country', 'legal_id'],
            ['Brauhaus', '12 Hauptstrasse', '10115', 'Berlin', 'DE', 'Germany', 'DE123456789'],
        ]
        with TemporaryDirectory() as tmp:
            src = Path(tmp) / 'input.xlsx'
            out = Path(tmp) / 'output.csv'
            pd.DataFrame(rows[1:], columns=rows[0]).to_excel(src, index=False)
            service = NormalizerService()
            service.run(src, out, NormalizerOptions(country_code='DE'))
            df = pd.read_csv(out)
        self.assertEqual(df.loc[0, 'country'], 'DE')
        self.assertEqual(str(df.loc[0, 'zipcode']), '10115')
        self.assertTrue(str(df.loc[0, 'matchcode']).startswith('DE-10115-'))

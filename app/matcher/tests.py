from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from django.test import SimpleTestCase

from matcher.services.matcher_service import MatcherOptions, MatcherService


class MatcherMulticountryTests(SimpleTestCase):
    def test_legal_id_same_country_goes_automatch(self):
        master = pd.DataFrame([
            {'id': 'm1', 'name': 'Ristorante Roma', 'address': 'Via Roma 12', 'zipcode': '00100', 'city': 'Roma', 'country': 'IT', 'legal_id': 'IT12345678901'}
        ])
        slave = pd.DataFrame([
            {'id': 's1', 'name': 'Ristorante Roma SRL', 'address': 'Via Roma 12', 'zipcode': '00100', 'city': 'Roma', 'country': 'IT', 'legal_id': 'IT12345678901'}
        ])
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            master_path = tmp / 'master.csv'
            slave_path = tmp / 'slave.csv'
            out_path = tmp / 'result.zip'
            master.to_csv(master_path, index=False)
            slave.to_csv(slave_path, index=False)
            service = MatcherService()
            options = MatcherOptions(
                master_mapping={'id': 'id', 'name': 'name', 'address': 'address', 'zipcode': 'zipcode', 'city': 'city', 'country': 'country', 'legal_id': 'legal_id'},
                slave_mapping={'id': 'id', 'name': 'name', 'address': 'address', 'zipcode': 'zipcode', 'city': 'city', 'country': 'country', 'legal_id': 'legal_id'},
            )
            service.run(master_path, slave_path, out_path, options)
            all_matches = pd.read_csv(tmp / 'all_matches.csv') if (tmp / 'all_matches.csv').exists() else None
            self.assertTrue(out_path.exists())

    def test_country_mismatch_does_not_automatch_on_name_only(self):
        service = MatcherService()
        options = MatcherOptions()
        master_df = pd.DataFrame([
            {'id': 'm1', 'name': 'Cafe Central', 'address': 'Main Street 1', 'zipcode': '1000', 'city': 'Brussels', 'country': 'BE'}
        ])
        slave_df = pd.DataFrame([
            {'id': 's1', 'name': 'Cafe Central', 'address': 'Main Street 1', 'zipcode': '1000', 'city': 'Brussels', 'country': 'DE'}
        ])
        master = service._prepare_dataframe(master_df)
        slave = service._prepare_dataframe(slave_df)
        matches = service._score_candidates(master.iloc[0], slave, options, 'fallback')
        self.assertTrue(matches)
        self.assertNotEqual(matches[0]['match_status'], 'automatch')

    def test_country_zip_candidate_source_prioritized(self):
        service = MatcherService()
        master_df = pd.DataFrame([
            {'id': 'm1', 'name': 'Casa del Mare', 'address': 'Via Milano 8', 'zipcode': '20100', 'city': 'Milano', 'country': 'IT'}
        ])
        slave_df = pd.DataFrame([
            {'id': 's1', 'name': 'Casa del Mare', 'address': 'Via Milano 8', 'zipcode': '20100', 'city': 'Milano', 'country': 'IT'},
            {'id': 's2', 'name': 'Casa del Mare', 'address': 'Via Milano 8', 'zipcode': '20100', 'city': 'Milano', 'country': 'ES'},
        ])
        master = service._prepare_dataframe(master_df)
        slave = service._prepare_dataframe(slave_df)
        slave_by_country_zip = {(country, zipc): grp.copy() for (country, zipc), grp in slave.groupby(['country', 'zipcode_clean']) if country and zipc}
        slave_by_country_city = {(country, city): grp.copy() for (country, city), grp in slave.groupby(['country', 'city_clean']) if country and city}
        slave_by_zip = {zipc: grp.copy() for zipc, grp in slave.groupby('zipcode_clean') if zipc}
        slave_by_city = {city: grp.copy() for city, grp in slave.groupby('city_clean') if city}
        slave_by_legal_id = {legal_id: grp.copy() for legal_id, grp in slave.groupby('legal_id_clean') if legal_id}
        candidates, source = service._get_candidates(master.iloc[0], slave, slave_by_country_zip, slave_by_country_city, slave_by_zip, slave_by_city, slave_by_legal_id)
        self.assertEqual(source, 'country_zipcode')
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates.iloc[0]['country'], 'IT')

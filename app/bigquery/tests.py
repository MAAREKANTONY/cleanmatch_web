from unittest.mock import Mock, patch
from types import SimpleNamespace
from unittest import TestCase

from bigquery.client import BigQueryService


class BigQueryServiceQueryTests(TestCase):
    def _make_service(self):
        with patch.object(BigQueryService, '_build_client', return_value=SimpleNamespace()):
            return BigQueryService()

    @patch('django.conf.settings.BIGQUERY_PROJECT_ID', 'proj')
    @patch('django.conf.settings.BIGQUERY_DATASET', 'dataset')
    @patch('django.conf.settings.BIGQUERY_LOCATION', '')
    @patch('django.conf.settings.BIGQUERY_CREDENTIALS_FILE', '')
    @patch('django.conf.settings.BIGQUERY_INPUT_TABLE', 'google_map_clean')
    def test_build_select_query_without_country(self):
        service = self._make_service()
        sql, config = service.build_select_query('google_map_clean', country_code='', limit=20)
        self.assertEqual(sql, 'SELECT * FROM `proj.dataset.google_map_clean` LIMIT 20')
        self.assertEqual(config.query_parameters, [])

    @patch('django.conf.settings.BIGQUERY_PROJECT_ID', 'proj')
    @patch('django.conf.settings.BIGQUERY_DATASET', 'dataset')
    @patch('django.conf.settings.BIGQUERY_LOCATION', '')
    @patch('django.conf.settings.BIGQUERY_CREDENTIALS_FILE', '')
    @patch('django.conf.settings.BIGQUERY_INPUT_TABLE', 'google_map_clean')
    def test_build_select_query_with_country(self):
        service = self._make_service()
        service.client = Mock()
        service.client.get_table.return_value = type('T', (), {'schema': [type('F', (), {'name': 'country_code'})()]})()
        sql, config = service.build_select_query('google_map_clean', country_code='es', limit=None)
        self.assertEqual(sql, 'SELECT * FROM `proj.dataset.google_map_clean` WHERE UPPER(country_code) = @country_code')
        self.assertEqual(len(config.query_parameters), 1)
        self.assertEqual(config.query_parameters[0].name, 'country_code')
        self.assertEqual(config.query_parameters[0].value, 'ES')


    @patch('django.conf.settings.BIGQUERY_INPUT_TABLE', 'google_map')
    @patch('django.conf.settings.BIGQUERY_DATASET', 'dataset')
    @patch('django.conf.settings.BIGQUERY_PROJECT_ID', 'proj')
    def test_inspect_table_includes_missing_required_and_mappings(self):
        with patch.object(BigQueryService, '_build_client'):
            service = BigQueryService()
            mock_table = type('T', (), {'schema': [type('F', (), {'name': 'google_place_id'})(), type('F', (), {'name': 'country_code'})()], 'num_rows': 12})()
            service.client = Mock()
            service.client.get_table.return_value = mock_table
            with patch.object(service, 'iter_rows', return_value=iter([])):
                payload = service.inspect_table('google_map', country_code='FR', limit=5)
        sheet = payload['sheets'][0]
        self.assertIn('missing_required', sheet)
        self.assertIsInstance(sheet['missing_required'], list)
        self.assertEqual(payload['table_name'], 'google_map')
        self.assertIn('mapping_suggestions', sheet)
        self.assertIn('ai_review_mapping_suggestions', sheet)


    @patch('django.conf.settings.BIGQUERY_PROJECT_ID', 'proj')
    @patch('django.conf.settings.BIGQUERY_DATASET', 'dataset')
    @patch('django.conf.settings.BIGQUERY_LOCATION', '')
    @patch('django.conf.settings.BIGQUERY_CREDENTIALS_FILE', '')
    @patch('django.conf.settings.BIGQUERY_INPUT_TABLE', 'google_map_clean')
    def test_build_select_query_skips_country_filter_when_column_missing(self):
        service = self._make_service()
        service.client = Mock()
        service.client.get_table.return_value = type('T', (), {'schema': [type('F', (), {'name': 'google_place_id'})()]})()
        sql, config = service.build_select_query('google_map_clean', country_code='fr', limit=None)
        self.assertEqual(sql, 'SELECT * FROM `proj.dataset.google_map_clean`')
        self.assertEqual(config.query_parameters, [])

    @patch('django.conf.settings.BIGQUERY_PROJECT_ID', 'proj')
    @patch('django.conf.settings.BIGQUERY_DATASET', 'dataset')
    @patch('django.conf.settings.BIGQUERY_LOCATION', '')
    @patch('django.conf.settings.BIGQUERY_CREDENTIALS_FILE', '')
    @patch('django.conf.settings.BIGQUERY_INPUT_TABLE', 'google_map_clean')
    def test_table_ref_accepts_fully_qualified_name(self):
        service = self._make_service()
        ref = service.table_ref('other_proj.other_ds.some_table')
        self.assertEqual(ref.full_name, 'other_proj.other_ds.some_table')

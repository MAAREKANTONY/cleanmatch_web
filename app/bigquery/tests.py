from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

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
        sql, config = service.build_select_query('google_map_clean', country_code='es', limit=None)
        self.assertEqual(sql, 'SELECT * FROM `proj.dataset.google_map_clean` WHERE UPPER(country_code) = @country_code')
        self.assertEqual(len(config.query_parameters), 1)
        self.assertEqual(config.query_parameters[0].name, 'country_code')
        self.assertEqual(config.query_parameters[0].value, 'ES')

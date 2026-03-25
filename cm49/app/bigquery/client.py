from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Iterable, Tuple
import uuid

from django.conf import settings

from marketsegmenter.services.marketsegmenter_service import MARKETSEGMENTER_REQUIRED_FIELDS, suggest_column_mapping
from ai_review.services.ai_review_service import suggest_ai_review_mapping
from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class BigQueryTableRef:
    project_id: str
    dataset: str
    table_name: str

    @property
    def full_name(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table_name}"


class BigQueryService:
    def __init__(self):
        self.project_id = (settings.BIGQUERY_PROJECT_ID or '').strip()
        self.dataset = (settings.BIGQUERY_DATASET or '').strip()
        self.location = (settings.BIGQUERY_LOCATION or '').strip() or None
        self.credentials_file = (settings.BIGQUERY_CREDENTIALS_FILE or '').strip()
        self._schema_cache: dict[str, list[str]] = {}
        if not self.project_id:
            raise BigQueryConfigError('BIGQUERY_PROJECT_ID est requis.')
        if not self.dataset:
            raise BigQueryConfigError('BIGQUERY_DATASET est requis.')
        self.client = self._build_client()

    def _build_client(self) -> bigquery.Client:
        if self.credentials_file:
            credentials = service_account.Credentials.from_service_account_file(self.credentials_file)
            return bigquery.Client(project=self.project_id, credentials=credentials, location=self.location)
        return bigquery.Client(project=self.project_id, location=self.location)

    def table_ref(self, table_name: str | None) -> BigQueryTableRef:
        normalized = (table_name or '').strip() or settings.BIGQUERY_INPUT_TABLE
        normalized = normalized.strip('`')
        parts = [part for part in normalized.split('.') if part]
        if len(parts) == 3:
            project_id, dataset, table = parts
            return BigQueryTableRef(project_id=project_id, dataset=dataset, table_name=table)
        if len(parts) == 2:
            dataset, table = parts
            return BigQueryTableRef(project_id=self.project_id, dataset=dataset, table_name=table)
        return BigQueryTableRef(project_id=self.project_id, dataset=self.dataset, table_name=normalized)

    def _get_table_columns(self, table_name: str | None) -> list[str]:
        ref = self.table_ref(table_name)
        cache_key = ref.full_name
        if cache_key not in self._schema_cache:
            table = self.client.get_table(ref.full_name)
            self._schema_cache[cache_key] = [field.name for field in table.schema]
        return self._schema_cache[cache_key]

    def build_select_query(self, table_name: str | None, country_code: str = '', limit: int | None = None) -> Tuple[str, bigquery.QueryJobConfig]:
        ref = self.table_ref(table_name)
        sql = f"SELECT * FROM `{ref.full_name}`"
        query_parameters: list[bigquery.query.ScalarQueryParameter] = []
        normalized_country = (country_code or '').strip().upper()
        if normalized_country:
            available_columns = {col.lower() for col in self._get_table_columns(ref.table_name)}
            if 'country_code' in available_columns:
                sql += " WHERE UPPER(country_code) = @country_code"
                query_parameters.append(bigquery.ScalarQueryParameter('country_code', 'STRING', normalized_country))
        if limit is not None:
            safe_limit = max(1, int(limit))
            sql += f" LIMIT {safe_limit}"
        return sql, bigquery.QueryJobConfig(query_parameters=query_parameters)

    def inspect_table(self, table_name: str | None, country_code: str = '', limit: int = 20) -> dict:
        ref = self.table_ref(table_name)
        table = self.client.get_table(ref.full_name)
        columns = [field.name for field in table.schema]
        self._schema_cache[ref.full_name] = columns
        preview_rows = list(self.iter_rows(ref.table_name, country_code=country_code, limit=limit))
        preview = [columns] + [[self._stringify(row.get(col, '')) for col in columns] for row in preview_rows]
        total_rows = table.num_rows
        sql, _ = self.build_select_query(ref.table_name, country_code=country_code, limit=limit)
        marketsegmenter_mapping = suggest_column_mapping(columns)
        ai_review_mapping = suggest_ai_review_mapping(columns)
        return {
            'filename': ref.full_name,
            'kind': 'bigquery',
            'table_name': ref.table_name,
            'full_table_name': ref.full_name,
            'executed_sql_preview': sql,
            'sheets': [{
                'name': '__bigquery__',
                'max_row': total_rows,
                'max_column': len(columns),
                'preview': preview,
                'detected_columns': columns,
                'mapping_suggestions': marketsegmenter_mapping,
                'ai_review_mapping_suggestions': ai_review_mapping,
                'missing_required': sorted(MARKETSEGMENTER_REQUIRED_FIELDS - set(marketsegmenter_mapping.keys())),
            }],
        }

    def iter_rows(self, table_name: str | None, country_code: str = '', limit: int | None = None) -> Iterable[dict[str, object]]:
        sql, job_config = self.build_select_query(table_name=table_name, country_code=country_code, limit=limit)
        page_size = max(1, int(getattr(settings, 'BIGQUERY_EXPORT_PAGE_SIZE', 1000) or 1000))
        result = self.client.query(sql, job_config=job_config, location=self.location).result(page_size=page_size)
        for row in result:
            yield dict(row.items())

    def export_table_to_csv(self, table_name: str | None, output_path: Path, country_code: str = '') -> tuple[Path, list[str], int]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.iter_rows(table_name, country_code=country_code, limit=None)
        count = 0
        headers: list[str] = []
        with output_path.open('w', encoding='utf-8-sig', newline='') as fh:
            writer = None
            for row in rows:
                if not headers:
                    headers = list(row.keys())
                    writer = csv.DictWriter(fh, fieldnames=headers, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
                    writer.writeheader()
                writer.writerow({key: self._stringify(row.get(key, '')) for key in headers})
                count += 1
        if not headers:
            ref = self.table_ref(table_name)
            table = self.client.get_table(ref.full_name)
            headers = [field.name for field in table.schema]
            with output_path.open('w', encoding='utf-8-sig', newline='') as fh:
                writer = csv.DictWriter(fh, fieldnames=headers, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
                writer.writeheader()
        return output_path, headers, count


    def ensure_source_readable(self, table_name: str | None, country_code: str = '') -> dict[str, object]:
        ref = self.table_ref(table_name)
        table = self.client.get_table(ref.full_name)
        columns = [field.name for field in table.schema]
        self._schema_cache[ref.full_name] = columns
        sql, job_config = self.build_select_query(table_name=ref.table_name, country_code=country_code, limit=1)
        list(self.client.query(sql, job_config=job_config, location=self.location).result(max_results=1))
        return {
            'table': ref.full_name,
            'columns': columns,
        }

    def ensure_output_writable(self, table_name: str | None) -> dict[str, object]:
        ref = self.table_ref(table_name or settings.BIGQUERY_OUTPUT_TABLE)
        table_id = ref.full_name
        dataset_ref = bigquery.DatasetReference(ref.project_id, ref.dataset)
        self.client.get_dataset(dataset_ref)
        try:
            table = self.client.get_table(table_id)
            return {'table': table_id, 'created': False, 'exists': True}
        except Exception as exc:
            if getattr(exc, 'code', None) not in {404, '404'}:
                raise

        schema = [
            bigquery.SchemaField('google_place_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('market_segment_type0', 'STRING'),
            bigquery.SchemaField('market_segment_type1', 'STRING'),
            bigquery.SchemaField('market_segment_type2', 'STRING'),
            bigquery.SchemaField('market_segment_type3', 'STRING'),
            bigquery.SchemaField('created_at', 'DATETIME', mode='REQUIRED'),
            bigquery.SchemaField('process_id', 'STRING', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_id, schema=schema)
        self.client.create_table(table, exists_ok=True)
        return {'table': table_id, 'created': True, 'exists': False}

    def _get_output_created_at_mode(self, table_name: str | None) -> str:
        ref = self.table_ref(table_name or settings.BIGQUERY_OUTPUT_TABLE)
        table = self.client.get_table(ref.full_name)
        for field in getattr(table, 'schema', []) or []:
            if getattr(field, 'name', '') == 'created_at':
                return str(getattr(field, 'field_type', 'DATETIME') or 'DATETIME').upper()
        return 'DATETIME'

    def write_segmented_rows(self, table_name: str | None, rows: list[dict[str, object]]) -> int:
        access = self.ensure_output_writable(table_name)
        table_id = access['table']
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f'Échec écriture BigQuery vers {table_id}: {errors[:3]}')
        return len(rows)

    def write_segmented_rows_in_batches(self, table_name: str | None, rows: Iterable[dict[str, object]], batch_size: int | None = None) -> int:
        access = self.ensure_output_writable(table_name)
        table_id = access['table']
        size = max(1, int(batch_size or getattr(settings, 'BIGQUERY_INSERT_BATCH_SIZE', 1000) or 1000))
        total = 0
        iterator = iter(rows)
        while True:
            batch = list(islice(iterator, size))
            if not batch:
                break
            errors = self.client.insert_rows_json(table_id, batch)
            if errors:
                raise RuntimeError(f'Échec écriture BigQuery vers {table_id}: {errors[:3]}')
            total += len(batch)
        return total

    @staticmethod
    def _format_created_at_for_mode(value: datetime, mode: str) -> str:
        normalized_mode = (mode or 'DATETIME').upper()
        aware_utc = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if normalized_mode == 'TIMESTAMP':
            return aware_utc.isoformat()
        naive = aware_utc.replace(tzinfo=None)
        return naive.strftime('%Y-%m-%d %H:%M:%S.%f')

    @classmethod
    def build_segmented_row(cls, *, google_place_id: str, segments: list[str], process_id: str, created_at_mode: str = 'DATETIME') -> dict[str, object]:
        parts = (segments + ['', '', '', ''])[:4]
        return {
            'google_place_id': str(google_place_id),
            'market_segment_type0': parts[0],
            'market_segment_type1': parts[1],
            'market_segment_type2': parts[2],
            'market_segment_type3': parts[3],
            'created_at': cls._format_created_at_for_mode(datetime.now(timezone.utc), created_at_mode),
            'process_id': str(process_id),
        }

    @staticmethod
    def _stringify(value: object) -> str:
        if value is None:
            return ''
        if isinstance(value, (list, dict)):
            import json
            return json.dumps(value, ensure_ascii=False)
        return str(value)

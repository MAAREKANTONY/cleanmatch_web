from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

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
        self._table_schema_cache: dict[str, list[bigquery.SchemaField]] = {}
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

    def _get_table_schema(self, table_name: str | None) -> list[bigquery.SchemaField]:
        ref = self.table_ref(table_name)
        cache_key = ref.full_name
        if cache_key not in self._table_schema_cache:
            table = self.client.get_table(ref.full_name)
            self._table_schema_cache[cache_key] = list(table.schema)
            self._schema_cache[cache_key] = [field.name for field in table.schema]
        return self._table_schema_cache[cache_key]

    def _get_table_columns(self, table_name: str | None) -> list[str]:
        ref = self.table_ref(table_name)
        cache_key = ref.full_name
        if cache_key not in self._schema_cache:
            self._get_table_schema(table_name)
        return self._schema_cache[cache_key]

    def _resolve_selected_columns(self, table_name: str | None, selected_columns: list[str] | None = None) -> list[str]:
        requested = [str(col).strip() for col in (selected_columns or []) if str(col).strip()]
        if not requested:
            return []
        available_columns = self._get_table_columns(table_name)
        available_lookup = {col.lower(): col for col in available_columns}
        resolved: list[str] = []
        seen: set[str] = set()
        for candidate in requested:
            actual = available_lookup.get(candidate.lower())
            if actual and actual not in seen:
                seen.add(actual)
                resolved.append(actual)
        return resolved

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '`' + str(identifier).replace('`', '') + '`'

    def build_select_query(self, table_name: str | None, country_code: str = '', limit: int | None = None, selected_columns: list[str] | None = None) -> Tuple[str, bigquery.QueryJobConfig]:
        ref = self.table_ref(table_name)
        resolved_columns = self._resolve_selected_columns(ref.table_name, selected_columns)
        if resolved_columns:
            select_clause = ', '.join(self._quote_identifier(col) for col in resolved_columns)
        else:
            select_clause = '*'
        sql = f"SELECT {select_clause} FROM `{ref.full_name}`"
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
        self._table_schema_cache[ref.full_name] = list(table.schema)
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

    def iter_rows(self, table_name: str | None, country_code: str = '', limit: int | None = None, page_size: int | None = None, selected_columns: list[str] | None = None) -> Iterable[dict[str, object]]:
        sql, job_config = self.build_select_query(table_name=table_name, country_code=country_code, limit=limit, selected_columns=selected_columns)
        effective_page_size = max(1, int(page_size or getattr(settings, 'BIGQUERY_READ_PAGE_SIZE', 1000) or 1000))
        result = self.client.query(sql, job_config=job_config, location=self.location).result(page_size=effective_page_size)
        for row in result:
            yield dict(row.items())

    def export_table_to_csv(self, table_name: str | None, output_path: Path, country_code: str = '', page_size: int | None = None, selected_columns: list[str] | None = None) -> tuple[Path, list[str], int]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.iter_rows(table_name, country_code=country_code, limit=None, page_size=page_size, selected_columns=selected_columns)
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

    def _prepare_segmented_table(self, table_name: str | None) -> tuple[str, str]:
        ref = self.table_ref(table_name or settings.BIGQUERY_OUTPUT_TABLE)
        table_id = ref.full_name
        schema = [
            bigquery.SchemaField('google_place_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('market_segment_type0', 'STRING'),
            bigquery.SchemaField('market_segment_type1', 'STRING'),
            bigquery.SchemaField('market_segment_type2', 'STRING'),
            bigquery.SchemaField('market_segment_type3', 'STRING'),
            bigquery.SchemaField('created_at', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('process_id', 'STRING', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_id, schema=schema)
        self.client.create_table(table, exists_ok=True)
        actual_schema = self._get_table_schema(ref.table_name)
        created_at_type = next((field.field_type.upper() for field in actual_schema if field.name == 'created_at'), 'TIMESTAMP')
        return table_id, created_at_type

    def write_segmented_rows_iterable(self, table_name: str | None, rows: Iterable[dict[str, object]], batch_size: int | None = None) -> int:
        table_id, created_at_type = self._prepare_segmented_table(table_name)
        safe_batch_size = max(1, int(batch_size or getattr(settings, 'BIGQUERY_WRITE_BATCH_SIZE', 1000) or 1000))
        total_inserted = 0
        batch: list[dict[str, object]] = []
        for row in rows:
            batch.append(self._normalize_segmented_row(row, created_at_type=created_at_type))
            if len(batch) >= safe_batch_size:
                errors = self.client.insert_rows_json(table_id, batch)
                if errors:
                    raise RuntimeError(f'Échec écriture BigQuery vers {table_id}: {errors[:3]}')
                total_inserted += len(batch)
                batch = []
        if batch:
            errors = self.client.insert_rows_json(table_id, batch)
            if errors:
                raise RuntimeError(f'Échec écriture BigQuery vers {table_id}: {errors[:3]}')
            total_inserted += len(batch)
        return total_inserted

    def write_segmented_rows(self, table_name: str | None, rows: list[dict[str, object]], batch_size: int = 1000) -> int:
        return self.write_segmented_rows_iterable(table_name, rows, batch_size=batch_size)

    def delete_segmented_rows_for_process(self, table_name: str | None, process_id: str, google_place_ids: list[str] | None = None) -> int:
        ref = self.table_ref(table_name or settings.BIGQUERY_OUTPUT_TABLE)
        sql = f"DELETE FROM `{ref.full_name}` WHERE process_id = @process_id"
        params: list[bigquery.query.ScalarQueryParameter | bigquery.query.ArrayQueryParameter] = [
            bigquery.ScalarQueryParameter('process_id', 'STRING', str(process_id)),
        ]
        ids = [str(v).strip() for v in (google_place_ids or []) if str(v).strip()]
        if ids:
            sql += " AND google_place_id IN UNNEST(@google_place_ids)"
            params.append(bigquery.ArrayQueryParameter('google_place_ids', 'STRING', ids))
        job = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=self.location)
        result = job.result()
        return int(getattr(result, 'num_dml_affected_rows', 0) or 0)


    @staticmethod
    def build_segmented_row(*, google_place_id: str, segments: list[str], process_id: str) -> dict[str, object]:
        parts = (segments + ['', '', '', ''])[:4]
        return {
            'google_place_id': str(google_place_id),
            'market_segment_type0': parts[0],
            'market_segment_type1': parts[1],
            'market_segment_type2': parts[2],
            'market_segment_type3': parts[3],
            'created_at': BigQueryService._utc_rfc3339_now(),
            'process_id': str(process_id),
        }

    @staticmethod
    def _utc_rfc3339_now() -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    @staticmethod
    def _normalize_segmented_row(row: dict[str, object], created_at_type: str = 'TIMESTAMP') -> dict[str, object]:
        normalized = dict(row)
        created_at = normalized.get('created_at')
        target_type = (created_at_type or 'TIMESTAMP').upper()
        if isinstance(created_at, datetime):
            dt = created_at.astimezone(timezone.utc)
        elif created_at in (None, ''):
            dt = datetime.now(timezone.utc)
        else:
            raw_value = str(created_at).strip()
            if raw_value.endswith('Z'):
                raw_value = raw_value[:-1] + '+00:00'
            if 'T' in raw_value or '+' in raw_value or raw_value.endswith('00:00'):
                try:
                    dt = datetime.fromisoformat(raw_value)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                except ValueError:
                    dt = datetime.now(timezone.utc)
            else:
                try:
                    naive_dt = datetime.fromisoformat(raw_value.replace(' ', 'T'))
                    dt = naive_dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    dt = datetime.now(timezone.utc)
        if target_type == 'DATETIME':
            created_at = dt.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            created_at = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        normalized['created_at'] = created_at
        normalized['google_place_id'] = str(normalized.get('google_place_id') or '').strip()
        normalized['process_id'] = str(normalized.get('process_id') or '').strip()
        return normalized

    @staticmethod
    def _stringify(value: object) -> str:
        if value is None:
            return ''
        if isinstance(value, (list, dict)):
            import json
            return json.dumps(value, ensure_ascii=False)
        return str(value)

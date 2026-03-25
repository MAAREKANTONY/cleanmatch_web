import csv
import json
import os
import time
from pathlib import Path

from celery import shared_task
from django.core.files import File
from django.core.files.base import ContentFile
from django.conf import settings

from jobs.models import Job
from jobs.services import JobCancelledError, JobService
from normalizer.services.normalizer_service import NormalizerOptions, NormalizerService
from matcher.services.matcher_service import MatcherOptions, MatcherService
from geocoder.services.geocoder_service import GeocoderOptions, GeocoderService
from geoclass.services.geoclass_service import GeoclassOptions, GeoclassService
from marketsegmenter.services.marketsegmenter_service import (
    DEBUG_OUTPUT_COLUMNS,
    MarketSegmenterOptions,
    MarketSegmenterService,
    _csv_safe,
    _map_row_dict,
    _prepare_output_headers,
)
from ai_review.services.ai_review_service import AIReviewOptions, AIReviewService
from bigquery.client import BigQueryService


def _read_text_preview(path: str, limit: int = 4000) -> str:
    if not path or not os.path.exists(path):
        return 'Aperçu indisponible : fichier introuvable.'
    try:
        with open(path, 'rb') as fh:
            raw = fh.read(limit)
        return raw.decode('utf-8', errors='replace')
    except Exception as exc:
        return f'Aperçu indisponible : {exc}'


def _job_storage_root() -> str:
    return str(Path(settings.MEDIA_ROOT))


def _timed_log(log, label: str):
    start = time.monotonic()
    log(f'▶️ {label} — début')

    def done(extra: str = ''):
        elapsed = time.monotonic() - start
        suffix = f' | {extra}' if extra else ''
        log(f'✅ {label} — fin en {elapsed:.1f}s{suffix}')

    return done


def _parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    return bool(value)


@shared_task(bind=True)
def run_uploaded_job(self, job_id: str):
    job = Job.objects.get(id=job_id)
    current_task_id = str(getattr(getattr(self, 'request', None), 'id', '') or '')
    if current_task_id and job.celery_task_id != current_task_id:
        job.celery_task_id = current_task_id
        job.save(update_fields=['celery_task_id'])
    input_path = job.input_file_1.path if job.input_file_1 else ''
    second_input_path = job.input_file_2.path if job.input_file_2 else ''

    try:
        JobService.ensure_disk_space(_job_storage_root())
        JobService.mark_running(job, 'Initialisation du traitement')
        JobService.append_runtime_log(job, f"🚀 Task démarrée: type={job.job_type} celery_task_id={current_task_id or job.celery_task_id or '?'}")
        JobService.enforce_not_cancelled(job)
        if job.job_type == Job.JobType.NORMALIZER:
            return _run_normalizer_job(job)
        if job.job_type == Job.JobType.MATCHER:
            return _run_matcher_job(job)
        if job.job_type == Job.JobType.GEOCODER:
            return _run_geocoder_job(job)
        if job.job_type == Job.JobType.GEOCLASS:
            return _run_geoclass_job(job)
        if job.job_type == Job.JobType.MARKETSEGMENTER:
            return _run_marketsegmenter_job(job)
        if job.job_type == Job.JobType.AI_REVIEW:
            return _run_ai_review_job(job)
        return _run_stub_job(job, input_path, second_input_path)
    except JobCancelledError as exc:
        job.refresh_from_db()
        JobService.mark_cancelled(job, str(exc))
        return str(job.id)
    except Exception as exc:
        job.refresh_from_db()
        JobService.mark_failed(job, str(exc))
        raise


@shared_task
def monitor_stale_jobs():
    return JobService.fail_stale_jobs()


def _run_normalizer_job(job: Job):
    parameters = job.parameters_json or {}
    input_path = Path(job.input_file_1.path)
    output_name = _build_normalizer_output_name(input_path, parameters)
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.ensure_disk_space(_job_storage_root())
        JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.append_runtime_log(job, message)

    service = NormalizerService(progress_callback=progress, log_callback=log)
    options = NormalizerOptions(
        do_clean=bool(parameters.get('do_clean', True)),
        do_matchcode=bool(parameters.get('do_matchcode', True)),
        sheet_name=(parameters.get('sheet_name') or '').strip() or None,
        column_mapping=parameters.get('column_mapping') or {},
        country_code=(parameters.get('country_code') or 'FR'),
    )

    log('🚀 Lancement du normalizer web V16 Matcher Multi-country')
    log(f'📂 Fichier source : {input_path.name}')
    log('💾 Format de sortie : CSV UTF-8 (compatible gros volumes)')
    result_path = service.run(input_path=input_path, output_path=output_path, options=options)

    job.refresh_from_db()
    JobService.enforce_not_cancelled(job)
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='Normalizer terminé avec succès')
    return str(job.id)


def _build_normalizer_output_name(input_path: Path, parameters: dict) -> str:
    stem = input_path.stem
    for suffix in ['_enriched', '_cleaned', '_matchcoded', '_normalized']:
        if stem.endswith(suffix):
            stem = stem[:-len(suffix)]
            break
    do_clean = bool(parameters.get('do_clean', True))
    do_matchcode = bool(parameters.get('do_matchcode', True))
    if do_clean and do_matchcode:
        suffix = '_normalized.csv'
    elif do_clean:
        suffix = '_cleaned.csv'
    else:
        suffix = '_matchcoded.csv'
    return f'{stem}{suffix}'



def _run_matcher_job(job: Job):
    parameters = job.parameters_json or {}
    master_path = Path(job.input_file_1.path)
    slave_path = Path(job.input_file_2.path)
    output_name = f"{master_path.stem}__vs__{slave_path.stem}_matcher_v2.zip"
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.ensure_disk_space(_job_storage_root())
        JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.append_runtime_log(job, message)

    service = MatcherService(progress_callback=progress, log_callback=log)
    options = MatcherOptions(
        threshold_name=int(parameters.get('threshold_name') or 85),
        threshold_voie=int(parameters.get('threshold_voie') or 70),
        top_k_per_master=int(parameters.get('top_k_per_master') or 5),
        master_sheet_name=parameters.get('master_sheet_name') or None,
        slave_sheet_name=parameters.get('slave_sheet_name') or None,
        master_mapping=parameters.get('master_mapping') or {},
        slave_mapping=parameters.get('slave_mapping') or {},
    )

    log('🚀 Lancement du matcher web V4 Multi-country')
    log(f'📂 Master : {master_path.name}')
    log(f'📂 Slave : {slave_path.name}')
    log('💾 Format de sortie : ZIP contenant all_matches.csv, automatch.csv, review.csv, unmatched.csv, diagnostics.csv et summary.json')
    result_path = service.run(master_path=master_path, slave_path=slave_path, output_path=output_path, options=options)

    job.refresh_from_db()
    JobService.enforce_not_cancelled(job)
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='Matcher V4 terminé avec succès')
    return str(job.id)



def _run_geocoder_job(job: Job):
    parameters = job.parameters_json or {}
    input_path = Path(job.input_file_1.path)
    output_name = f"{input_path.stem}_geocoded.csv"
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.ensure_disk_space(_job_storage_root())
        JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.append_runtime_log(job, message)

    service = GeocoderService(progress_callback=progress, log_callback=log)
    options = GeocoderOptions(
        provider=(parameters.get('geocoder_provider') or 'existing_or_nominatim'),
        geocoder_sheet_name=parameters.get('geocoder_sheet_name') or None,
        geocoder_mapping=parameters.get('geocoder_mapping') or {},
        country_hint=parameters.get('country_hint') or '',
        cache_db_path=Path(settings.MEDIA_ROOT) / 'cache' / 'geocode_cache_web.sqlite3',
    )

    log('🚀 Lancement du geocoder web V2 checkpoint')
    log(f'📂 Fichier source : {input_path.name}')
    log(f"🧭 Provider : {options.provider}")
    log('💾 Format de sortie : CSV UTF-8 avec lat/lng, geocoder_status, geocoder_source et geocoder_label')
    result_path = service.run(input_path=input_path, output_path=output_path, options=options)

    job.refresh_from_db()
    JobService.enforce_not_cancelled(job)
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='Geocoder V2 checkpoint terminé avec succès')
    return str(job.id)


def _run_geoclass_job(job: Job):
    parameters = job.parameters_json or {}
    input_path = Path(job.input_file_1.path)
    output_name = f"{input_path.stem}_geoclass.csv"
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.ensure_disk_space(_job_storage_root())
        JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.append_runtime_log(job, message)

    service = GeoclassService(progress_callback=progress, log_callback=log)
    options = GeoclassOptions(
        geoclass_sheet_name=parameters.get('geoclass_sheet_name') or None,
        geoclass_mapping=parameters.get('geoclass_mapping') or {},
    )

    log('🚀 Lancement du geoclass web V1 heuristique')
    log(f'📂 Fichier source : {input_path.name}')
    log('💾 Format de sortie : CSV UTF-8 avec geoclass_code, geoclass_category, geoclass_subcategory et score')
    result_path = service.run(input_path=input_path, output_path=output_path, options=options)

    job.refresh_from_db()
    JobService.enforce_not_cancelled(job)
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='Geoclass V1 terminé avec succès')
    return str(job.id)


def _run_marketsegmenter_job(job: Job):
    parameters = job.parameters_json or {}
    source_mode = str(parameters.get('marketsegmenter_source_mode') or parameters.get('mode') or 'uploaded')

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db(); JobService.enforce_not_cancelled(job); JobService.ensure_disk_space(_job_storage_root()); JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db(); JobService.enforce_not_cancelled(job); JobService.append_runtime_log(job, message)

    if source_mode == 'bigquery':
        return _run_marketsegmenter_bigquery_job(job, parameters, progress, log)

    input_path = Path(job.input_file_1.path)
    output_name = f"{input_path.stem}_market_segmented.csv"
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))
    service = MarketSegmenterService(progress_callback=progress, log_callback=log)
    options = MarketSegmenterOptions(
        marketsegmenter_sheet_name=parameters.get('marketsegmenter_sheet_name') or None,
        marketsegmenter_mapping=parameters.get('marketsegmenter_mapping') or {},
        country_default=parameters.get('marketsegmenter_country_default') or '',
    )
    result_path = service.run(input_path=input_path, output_path=output_path, options=options)
    job.refresh_from_db(); JobService.enforce_not_cancelled(job)
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='Market segmenter FYRE terminé avec succès')
    return str(job.id)


def _run_marketsegmenter_bigquery_job(job: Job, parameters: dict, progress, log):
    bq = BigQueryService()
    table_name = str(parameters.get('marketsegmenter_bq_table_name') or settings.BIGQUERY_INPUT_TABLE)
    output_table_name = str(parameters.get('marketsegmenter_bq_output_table_name') or settings.BIGQUERY_OUTPUT_TABLE)
    country_code = str(parameters.get('marketsegmenter_bq_country_code') or '').strip()
    low_conf_threshold = float(parameters.get('ai_review_low_confidence_threshold') or 0.65)
    job_root = Path(job.output_file.field.storage.path(f'outputs/{job.id}'))
    job_root.mkdir(parents=True, exist_ok=True)
    source_csv = job_root / f'{table_name}_source.csv'
    first_csv = job_root / f'{table_name}_marketsegmenter.csv'
    ai_csv = job_root / f'{table_name}_ai_review.csv'
    final_csv = job_root / f'{table_name}_segmented_simple.csv'

    progress(3, 'Préflight BigQuery source + sortie')
    log(f'🧾 Source BigQuery: {table_name} | filtre country_code={country_code or "ALL"}')
    done_preflight = _timed_log(log, 'Préflight BigQuery')
    source_access = bq.ensure_source_readable(table_name=table_name, country_code=country_code)
    log(f"✅ Accès lecture BigQuery validé sur {source_access['table']}")
    output_access = bq.ensure_output_writable(output_table_name)
    if output_access.get('created'):
        log(f"✅ Table BigQuery de sortie créée avant traitement : {output_access['table']}")
    else:
        log(f"✅ Accès écriture BigQuery validé sur {output_access['table']}")
    done_preflight(f"source={source_access['table']} output={output_access['table']}")

    done_estimate = _timed_log(log, 'Estimation volume BigQuery')
    estimated_rows = bq.estimate_row_count(table_name=table_name, country_code=country_code)
    if estimated_rows is not None:
        log(f'📊 BigQuery estime {estimated_rows} ligne(s) à exporter avant segmentation')
        done_estimate(f'rows={estimated_rows}')
    else:
        done_estimate('rows=unknown')
    progress(5, 'Segmentation BigQuery directe : initialisation')
    ms_service = MarketSegmenterService(progress_callback=progress, log_callback=log)
    ms_options = MarketSegmenterOptions(
        marketsegmenter_sheet_name=None,
        marketsegmenter_mapping=parameters.get('marketsegmenter_mapping') or {},
        country_default=parameters.get('marketsegmenter_country_default') or country_code,
    )
    done_stream = _timed_log(log, 'Streaming BigQuery → rules')
    source_headers, row_count = _stream_bigquery_to_marketsegmenter_csv(
        job=job,
        bq=bq,
        table_name=table_name,
        country_code=country_code,
        estimated_rows=estimated_rows,
        output_path=first_csv,
        service=ms_service,
        options=ms_options,
        progress=progress,
        log=log,
    )
    done_stream(f'rows={row_count} output={first_csv.name}')
    log(f'📥 {row_count} ligne(s) BigQuery segmentées directement dans {first_csv.name}')

    progress(55, 'AI Review ciblée sur lignes à faible confiance')
    log(f'🧠 AI Review démarrage sur {first_csv.name} ({first_csv.stat().st_size if first_csv.exists() else 0} octets)')

    progress(55, 'AI Review ciblée sur lignes à faible confiance')
    ai_service = AIReviewService(progress_callback=progress, log_callback=log)
    ai_review_mapping = dict(parameters.get('ai_review_mapping') or {})
    ai_review_mapping.pop('segmentation_confidence', None)
    ai_options = AIReviewOptions(
        ai_review_sheet_name=None,
        ai_review_mapping=ai_review_mapping,
        low_confidence_threshold=low_conf_threshold,
        only_low_confidence=True,
        action_profile=(parameters.get('ai_review_action_profile') or 'standard'),
        llm_enabled=_parse_bool(parameters.get('ai_review_llm_enabled', False)),
        llm_provider=str(parameters.get('ai_review_llm_provider') or ''),
        llm_model=str(parameters.get('ai_review_llm_model') or ''),
        llm_max_budget_eur=float(parameters.get('ai_review_llm_max_budget_eur') or 0.0),
        llm_max_cost_per_row_eur=float(parameters.get('ai_review_llm_max_cost_per_row_eur') or 0.0),
        llm_max_calls_per_row=int(parameters.get('ai_review_llm_max_calls_per_row') or 1),
    )
    done_ai = _timed_log(log, 'AI Review')
    ai_service.run(input_path=first_csv, output_path=ai_csv, options=ai_options)
    done_ai(f'output={ai_csv.name} size={ai_csv.stat().st_size if ai_csv.exists() else 0}')

    progress(82, 'Consolidation résultat simple + écriture BigQuery')
    created_at_mode = bq._get_output_created_at_mode(output_table_name)
    insert_batch_size = max(1, int(getattr(settings, 'BIGQUERY_INSERT_BATCH_SIZE', 1000) or 1000))
    progress_log_every = max(1, int(getattr(settings, 'BIGQUERY_PROGRESS_LOG_EVERY', 5000) or 5000))
    done_consolidate = _timed_log(log, 'Consolidation + écriture BigQuery')
    stats = _consolidate_marketsegmenter_ai_results_streaming(
        ai_csv_path=ai_csv,
        final_csv_path=final_csv,
        process_id=str(job.id),
        low_conf_threshold=low_conf_threshold,
        progress=progress,
        log=log,
        bigquery_batch_callback=lambda rows: bq.write_segmented_rows(output_table_name, rows),
        created_at_mode=created_at_mode,
        insert_batch_size=insert_batch_size,
        progress_log_every=progress_log_every,
    )

    summary = {
        'job_id': str(job.id),
        'source_mode': 'bigquery',
        'source_table': table_name,
        'source_country_code': country_code,
        'output_table': output_table_name,
        'source_rows': row_count,
        'result_rows': int(stats['result_rows']),
        'output_csv': final_csv.name,
        'low_confidence_threshold': low_conf_threshold,
        'bigquery_created_at_mode': created_at_mode,
        'bigquery_insert_batch_size': insert_batch_size,
    }
    done_consolidate(f"result_rows={stats.get('result_rows', 0)} inserted={stats.get('bigquery_rows_inserted', 0)}")
    bq_error = stats.get('bigquery_write_error')
    if bq_error:
        log(f'⚠️ Persistance BigQuery échouée après calcul : {bq_error}')
        summary['bigquery_write_status'] = 'failed'
        summary['bigquery_error'] = str(bq_error)
        summary['bigquery_rows_inserted'] = int(stats.get('bigquery_rows_inserted', 0) or 0)
        success_message = None
        final_error = (
            'Segmentation terminée, fichier résultat disponible, mais écriture BigQuery impossible : '
            f'{bq_error}'
        )
    else:
        inserted = int(stats['bigquery_rows_inserted'])
        log(f'📤 {inserted} ligne(s) écrites dans BigQuery table {output_table_name} par batch de {insert_batch_size}')
        summary['bigquery_write_status'] = 'success'
        summary['bigquery_rows_inserted'] = inserted
        success_message = 'Market segmenter BigQuery + AI terminé avec succès'
        final_error = None

    final_csv.with_name(final_csv.stem + '_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    job.refresh_from_db(); JobService.enforce_not_cancelled(job)
    with final_csv.open('rb') as fh:
        job.output_file.save(final_csv.name, File(fh), save=False)
    if final_error:
        JobService.mark_failed(job, error_message=final_error)
    else:
        JobService.mark_success(job, message=success_message)
    return str(job.id)



def _stream_bigquery_to_marketsegmenter_csv(
    *,
    job: Job,
    bq: BigQueryService,
    table_name: str,
    country_code: str,
    estimated_rows: int | None,
    output_path: Path,
    service: MarketSegmenterService,
    options: MarketSegmenterOptions,
    progress,
    log,
) -> tuple[list[str], int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    progress_log_every = max(1, int(getattr(settings, 'BIGQUERY_PROGRESS_LOG_EVERY', 5000) or 5000))
    debug_cols = DEBUG_OUTPUT_COLUMNS if options.emit_debug_columns else []
    processed = 0
    source_headers: list[str] | None = None
    projected_headers: list[str] | None = None
    writer = None

    log(
        '🚰 Streaming BigQuery → MarketSegmenter direct activé '         f'(page_size={max(1, int(getattr(settings, "BIGQUERY_EXPORT_PAGE_SIZE", 1000) or 1000))})'
    )
    with output_path.open('w', newline='', encoding='utf-8-sig') as fh:
        log(f'📝 Fichier marketsegmenter cible: {output_path}')
        for raw_row in bq.iter_rows_streaming(
            table_name=table_name,
            country_code=country_code,
            limit=None,
            page_size=max(1, int(getattr(settings, 'BIGQUERY_EXPORT_PAGE_SIZE', 1000) or 1000)),
            progress_callback=None,
            log_callback=lambda message: log(f'BQ: {message}'),
        ):
            job.refresh_from_db()
            JobService.enforce_not_cancelled(job)
            JobService.ensure_disk_space(_job_storage_root())
            if source_headers is None:
                log('🔍 Première ligne BigQuery reçue, démarrage mapping/segmentation locale')
                source_headers = list(raw_row.keys())
                projected_headers = _prepare_output_headers(source_headers, options.marketsegmenter_mapping)
                output_columns = list(projected_headers) + [
                    'fyre_market_segment_type0', 'fyre_market_segment_type1',
                    'fyre_market_segment_type2', 'fyre_market_segment_type3',
                    'segmentation_confidence', 'segmentation_reasons',
                    'base_main_type_path', 'all_types_paths_considered',
                    'keyword_hits', 'language_scope',
                ] + debug_cols
                writer = csv.DictWriter(
                    fh,
                    fieldnames=output_columns,
                    delimiter=';',
                    quotechar='"',
                    quoting=csv.QUOTE_ALL,
                    lineterminator='\n',
                )
                writer.writeheader()
                log(f'🧱 Entêtes BigQuery détectés: {len(source_headers)} colonnes')

            mapped_row = _map_row_dict(raw_row, source_headers or [], options.marketsegmenter_mapping, projected_headers or [])
            classified = service._classify_row(mapped_row, options)
            out = {col: _csv_safe(mapped_row.get(col, '')) for col in (projected_headers or [])}
            out.update({k: _csv_safe(v) for k, v in classified.items()})
            writer.writerow(out)
            processed += 1

            if processed == 1 or processed % progress_log_every == 0:
                if estimated_rows:
                    pct = min(54, 5 + int((processed / max(1, estimated_rows)) * 45))
                    progress(pct, f'Segmentation BigQuery directe : {processed}/{estimated_rows}')
                else:
                    progress(25, f'Segmentation BigQuery directe : {processed}')
                JobService.heartbeat(job, f'BigQuery->rules streaming {processed}/{estimated_rows or "?"}')
                log(f'🧮 Segmentation directe en cours : {processed} ligne(s) traitées')

    if source_headers is None:
        source_headers = []
        projected_headers = _prepare_output_headers(source_headers, options.marketsegmenter_mapping)
        output_columns = list(projected_headers) + [
            'fyre_market_segment_type0', 'fyre_market_segment_type1',
            'fyre_market_segment_type2', 'fyre_market_segment_type3',
            'segmentation_confidence', 'segmentation_reasons',
            'base_main_type_path', 'all_types_paths_considered',
            'keyword_hits', 'language_scope',
        ] + debug_cols
        log(f'📝 Fichier marketsegmenter cible: {output_path}')
        with output_path.open('w', newline='', encoding='utf-8-sig') as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=output_columns,
                delimiter=';',
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                lineterminator='\n',
            )
            writer.writeheader()
    progress(54, f'Segmentation BigQuery directe terminée : {processed} ligne(s)')
    log(f'✅ Streaming BigQuery → MarketSegmenter terminé : {processed} ligne(s)')
    return source_headers, int(processed)

def _split_segment_path(raw_value: str) -> list[str]:
    text = (raw_value or '').strip()
    if not text:
        return []
    return [part.strip() for part in text.split('>') if part.strip()]


def _consolidate_marketsegmenter_ai_results_streaming(
    ai_csv_path: Path,
    final_csv_path: Path,
    process_id: str,
    low_conf_threshold: float,
    progress,
    log,
    bigquery_batch_callback,
    created_at_mode: str = 'DATETIME',
    insert_batch_size: int = 1000,
    progress_log_every: int = 5000,
):
    final_csv_path.parent.mkdir(parents=True, exist_ok=True)
    result_rows = 0
    bigquery_rows_inserted = 0
    bigquery_write_error = None
    pending_batch: list[dict[str, object]] = []

    def flush_batch() -> None:
        nonlocal pending_batch, bigquery_rows_inserted, bigquery_write_error
        if not pending_batch or bigquery_write_error:
            pending_batch = []
            return
        try:
            bigquery_rows_inserted += int(bigquery_batch_callback(list(pending_batch)) or 0)
        except Exception as exc:
            bigquery_write_error = str(exc)
            log(f'⚠️ Erreur écriture BigQuery batch après {bigquery_rows_inserted} ligne(s) : {exc}')
        finally:
            pending_batch = []

    with ai_csv_path.open('r', encoding='utf-8-sig', newline='') as in_fh, final_csv_path.open('w', encoding='utf-8-sig', newline='') as out_fh:
        reader = csv.DictReader(in_fh, delimiter=';')
        writer = csv.DictWriter(
            out_fh,
            fieldnames=['google_place_id', 'market_segment_type0', 'market_segment_type1', 'market_segment_type2', 'market_segment_type3'],
            delimiter=';',
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            lineterminator='\n',
        )
        writer.writeheader()
        for idx, row in enumerate(reader, start=1):
            google_place_id = (row.get('google_place_id') or row.get('place_id') or row.get('google_id') or '').strip()
            ai_source = (row.get('ai_segment_source') or '').strip()
            ai_selected = (row.get('ai_selected_for_review') or '').strip().lower() == 'yes'
            ai_segments = _split_segment_path(row.get('ai_segment_suggested', ''))
            rules_segments = [
                (row.get('fyre_market_segment_type0') or '').strip(),
                (row.get('fyre_market_segment_type1') or '').strip(),
                (row.get('fyre_market_segment_type2') or '').strip(),
                (row.get('fyre_market_segment_type3') or '').strip(),
            ]
            try:
                rules_conf = float(str(row.get('segmentation_confidence') or '').replace(',', '.'))
            except Exception:
                rules_conf = 0.0

            rules_has_segments = any(rules_segments)
            if ai_selected and ai_segments and ai_source.startswith('llm_'):
                final_segments = (ai_segments + ['', '', '', ''])[:4]
            elif not ai_selected and rules_conf >= low_conf_threshold and rules_has_segments:
                final_segments = rules_segments
            elif ai_segments and ai_source != 'rules_initial':
                final_segments = (ai_segments + ['', '', '', ''])[:4]
            elif rules_has_segments:
                final_segments = rules_segments
            else:
                final_segments = ['', '', '', '']

            writer.writerow({
                'google_place_id': google_place_id,
                'market_segment_type0': final_segments[0],
                'market_segment_type1': final_segments[1],
                'market_segment_type2': final_segments[2],
                'market_segment_type3': final_segments[3],
            })
            pending_batch.append(BigQueryService.build_segmented_row(
                google_place_id=google_place_id,
                segments=final_segments,
                process_id=process_id,
                created_at_mode=created_at_mode,
            ))
            result_rows += 1
            if len(pending_batch) >= max(1, int(insert_batch_size)):
                flush_batch()
            if idx == 1 or idx % max(1, int(progress_log_every)) == 0:
                progress(82, f'Consolidation des résultats : {idx} ligne(s)')
                log(f'🧩 Consolidation streaming: {idx} ligne(s) | BQ insérées={bigquery_rows_inserted} | erreur_bq={"yes" if bigquery_write_error else "no"}')

    flush_batch()
    log(f'🧩 Consolidation finale terminée : {result_rows} ligne(s), seuil rules={low_conf_threshold}, BQ insérées={bigquery_rows_inserted}')
    return {
        'result_rows': int(result_rows),
        'bigquery_rows_inserted': int(bigquery_rows_inserted),
        'bigquery_write_error': bigquery_write_error,
    }


def _run_ai_review_job(job: Job):
    parameters = job.parameters_json or {}
    input_path = Path(job.input_file_1.path)
    output_name = f"{input_path.stem}_ai_review.csv"
    output_path = Path(job.output_file.field.storage.path(f'outputs/{output_name}'))

    def progress(percent: int, message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.ensure_disk_space(_job_storage_root())
        JobService.update_progress(job, percent, message)

    def log(message: str) -> None:
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.append_runtime_log(job, message)

    service = AIReviewService(progress_callback=progress, log_callback=log)
    options = AIReviewOptions(
        ai_review_sheet_name=parameters.get('ai_review_sheet_name') or None,
        ai_review_mapping=parameters.get('ai_review_mapping') or {},
        low_confidence_threshold=float(parameters.get('ai_review_low_confidence_threshold') or 0.65),
        only_low_confidence=True,
        action_profile=(parameters.get('ai_review_action_profile') or 'standard'),
        llm_enabled=_parse_bool(parameters.get('ai_review_llm_enabled', False)),
        llm_provider=str(parameters.get('ai_review_llm_provider') or ''),
        llm_model=str(parameters.get('ai_review_llm_model') or ''),
        llm_max_budget_eur=float(parameters.get('ai_review_llm_max_budget_eur') or 0.0),
        llm_max_cost_per_row_eur=float(parameters.get('ai_review_llm_max_cost_per_row_eur') or 0.0),
        llm_max_calls_per_row=int(parameters.get('ai_review_llm_max_calls_per_row') or 1),
    )

    log('🚀 Lancement du process AI Review hardened multi-provider LLM')
    log(f'📂 Fichier source : {input_path.name}')
    log(f"🎯 Seuil faible confiance : {options.low_confidence_threshold}")
    log(f"🧠 Profil d’action : {options.action_profile}")
    log(f"🤖 LLM enabled={options.llm_enabled} provider={options.llm_provider} model={options.llm_model} budget={options.llm_max_budget_eur}€ row_max={options.llm_max_cost_per_row_eur}€ calls/row={options.llm_max_calls_per_row}")
    log('💾 Format de sortie : CSV UTF-8 avec colonnes AI review additives + summary JSON + guardrails LLM + JSON provider output')
    result_path = service.run(input_path=input_path, output_path=output_path, options=options)

    job.refresh_from_db()
    JobService.enforce_not_cancelled(job)
    with result_path.open('rb') as fh:
        job.output_file.save(result_path.name, File(fh), save=False)
    JobService.mark_success(job, message='AI Review hardening terminé avec succès')
    return str(job.id)

def _run_stub_job(job: Job, input_path: str, second_input_path: str):
    JobService.update_progress(job, 5, 'Vérification des fichiers uploadés')
    time.sleep(1)

    if input_path and not os.path.exists(input_path):
        raise FileNotFoundError(f'Fichier principal introuvable : {input_path}')

    primary_size = os.path.getsize(input_path) if input_path and os.path.exists(input_path) else 0
    secondary_size = os.path.getsize(second_input_path) if second_input_path and os.path.exists(second_input_path) else 0
    JobService.append_runtime_log(
        job,
        f"Fichier principal : {Path(input_path).name if input_path else '-'} ({primary_size} bytes)",
    )
    if second_input_path:
        JobService.append_runtime_log(
            job,
            f"Fichier secondaire : {Path(second_input_path).name} ({secondary_size} bytes)",
        )

    for percent, message in [
        (20, 'Lecture des métadonnées du fichier'),
        (40, 'Simulation du pré-traitement'),
        (60, 'Simulation du traitement asynchrone'),
        (80, 'Génération du livrable de sortie'),
    ]:
        time.sleep(1)
        job.refresh_from_db()
        JobService.enforce_not_cancelled(job)
        JobService.ensure_disk_space(_job_storage_root())
        JobService.update_progress(job, percent, message)

    preview = _read_text_preview(input_path)
    output_lines = [
        'CleanMatch Web - Iteration 5',
        f'Job ID: {job.id}',
        f'Type: {job.job_type}',
        f'Fichier principal: {Path(input_path).name if input_path else "-"}',
        f'Taille fichier principal: {primary_size} bytes',
        f'Fichier secondaire: {Path(second_input_path).name if second_input_path else "-"}',
        f'Taille fichier secondaire: {secondary_size} bytes',
        '',
        'Aperçu du fichier principal (premiers octets décodés en UTF-8 avec remplacement) :',
        preview,
        '',
        'Normalizer et Matcher branchés. Geocoder V2 checkpoint est désormais disponible.',
    ]
    output_name = f'result_{job.id}.txt'
    output_content = '\n'.join(output_lines)

    job.refresh_from_db()
    JobService.enforce_not_cancelled(job)
    job.output_file.save(output_name, ContentFile(output_content.encode('utf-8')), save=False)
    JobService.mark_success(job, message='Job terminé avec succès')
    return str(job.id)

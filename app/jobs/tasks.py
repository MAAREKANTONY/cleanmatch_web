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
from jobs.services import JobCancelledError, JobService, JobTracker
from normalizer.services.normalizer_service import NormalizerOptions, NormalizerService
from matcher.services.matcher_service import MatcherOptions, MatcherService
from geocoder.services.geocoder_service import GeocoderOptions, GeocoderService
from geoclass.services.geoclass_service import GeoclassOptions, GeoclassService
from marketsegmenter.services.marketsegmenter_service import MarketSegmenterOptions, MarketSegmenterService
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
    input_path = job.input_file_1.path if job.input_file_1 else ''
    second_input_path = job.input_file_2.path if job.input_file_2 else ''
    try:
        JobService.ensure_disk_space(_job_storage_root())
        JobService.mark_running(job, 'Initialisation du traitement')
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
    tracker = JobTracker(job)
    bq = BigQueryService()
    table_name = str(parameters.get('marketsegmenter_bq_table_name') or settings.BIGQUERY_INPUT_TABLE)
    output_table_name = str(parameters.get('marketsegmenter_bq_output_table_name') or settings.BIGQUERY_OUTPUT_TABLE)
    country_code = str(parameters.get('marketsegmenter_bq_country_code') or '').strip()
    low_conf_threshold = float(parameters.get('ai_review_low_confidence_threshold') or settings.AI_REVIEW_LOW_CONFIDENCE_THRESHOLD)
    min_conf_threshold = float(parameters.get('ai_review_min_confidence_threshold') or settings.AI_REVIEW_MIN_CONFIDENCE_THRESHOLD)
    read_page_size = int(parameters.get('marketsegmenter_bq_read_page_size') or getattr(settings, 'BIGQUERY_READ_PAGE_SIZE', 1000) or 1000)
    write_batch_size = int(parameters.get('marketsegmenter_bq_write_batch_size') or getattr(settings, 'BIGQUERY_WRITE_BATCH_SIZE', 1000) or 1000)
    cleanup_intermediate = _parse_bool(parameters.get('marketsegmenter_bq_cleanup_intermediate_files', getattr(settings, 'BIGQUERY_CLEANUP_INTERMEDIATE_FILES', True)))
    cleanup_workdir = _parse_bool(parameters.get('marketsegmenter_bq_cleanup_workdir_files', getattr(settings, 'BIGQUERY_CLEANUP_WORKDIR_FILES', True)))
    source_column_pruning = _parse_bool(parameters.get('marketsegmenter_bq_source_column_pruning', getattr(settings, 'BIGQUERY_SOURCE_COLUMN_PRUNING', True)))
    selected_columns = _build_bigquery_selected_columns(parameters) if source_column_pruning else []
    job_root = Path(job.output_file.field.storage.path(f'outputs/{job.id}'))
    job_root.mkdir(parents=True, exist_ok=True)
    safe_prefix = table_name.replace('.', '_')
    source_csv = job_root / f'{safe_prefix}_source.csv'
    first_csv = job_root / f'{safe_prefix}_marketsegmenter.csv'
    ai_csv = job_root / f'{safe_prefix}_ai_review.csv'
    final_csv = job_root / f'{safe_prefix}_segmented_simple.csv'
    tracker.step('INIT', 'Initialisation du job BigQuery')
    tracker.set_metric('source_column_pruning_enabled', 1 if source_column_pruning else 0)
    tracker.set_metric('source_selected_columns_count', len(selected_columns))
    tracker.log(f"[JOB] Step=INIT | source={table_name} | output={output_table_name} | country_code={country_code or 'ALL'} | read_page_size={read_page_size} | write_batch_size={write_batch_size} | cleanup_intermediate={cleanup_intermediate} | cleanup_workdir={cleanup_workdir} | source_column_pruning={source_column_pruning} | selected_columns={len(selected_columns) or 'ALL'}")
    if selected_columns:
        log(f"🧾 BigQuery column pruning actif: {len(selected_columns)} colonne(s) source sélectionnée(s)")
    progress(5, 'Lecture BigQuery source')
    tracker.step('BQ_READ', 'Lecture BigQuery source')
    export_started = time.monotonic()
    export_log_every = max(1, int(getattr(settings, 'PIPELINE_EXPORT_PROGRESS_LOG_EVERY_ROWS', 10000) or 10000))
    log(f'🧾 Source BigQuery: {table_name} | filtre country_code={country_code or "ALL"} | page_size={read_page_size} | progress_every={export_log_every}')
    def _on_export_progress(exported_rows: int) -> None:
        elapsed = max(time.monotonic() - export_started, 0.001)
        rate = exported_rows / elapsed * 60.0
        tracker.set_metric('bq_read_rows_so_far', exported_rows)
        tracker.set_metric('bq_read_rate_rows_per_min', round(rate, 2))
        tracker.log(f'[JOB] Step=BQ_READ | exported={exported_rows} | rate={rate:.1f} rows/min')
    source_path, source_headers, row_count = bq.export_table_to_csv(
        table_name=table_name,
        output_path=source_csv,
        country_code=country_code,
        page_size=read_page_size,
        selected_columns=selected_columns,
        progress_every_rows=export_log_every,
        progress_callback=_on_export_progress,
    )
    export_elapsed = max(time.monotonic() - export_started, 0.001)
    export_rate = row_count / export_elapsed * 60.0 if row_count else 0.0
    tracker.set_metric('total_rows', row_count)
    tracker.set_metric('source_exported_columns_count', len(source_headers))
    tracker.set_metric('bq_read_elapsed_ms', int(export_elapsed * 1000))
    tracker.set_metric('bq_read_rate_rows_per_min', round(export_rate, 2))
    tracker.log(f'[JOB] Step=BQ_READ | rows={row_count} | columns={len(source_headers)} | elapsed_ms={int(export_elapsed * 1000)} | rate={export_rate:.1f} rows/min | file={source_path.name}')
    log(f'📥 {row_count} ligne(s) exportées depuis BigQuery dans {source_path.name} en {export_elapsed:.1f}s ({export_rate:.1f} rows/min)')
    progress(20, 'Segmentation règles / keywords')
    tracker.step('RULES', 'Segmentation règles / keywords')
    ms_service = MarketSegmenterService(progress_callback=progress, log_callback=log)
    ms_options = MarketSegmenterOptions(
        marketsegmenter_sheet_name=None,
        marketsegmenter_mapping=parameters.get('marketsegmenter_mapping') or {},
        country_default=parameters.get('marketsegmenter_country_default') or country_code,
    )
    ms_service.run(input_path=source_path, output_path=first_csv, options=ms_options)
    if cleanup_intermediate:
        _cleanup_temp_file(source_csv, log, tracker)
    rules_metrics = _compute_rules_gating_metrics(first_csv, low_conf_threshold, min_conf_threshold)
    for key, value in rules_metrics.items():
        tracker.set_metric(key, value)
    tracker.log('[JOB] Step=RULES | total={total} | high={high} | ai={mid} | out={low}'.format(
        total=rules_metrics.get('total_rows', 0),
        high=rules_metrics.get('rules_high_confidence', 0),
        mid=rules_metrics.get('rules_low_confidence_ai', 0),
        low=rules_metrics.get('rules_very_low_out_of_scope', 0),
    ))
    progress(55, 'AI Review ciblée sur lignes à faible confiance')
    tracker.step('AI_REVIEW', 'AI Review ciblée sur lignes à faible confiance')
    ai_service = AIReviewService(progress_callback=progress, log_callback=log)
    ai_review_mapping = dict(parameters.get('ai_review_mapping') or {})
    ai_review_mapping.pop('segmentation_confidence', None)
    ai_options = AIReviewOptions(
        ai_review_sheet_name=None,
        ai_review_mapping=ai_review_mapping,
        low_confidence_threshold=low_conf_threshold,
        min_confidence_threshold=min_conf_threshold,
        only_low_confidence=True,
        action_profile=(parameters.get('ai_review_action_profile') or 'standard'),
        llm_enabled=_parse_bool(parameters.get('ai_review_llm_enabled', False)),
        llm_provider=str(parameters.get('ai_review_llm_provider') or ''),
        llm_model=str(parameters.get('ai_review_llm_model') or ''),
        llm_max_budget_eur=float(parameters.get('ai_review_llm_max_budget_eur') or 0.0),
        llm_max_cost_per_row_eur=float(parameters.get('ai_review_llm_max_cost_per_row_eur') or 0.0),
        llm_max_calls_per_row=int(parameters.get('ai_review_llm_max_calls_per_row') or 1),
    )
    ai_service.run(input_path=first_csv, output_path=ai_csv, options=ai_options)
    if cleanup_intermediate:
        _cleanup_temp_file(first_csv, log, tracker)
    ai_metrics = _compute_ai_review_metrics(ai_csv, low_conf_threshold, min_conf_threshold)
    ai_summary_path = ai_csv.with_name(ai_csv.stem + '_summary.json')
    ai_summary = _read_json_if_exists(ai_summary_path)
    if ai_summary:
        ai_metrics['ai_candidate_rows'] = int(ai_summary.get('ai_candidate_rows', ai_metrics.get('ai_selected_yes', 0)) or 0)
        ai_metrics['ai_hardened_rows'] = int(ai_summary.get('ai_hardened_rows', ai_metrics.get('ai_selected_yes', 0)) or 0)
        ai_metrics['ai_hardening_elapsed_ms'] = int(ai_summary.get('ai_hardening_elapsed_ms', 0) or 0)
    else:
        ai_metrics['ai_candidate_rows'] = int(ai_metrics.get('ai_selected_yes', 0) or 0)
        ai_metrics['ai_hardened_rows'] = int(ai_metrics.get('ai_selected_yes', 0) or 0)
        ai_metrics['ai_hardening_elapsed_ms'] = 0
    for key, value in ai_metrics.items():
        tracker.set_metric(key, value)
    tracker.log('[JOB] Step=AI_REVIEW | selected={selected} | hardened={hardened} | hardening_ms={hardening_ms} | llm_calls={llm_calls} | llm_success={llm_success} | llm_failed={llm_failed}'.format(
        selected=ai_metrics.get('ai_candidate_rows', 0),
        hardened=ai_metrics.get('ai_hardened_rows', 0),
        hardening_ms=ai_metrics.get('ai_hardening_elapsed_ms', 0),
        llm_calls=ai_metrics.get('llm_calls', 0),
        llm_success=ai_metrics.get('llm_success', 0),
        llm_failed=ai_metrics.get('llm_failed', 0),
    ))
    progress(82, 'Consolidation résultat simple + écriture BigQuery')
    tracker.step('CONSOLIDATION', 'Consolidation résultat simple + écriture BigQuery')
    consolidation_metrics = _consolidate_marketsegmenter_ai_results(
        ai_csv_path=ai_csv,
        final_csv_path=final_csv,
        output_table_name=output_table_name,
        process_id=str(job.id),
        low_conf_threshold=low_conf_threshold,
        min_conf_threshold=min_conf_threshold,
        progress=progress,
        log=log,
        bq=bq,
        write_batch_size=write_batch_size,
        progress_log_every=max(1, int(getattr(settings, 'PIPELINE_PROGRESS_LOG_EVERY_ROWS', 5000) or 5000)),
        tracker=tracker,
    )
    if cleanup_intermediate:
        _cleanup_temp_file(ai_csv, log, tracker)
        _cleanup_temp_file(ai_summary_path, log, tracker)
    for key, value in consolidation_metrics.items():
        tracker.set_metric(key, value)
    tracker.step('BQ_WRITE', 'Écriture BigQuery du résultat final')
    tracker.log(f"[JOB] Step=BQ_WRITE | written={consolidation_metrics.get('rows_written', 0)} | batches={consolidation_metrics.get('bq_write_batches', 0)} | table={output_table_name}")
    log(f"📤 {consolidation_metrics.get('rows_written', 0)} ligne(s) écrites dans BigQuery table {output_table_name} en streaming batché")
    tracker.step('DONE', 'Résumé et finalisation du job')
    summary = {
        'job_id': str(job.id),
        'source_mode': 'bigquery',
        'source_table': table_name,
        'source_country_code': country_code,
        'output_table': output_table_name,
        'source_rows': row_count,
        'result_rows': consolidation_metrics.get('result_rows', 0),
        'rows_written': consolidation_metrics.get('rows_written', 0),
        'write_batches': consolidation_metrics.get('bq_write_batches', 0),
        'output_csv': final_csv.name,
        'low_confidence_threshold': low_conf_threshold,
        'min_confidence_threshold': min_conf_threshold,
        'read_page_size': read_page_size,
        'write_batch_size': write_batch_size,
        'cleanup_intermediate': cleanup_intermediate,
        'cleanup_workdir': cleanup_workdir,
        'source_column_pruning': source_column_pruning,
        'selected_columns': selected_columns,
        'observability': tracker.snapshot(),
    }
    final_summary_path = final_csv.with_name(final_csv.stem + '_summary.json')
    final_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    job.refresh_from_db(); JobService.enforce_not_cancelled(job)
    with final_csv.open('rb') as fh:
        job.output_file.save(final_csv.name, File(fh), save=False)
    if cleanup_workdir:
        _cleanup_temp_file(final_csv, log, tracker)
        _cleanup_temp_file(final_summary_path, log, tracker)
        _cleanup_empty_dir(job_root, log, tracker)
    JobService.mark_success(job, message='Market segmenter BigQuery + AI terminé avec succès')
    return str(job.id)


def _build_bigquery_selected_columns(parameters: dict) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()

    def _push(value: object) -> None:
        cleaned = str(value or '').strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            selected.append(cleaned)

    marketsegmenter_mapping = dict(parameters.get('marketsegmenter_mapping') or {})
    ai_review_mapping = dict(parameters.get('ai_review_mapping') or {})
    for source_column in marketsegmenter_mapping.values():
        _push(source_column)
    for source_column in ai_review_mapping.values():
        _push(source_column)
    for fallback_col in ['google_place_id', 'place_id', 'google_id', 'country_code']:
        _push(fallback_col)
    return selected


def _cleanup_temp_file(path: Path, log, tracker: JobTracker | None = None) -> None:
    try:
        if path.exists():
            size_bytes = path.stat().st_size
            path.unlink()
            if tracker is not None:
                tracker.incr('cleanup_files_deleted', 1)
                tracker.incr('cleanup_bytes_freed', size_bytes)
            log(f'🧹 Fichier intermédiaire supprimé : {path.name} ({size_bytes} bytes libérés)')
    except Exception as exc:
        log(f'⚠️ Impossible de supprimer le fichier intermédiaire {path.name}: {exc}')


def _cleanup_empty_dir(path: Path, log, tracker: JobTracker | None = None) -> None:
    try:
        if path.exists() and path.is_dir() and not any(path.iterdir()):
            path.rmdir()
            if tracker is not None:
                tracker.incr('cleanup_dirs_deleted', 1)
            log(f'🧹 Répertoire de travail supprimé : {path.name}')
    except Exception as exc:
        log(f'⚠️ Impossible de supprimer le répertoire de travail {path.name}: {exc}')

def _split_segment_path(raw_value: str) -> list[str]:
    text = (raw_value or '').strip()
    if not text:
        return []
    return [part.strip() for part in text.split('>') if part.strip()]


def _consolidate_marketsegmenter_ai_results(ai_csv_path: Path, final_csv_path: Path, output_table_name: str, process_id: str, low_conf_threshold: float, min_conf_threshold: float, progress, log, bq: BigQueryService | None = None, write_batch_size: int | None = None, progress_log_every: int | None = None, tracker: JobTracker | None = None):
    metrics = {
        'consolidated_rules_confident': 0,
        'consolidated_llm': 0,
        'consolidated_out_of_scope': 0,
        'consolidated_rules_fallback': 0,
        'consolidated_ai_fallback': 0,
        'consolidated_none': 0,
        'result_rows': 0,
        'rows_written': 0,
        'bq_write_batches': 0,
    }
    writer_headers = ['google_place_id', 'market_segment_type0', 'market_segment_type1', 'market_segment_type2', 'market_segment_type3']
    final_csv_path.parent.mkdir(parents=True, exist_ok=True)
    batch_size = max(1, int(write_batch_size or getattr(settings, 'BIGQUERY_WRITE_BATCH_SIZE', 1000) or 1000))
    safe_progress_every = max(1, int(progress_log_every or getattr(settings, 'PIPELINE_PROGRESS_LOG_EVERY_ROWS', 5000) or 5000))
    bq_service = bq or BigQueryService()
    consolidation_started = time.monotonic()
    with ai_csv_path.open('r', encoding='utf-8-sig', newline='') as in_fh, final_csv_path.open('w', encoding='utf-8-sig', newline='') as out_fh:
        reader = csv.DictReader(in_fh, delimiter=';')
        writer = csv.DictWriter(out_fh, fieldnames=writer_headers, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
        writer.writeheader()
        def _iter_bq_rows():
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
                if rules_conf <= min_conf_threshold:
                    final_segments = ['hors cible', '', '', '']
                    metrics['consolidated_out_of_scope'] += 1
                elif ai_selected and ai_segments and ai_source.startswith('llm_'):
                    final_segments = (ai_segments + ['', '', '', ''])[:4]
                    metrics['consolidated_llm'] += 1
                elif not ai_selected and rules_conf >= low_conf_threshold and rules_has_segments:
                    final_segments = rules_segments
                    metrics['consolidated_rules_confident'] += 1
                elif ai_segments and ai_source != 'rules_initial':
                    final_segments = (ai_segments + ['', '', '', ''])[:4]
                    metrics['consolidated_ai_fallback'] += 1
                elif rules_has_segments:
                    final_segments = rules_segments
                    metrics['consolidated_rules_fallback'] += 1
                else:
                    final_segments = ['', '', '', '']
                    metrics['consolidated_none'] += 1
                simple_row = {
                    'google_place_id': google_place_id,
                    'market_segment_type0': final_segments[0],
                    'market_segment_type1': final_segments[1],
                    'market_segment_type2': final_segments[2],
                    'market_segment_type3': final_segments[3],
                }
                writer.writerow(simple_row)
                metrics['result_rows'] += 1
                metrics['bq_write_batches'] = ((metrics['result_rows'] - 1) // batch_size) + 1
                if idx == 1 or idx % safe_progress_every == 0:
                    elapsed = max(time.monotonic() - consolidation_started, 0.001)
                    rate = idx / elapsed * 60.0
                    progress(82, f'Consolidation / écriture streaming : {idx} ligne(s) | {rate:.1f} rows/min')
                    if tracker is not None:
                        tracker.set_metric('consolidation_rows_so_far', idx)
                        tracker.set_metric('consolidation_rate_rows_per_min', round(rate, 2))
                        tracker.log(f'[JOB] Step=CONSOLIDATION | rows={idx} | rate={rate:.1f} rows/min | batches={metrics["bq_write_batches"]}')
                yield BigQueryService.build_segmented_row(google_place_id=google_place_id, segments=final_segments, process_id=process_id)
        metrics['rows_written'] = bq_service.write_segmented_rows_iterable(output_table_name, _iter_bq_rows(), batch_size=batch_size)
    total_elapsed = max(time.monotonic() - consolidation_started, 0.001)
    metrics['consolidation_elapsed_ms'] = int(total_elapsed * 1000)
    metrics['consolidation_rate_rows_per_min'] = round((metrics['result_rows'] / total_elapsed * 60.0), 2) if metrics['result_rows'] else 0.0
    log(f"🧩 Consolidation finale terminée : {metrics['result_rows']} ligne(s), écrites en {metrics['bq_write_batches']} batch(es) BigQuery, seuil haut={low_conf_threshold}, seuil min={min_conf_threshold}, elapsed={total_elapsed:.1f}s, rate={metrics['consolidation_rate_rows_per_min']:.1f} rows/min")
    return metrics


def _compute_rules_gating_metrics(csv_path: Path, low_conf_threshold: float, min_conf_threshold: float) -> dict[str, int]:
    metrics = {
        'total_rows': 0,
        'rules_high_confidence': 0,
        'rules_low_confidence_ai': 0,
        'rules_very_low_out_of_scope': 0,
        'rules_unclassified': 0,
    }
    with csv_path.open('r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh, delimiter=';')
        for row in reader:
            metrics['total_rows'] += 1
            try:
                conf = float(str(row.get('segmentation_confidence') or '').replace(',', '.'))
            except Exception:
                conf = 0.0
            if conf >= low_conf_threshold:
                metrics['rules_high_confidence'] += 1
            elif conf <= min_conf_threshold:
                metrics['rules_very_low_out_of_scope'] += 1
            else:
                metrics['rules_low_confidence_ai'] += 1
    metrics['rules_unclassified'] = metrics['total_rows'] - (
        metrics['rules_high_confidence'] + metrics['rules_low_confidence_ai'] + metrics['rules_very_low_out_of_scope']
    )
    return metrics
def _compute_ai_review_metrics(csv_path: Path, low_conf_threshold: float, min_conf_threshold: float) -> dict[str, int]:
    metrics = {
        'llm_calls': 0,
        'llm_success': 0,
        'llm_failed': 0,
        'ai_selected_yes': 0,
    }
    with csv_path.open('r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh, delimiter=';')
        for row in reader:
            selected = (row.get('ai_selected_for_review') or '').strip().lower() == 'yes'
            if selected:
                metrics['ai_selected_yes'] += 1
            source = (row.get('ai_segment_source') or '').strip().lower()
            if source.startswith('llm_'):
                metrics['llm_calls'] += 1
                if 'error' in source or 'failed' in source:
                    metrics['llm_failed'] += 1
                else:
                    metrics['llm_success'] += 1
    return metrics

def _read_json_if_exists(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}
    return {}


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
        low_confidence_threshold=float(parameters.get('ai_review_low_confidence_threshold') or settings.AI_REVIEW_LOW_CONFIDENCE_THRESHOLD),
        min_confidence_threshold=float(parameters.get('ai_review_min_confidence_threshold') or settings.AI_REVIEW_MIN_CONFIDENCE_THRESHOLD),
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
    log(f"🎯 Gating AI : seuil haut={options.low_confidence_threshold} | seuil min={options.min_confidence_threshold}")
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

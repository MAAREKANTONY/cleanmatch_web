
from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.db.models import Count
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST
from rest_framework.decorators import api_view
from rest_framework.response import Response

from normalizer.services.normalizer_service import CANONICAL_MAPPING_FIELDS, inspect_excel_workbook
from matcher.services.matcher_service import MATCHER_MAPPING_FIELDS, inspect_table_file
from geocoder.services.geocoder_service import GEOCODER_MAPPING_FIELDS, inspect_geocoder_file
from geoclass.services.geoclass_service import GEOCLASS_MAPPING_FIELDS
from marketsegmenter.services.marketsegmenter_service import MARKETSEGMENTER_MAPPING_FIELDS, inspect_marketsegmenter_file, suggest_column_mapping as suggest_marketsegmenter_mapping
from ai_review.services.ai_review_service import AI_REVIEW_MAPPING_FIELDS, inspect_ai_review_file, suggest_ai_review_mapping
from bigquery.client import BigQueryService, BigQueryConfigError
from ai_review.services.capability_engine import AI_REVIEW_ACTION_PROFILES, AI_REVIEW_CAPABILITIES

from .forms import JobCreateForm
from .models import Job
from .serializers import JobSerializer
from .services import JobService
from .tasks import run_uploaded_job


def home(request):
    jobs = Job.objects.all()

    job_type = request.GET.get('job_type', '').strip()
    status = request.GET.get('status', '').strip()
    q = request.GET.get('q', '').strip()

    if job_type:
        jobs = jobs.filter(job_type=job_type)
    if status:
        jobs = jobs.filter(status=status)
    if q:
        jobs = jobs.filter(input_file_1__icontains=q)

    stats_raw = Job.objects.values('status').annotate(total=Count('id'))
    stats = {row['status']: row['total'] for row in stats_raw}
    storage = JobService.media_storage_stats()
    context = {
        'jobs': jobs[:30],
        'stats': {
            'total': Job.objects.count(),
            'running': stats.get(Job.Status.RUNNING, 0),
            'queued': stats.get(Job.Status.QUEUED, 0),
            'success': stats.get(Job.Status.SUCCESS, 0),
            'failed': stats.get(Job.Status.FAILED, 0),
        },
        'storage': {
            'input_size': JobService.human_bytes(storage['bytes']['inputs']),
            'output_size': JobService.human_bytes(storage['bytes']['outputs']),
            'error_size': JobService.human_bytes(storage['bytes']['errors']),
            'total_size': JobService.human_bytes(storage['bytes']['total']),
            'input_count': storage['counts']['inputs'],
            'output_count': storage['counts']['outputs'],
            'error_count': storage['counts']['errors'],
            'total_count': storage['counts']['total'],
        },
        'filters': {'job_type': job_type, 'status': status, 'q': q},
        'job_type_choices': Job.JobType.choices,
        'status_choices': Job.Status.choices,
    }
    return render(request, 'jobs/home.html', context)


def create_job(request):
    if request.method == 'POST':
        form = JobCreateForm(request.POST, request.FILES)
        if form.is_valid():
            parameters = {
                'mode': 'uploaded',
                'filename_1': form.cleaned_data['input_file_1'].name if form.cleaned_data.get('input_file_1') else None,
                'filename_2': form.cleaned_data['input_file_2'].name if form.cleaned_data.get('input_file_2') else None,
            }
            if form.cleaned_data['job_type'] == Job.JobType.NORMALIZER:
                parameters.update({
                    'do_clean': form.cleaned_data['normalizer_do_clean'],
                    'do_matchcode': form.cleaned_data['normalizer_do_matchcode'],
                    'sheet_name': form.cleaned_data['normalizer_sheet_name'].strip(),
                    'country_code': (form.cleaned_data.get('normalizer_country_code') or 'FR').strip() or 'FR',
                    'column_mapping': form.get_mapping_payload(form.cleaned_data),
                })
            elif form.cleaned_data['job_type'] == Job.JobType.MATCHER:
                parameters.update({
                    'master_sheet_name': (form.cleaned_data.get('matcher_master_sheet_name') or '').strip() or None,
                    'slave_sheet_name': (form.cleaned_data.get('matcher_slave_sheet_name') or '').strip() or None,
                    'threshold_name': form.cleaned_data.get('matcher_threshold_name') or 85,
                    'threshold_voie': form.cleaned_data.get('matcher_threshold_voie') or 70,
                    'top_k_per_master': form.cleaned_data.get('matcher_top_k') or 5,
                    'master_mapping': form.get_matcher_mapping_payload(form.cleaned_data, 'master'),
                    'slave_mapping': form.get_matcher_mapping_payload(form.cleaned_data, 'slave'),
                })
            elif form.cleaned_data['job_type'] == Job.JobType.GEOCODER:
                parameters.update({
                    'geocoder_sheet_name': (form.cleaned_data.get('geocoder_sheet_name') or '').strip() or None,
                    'geocoder_provider': (form.cleaned_data.get('geocoder_provider') or 'existing_or_nominatim').strip(),
                    'country_hint': (form.cleaned_data.get('geocoder_country_hint') or '').strip(),
                    'geocoder_mapping': form.get_geocoder_mapping_payload(form.cleaned_data),
                })
            elif form.cleaned_data['job_type'] == Job.JobType.MARKETSEGMENTER:
                source_mode = (form.cleaned_data.get('marketsegmenter_source_mode') or 'uploaded').strip() or 'uploaded'
                parameters.update({
                    'mode': 'bigquery' if source_mode == 'bigquery' else 'uploaded',
                    'marketsegmenter_source_mode': source_mode,
                    'marketsegmenter_sheet_name': (form.cleaned_data.get('marketsegmenter_sheet_name') or '').strip() or None,
                    'marketsegmenter_country_default': (form.cleaned_data.get('marketsegmenter_country_default') or '').strip(),
                    'marketsegmenter_mapping': form.get_marketsegmenter_mapping_payload(form.cleaned_data),
                    'ai_review_mapping': form.get_ai_review_mapping_payload(form.cleaned_data),
                    'ai_review_low_confidence_threshold': float(form.cleaned_data.get('ai_review_low_confidence_threshold') or 0.65),
                    'ai_review_action_profile': (form.cleaned_data.get('ai_review_action_profile') or 'standard').strip() or 'standard',
                    'ai_review_llm_enabled': bool(form.cleaned_data.get('ai_review_llm_enabled')),
                    'ai_review_llm_provider': str(form.cleaned_data.get('ai_review_llm_provider') or ''),
                    'ai_review_llm_model': str(form.cleaned_data.get('ai_review_llm_model') or ''),
                    'ai_review_llm_max_budget_eur': float(form.cleaned_data.get('ai_review_llm_max_budget_eur') or 0.0),
                    'ai_review_llm_max_cost_per_row_eur': float(form.cleaned_data.get('ai_review_llm_max_cost_per_row_eur') or 0.0),
                    'ai_review_llm_max_calls_per_row': int(form.cleaned_data.get('ai_review_llm_max_calls_per_row') or 1),
                    'marketsegmenter_bq_table_name': (form.cleaned_data.get('marketsegmenter_bq_table_name') or '').strip() or settings.BIGQUERY_INPUT_TABLE,
                    'marketsegmenter_bq_country_code': (form.cleaned_data.get('marketsegmenter_bq_country_code') or '').strip(),
                    'marketsegmenter_bq_output_table_name': (form.cleaned_data.get('marketsegmenter_bq_output_table_name') or '').strip() or settings.BIGQUERY_OUTPUT_TABLE,
                })
            elif form.cleaned_data['job_type'] == Job.JobType.AI_REVIEW:
                parameters.update({
                    'ai_review_sheet_name': (form.cleaned_data.get('ai_review_sheet_name') or '').strip() or None,
                    'ai_review_low_confidence_threshold': float(form.cleaned_data.get('ai_review_low_confidence_threshold') or 0.65),
                    'ai_review_action_profile': (form.cleaned_data.get('ai_review_action_profile') or 'standard').strip() or 'standard',
                    'ai_review_llm_enabled': bool(form.cleaned_data.get('ai_review_llm_enabled')),
                    'ai_review_llm_provider': str(form.cleaned_data.get('ai_review_llm_provider') or ''),
                    'ai_review_llm_model': str(form.cleaned_data.get('ai_review_llm_model') or ''),
                    'ai_review_llm_max_budget_eur': float(form.cleaned_data.get('ai_review_llm_max_budget_eur') or 0.0),
                    'ai_review_llm_max_cost_per_row_eur': float(form.cleaned_data.get('ai_review_llm_max_cost_per_row_eur') or 0.0),
                    'ai_review_llm_max_calls_per_row': int(form.cleaned_data.get('ai_review_llm_max_calls_per_row') or 1),
                    'ai_review_mapping': form.get_ai_review_mapping_payload(form.cleaned_data),
                })
            elif form.cleaned_data['job_type'] == Job.JobType.GEOCLASS:
                parameters.update({
                    'geoclass_sheet_name': (form.cleaned_data.get('geoclass_sheet_name') or '').strip() or None,
                    'geoclass_mapping': form.get_geoclass_mapping_payload(form.cleaned_data),
                })

            try:
                JobService.ensure_disk_space(str(Path('media').resolve()))
            except Exception as exc:
                messages.error(request, str(exc))
                return render(request, 'jobs/new.html', {
                    'form': form,
                    'canonical_mapping_fields': CANONICAL_MAPPING_FIELDS,
                    'matcher_mapping_fields': MATCHER_MAPPING_FIELDS,
                    'geocoder_mapping_fields': GEOCODER_MAPPING_FIELDS,
                    'geoclass_mapping_fields': GEOCLASS_MAPPING_FIELDS,
                    'marketsegmenter_mapping_fields': MARKETSEGMENTER_MAPPING_FIELDS,
                    'ai_review_mapping_fields': AI_REVIEW_MAPPING_FIELDS,
                    'ai_review_action_profiles': sorted(AI_REVIEW_ACTION_PROFILES.keys()),
                    'ai_review_capabilities': AI_REVIEW_CAPABILITIES,
                    'bigquery_input_table_default': settings.BIGQUERY_INPUT_TABLE,
                    'bigquery_output_table_default': settings.BIGQUERY_OUTPUT_TABLE,
                })

            job = Job.objects.create(
                job_type=form.cleaned_data['job_type'],
                status=Job.Status.PENDING,
                progress_message='Job créé',
                parameters_json=parameters,
                input_file_1=form.cleaned_data.get('input_file_1'),
                input_file_2=form.cleaned_data.get('input_file_2') or None,
            )
            async_result = run_uploaded_job.delay(str(job.id))
            JobService.mark_queued(job, async_result.id)
            messages.success(request, f'Job {job.id} créé et envoyé au worker.')
            return redirect('jobs:detail', job_id=job.id)
    else:
        form = JobCreateForm()

    return render(request, 'jobs/new.html', {
        'form': form,
        'canonical_mapping_fields': CANONICAL_MAPPING_FIELDS,
        'matcher_mapping_fields': MATCHER_MAPPING_FIELDS,
        'geocoder_mapping_fields': GEOCODER_MAPPING_FIELDS,
        'geoclass_mapping_fields': GEOCLASS_MAPPING_FIELDS,
        'marketsegmenter_mapping_fields': MARKETSEGMENTER_MAPPING_FIELDS,
        'ai_review_mapping_fields': AI_REVIEW_MAPPING_FIELDS,
        'ai_review_action_profiles': sorted(AI_REVIEW_ACTION_PROFILES.keys()),
        'ai_review_capabilities': AI_REVIEW_CAPABILITIES,
        'bigquery_input_table_default': settings.BIGQUERY_INPUT_TABLE,
        'bigquery_output_table_default': settings.BIGQUERY_OUTPUT_TABLE,
    })



def help_page(request):
    context = {
        'processes': [
            {'name': 'Normalizer', 'status': 'advanced', 'outputs': 'CSV UTF-8', 'notes': 'mapping, matchcode, multi-country Europe V1'},
            {'name': 'Matcher', 'status': 'advanced', 'outputs': 'ZIP multi-fichiers', 'notes': 'parity hardening, diagnostics enrichis'},
            {'name': 'Geocoder', 'status': 'intermediate', 'outputs': 'CSV + summary JSON', 'notes': 'checkpoint, reprise, cache'},
            {'name': 'Geoclass', 'status': 'initial', 'outputs': 'CSV + summary JSON', 'notes': 'classification heuristique addititve'},
            {'name': 'Market Segmenter FYRE', 'status': 'initial', 'outputs': 'CSV + summary JSON', 'notes': 'Google Places vers taxonomie FYRE avec types + mots-clés multilingues pays-aware + signal prix'},
            {'name': 'AI Review', 'status': 'active', 'outputs': 'CSV + summary JSON', 'notes': 'mapping canonique, action profiles et capacités agent sans LLM dans ce sprint'},
        ],
        'contracts': [
            'Conserver les contrats JSON côté inspection avec le flag ok.',
            'Ne jamais casser les hidden fields de mapping attendus par le front.',
            'Privilégier les exports CSV/ZIP plutôt que XLSX pour les gros volumes.',
            'Toujours rester additif sur .env, routes, structure et conventions de nommage.',
        ],
    }
    return render(request, 'jobs/help.html', context)

def job_detail(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    return render(request, 'jobs/job_detail.html', {'job': job})


@api_view(['GET'])
def api_job_detail(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    serializer = JobSerializer(job, context={'request': request})
    return Response(serializer.data)


@require_POST
def cancel_job(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    if job.is_finished:
        return JsonResponse({'ok': False, 'error': 'Le job est déjà terminé.'}, status=400)
    JobService.request_cancel(job)
    serializer = JobSerializer(job, context={'request': request})
    return JsonResponse({'ok': True, 'job': serializer.data})


@require_POST
def delete_job(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    delete_files = request.POST.get('delete_files') == '1'
    try:
        JobService.delete_job(job, delete_files=delete_files)
    except Exception as exc:
        messages.error(request, str(exc))
        return redirect('jobs:detail', job_id=job_id)
    messages.success(request, 'Job supprimé.' + (' Les fichiers liés ont aussi été supprimés.' if delete_files else ''))
    return redirect('jobs:home')


@require_POST
def cleanup_job_files(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    mode = request.POST.get('mode', 'both')
    try:
        result = JobService.delete_job_files(
            job,
            delete_input=mode in {'input', 'both'},
            delete_output=mode in {'output', 'both'},
            delete_error=mode in {'error', 'both', 'output'},
        )
    except Exception as exc:
        messages.error(request, f'Nettoyage impossible : {exc}')
        return redirect('jobs:detail', job_id=job.id)
    count = sum(1 for value in result.values() if value)
    messages.success(request, f'Nettoyage terminé : {count} fichier(s) supprimé(s).')
    return redirect('jobs:detail', job_id=job.id)


@require_POST
def maintenance_cleanup(request):
    action = request.POST.get('action', '')
    if action == 'cleanup_old_jobs':
        days = int(request.POST.get('days', '30') or '30')
        result = JobService.cleanup_old_jobs(days=days, delete_files=True)
        messages.success(request, f"Purge terminée : {result['deleted_jobs']} job(s) supprimé(s), {result['deleted_files']} fichier(s) supprimé(s).")
    elif action == 'cleanup_orphan_files':
        result = JobService.cleanup_orphan_files()
        messages.success(request, f"Nettoyage fichiers orphelins terminé : {result['deleted_count']} fichier(s) supprimé(s).")
    else:
        messages.error(request, 'Action de maintenance inconnue.')
    return redirect('jobs:home')


def inspect_excel(request):
    if request.method != 'POST':
        return JsonResponse({'ok': False, 'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('input_file_1')
    if not uploaded:
        return JsonResponse({'ok': False, 'error': 'Aucun fichier fourni.'}, status=400)
    filename = uploaded.name.lower()
    allowed_ext = {'.xlsx', '.xlsm', '.xltx', '.xltm'}
    if not any(filename.endswith(ext) for ext in allowed_ext):
        return JsonResponse({'ok': False, 'error': 'Inspection disponible uniquement pour les fichiers Excel.'}, status=400)
    try:
        sheets = inspect_excel_workbook(uploaded)
    except Exception as exc:
        return JsonResponse({'ok': False, 'error': f'Impossible de lire le fichier Excel : {exc}'}, status=400)
    return JsonResponse({'ok': True, 'filename': Path(uploaded.name).name, 'canonical_mapping_fields': CANONICAL_MAPPING_FIELDS, 'sheets': sheets})


def inspect_matcher_file(request):
    if request.method != 'POST':
        return JsonResponse({'ok': False, 'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('file')
    role = request.POST.get('role', 'master')
    if not uploaded:
        return JsonResponse({'ok': False, 'error': 'Aucun fichier fourni.'}, status=400)
    try:
        payload = inspect_table_file(uploaded)
    except Exception as exc:
        return JsonResponse({'ok': False, 'error': f'Impossible d’inspecter le fichier : {exc}'}, status=400)
    payload['role'] = role
    payload['mapping_fields'] = MATCHER_MAPPING_FIELDS
    payload['ok'] = True
    return JsonResponse(payload)



def inspect_ai_review(request):
    if request.method != 'POST':
        return JsonResponse({'ok': False, 'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('file') or request.FILES.get('input_file_1')
    if not uploaded:
        return JsonResponse({'ok': False, 'error': 'Aucun fichier fourni.'}, status=400)
    try:
        payload = inspect_ai_review_file(uploaded)
    except Exception as exc:
        return JsonResponse({'ok': False, 'error': f'Impossible d’inspecter le fichier : {exc}'}, status=400)
    payload['ok'] = True
    return JsonResponse(payload)

def inspect_geocoder(request):
    if request.method != 'POST':
        return JsonResponse({'ok': False, 'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('file') or request.FILES.get('input_file_1')
    if not uploaded:
        return JsonResponse({'ok': False, 'error': 'Aucun fichier fourni.'}, status=400)
    try:
        payload = inspect_geocoder_file(uploaded)
    except Exception as exc:
        return JsonResponse({'ok': False, 'error': f'Impossible d’inspecter le fichier : {exc}'}, status=400)
    payload['mapping_fields'] = GEOCODER_MAPPING_FIELDS
    payload['ok'] = True
    return JsonResponse(payload)

def inspect_marketsegmenter(request):
    if request.method != 'POST':
        return JsonResponse({'ok': False, 'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('file') or request.FILES.get('input_file_1')
    if not uploaded:
        return JsonResponse({'ok': False, 'error': 'Aucun fichier fourni.'}, status=400)
    try:
        payload = inspect_marketsegmenter_file(uploaded)
        for sheet in payload.get('sheets', []):
            sheet['ai_review_mapping_suggestions'] = suggest_ai_review_mapping(sheet.get('detected_columns', []))
    except Exception as exc:
        return JsonResponse({'ok': False, 'error': f'Impossible d’inspecter le fichier : {exc}'}, status=400)
    payload['mapping_fields'] = MARKETSEGMENTER_MAPPING_FIELDS
    payload['ok'] = True
    return JsonResponse(payload)


@require_POST
def inspect_marketsegmenter_bigquery(request):
    table_name = (request.POST.get('table_name') or '').strip() or settings.BIGQUERY_INPUT_TABLE
    country_code = (request.POST.get('country_code') or '').strip()
    try:
        service = BigQueryService()
        payload = service.inspect_table(table_name=table_name, country_code=country_code, limit=20)
        for sheet in payload.get('sheets', []):
            columns = sheet.get('detected_columns', [])
            sheet['mapping_suggestions'] = suggest_marketsegmenter_mapping(columns)
            sheet['ai_review_mapping_suggestions'] = suggest_ai_review_mapping(columns)
        return JsonResponse({'ok': True, **payload})
    except (BigQueryConfigError, Exception) as exc:
        return JsonResponse({'ok': False, 'error': str(exc)}, status=400)

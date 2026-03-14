
from pathlib import Path

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
                'filename_1': form.cleaned_data['input_file_1'].name,
                'filename_2': form.cleaned_data['input_file_2'].name if form.cleaned_data.get('input_file_2') else None,
            }
            if form.cleaned_data['job_type'] == Job.JobType.NORMALIZER:
                parameters.update({
                    'do_clean': form.cleaned_data['normalizer_do_clean'],
                    'do_matchcode': form.cleaned_data['normalizer_do_matchcode'],
                    'sheet_name': form.cleaned_data['normalizer_sheet_name'].strip(),
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

            try:
                JobService.ensure_disk_space(str(Path('media').resolve()))
            except Exception as exc:
                messages.error(request, str(exc))
                return render(request, 'jobs/new.html', {
                    'form': form,
                    'canonical_mapping_fields': CANONICAL_MAPPING_FIELDS,
                    'matcher_mapping_fields': MATCHER_MAPPING_FIELDS,
                    'geocoder_mapping_fields': GEOCODER_MAPPING_FIELDS,
                })

            job = Job.objects.create(
                job_type=form.cleaned_data['job_type'],
                status=Job.Status.PENDING,
                progress_message='Job créé',
                parameters_json=parameters,
                input_file_1=form.cleaned_data['input_file_1'],
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
    })


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
        return JsonResponse({'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('input_file_1')
    if not uploaded:
        return JsonResponse({'error': 'Aucun fichier fourni.'}, status=400)
    filename = uploaded.name.lower()
    allowed_ext = {'.xlsx', '.xlsm', '.xltx', '.xltm'}
    if not any(filename.endswith(ext) for ext in allowed_ext):
        return JsonResponse({'error': 'Inspection disponible uniquement pour les fichiers Excel.'}, status=400)
    try:
        sheets = inspect_excel_workbook(uploaded)
    except Exception as exc:
        return JsonResponse({'error': f'Impossible de lire le fichier Excel : {exc}'}, status=400)
    return JsonResponse({'filename': Path(uploaded.name).name, 'canonical_mapping_fields': CANONICAL_MAPPING_FIELDS, 'sheets': sheets})


def inspect_matcher_file(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('file')
    role = request.POST.get('role', 'master')
    if not uploaded:
        return JsonResponse({'error': 'Aucun fichier fourni.'}, status=400)
    try:
        payload = inspect_table_file(uploaded)
    except Exception as exc:
        return JsonResponse({'error': f'Impossible d’inspecter le fichier : {exc}'}, status=400)
    payload['role'] = role
    payload['mapping_fields'] = MATCHER_MAPPING_FIELDS
    return JsonResponse(payload)


def inspect_geocoder(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Méthode non autorisée.'}, status=405)
    uploaded = request.FILES.get('file') or request.FILES.get('input_file_1')
    if not uploaded:
        return JsonResponse({'error': 'Aucun fichier fourni.'}, status=400)
    try:
        payload = inspect_geocoder_file(uploaded)
    except Exception as exc:
        return JsonResponse({'error': f'Impossible d’inspecter le fichier : {exc}'}, status=400)
    payload['mapping_fields'] = GEOCODER_MAPPING_FIELDS
    return JsonResponse(payload)

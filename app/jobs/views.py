from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect, render
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .forms import JobCreateForm
from .models import Job
from .serializers import JobSerializer
from .services import JobService
from .tasks import run_uploaded_job


def home(request):
    jobs = Job.objects.all()[:20]
    return render(request, 'jobs/home.html', {'jobs': jobs})


def create_job(request):
    if request.method == 'POST':
        form = JobCreateForm(request.POST, request.FILES)
        if form.is_valid():
            job = Job.objects.create(
                job_type=form.cleaned_data['job_type'],
                status=Job.Status.PENDING,
                progress_message='Job créé',
                parameters_json={
                    'mode': 'uploaded',
                    'filename_1': form.cleaned_data['input_file_1'].name,
                    'filename_2': form.cleaned_data['input_file_2'].name if form.cleaned_data.get('input_file_2') else None,
                },
                input_file_1=form.cleaned_data['input_file_1'],
                input_file_2=form.cleaned_data.get('input_file_2') or None,
            )
            async_result = run_uploaded_job.delay(str(job.id))
            JobService.mark_queued(job, async_result.id)
            messages.success(request, f'Job {job.id} créé et envoyé au worker.')
            return redirect('jobs:detail', job_id=job.id)
    else:
        form = JobCreateForm()

    return render(request, 'jobs/new.html', {'form': form})


def job_detail(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    return render(request, 'jobs/job_detail.html', {'job': job})


@api_view(['GET'])
def api_job_detail(request, job_id):
    job = get_object_or_404(Job, id=job_id)
    serializer = JobSerializer(job, context={'request': request})
    return Response(serializer.data)

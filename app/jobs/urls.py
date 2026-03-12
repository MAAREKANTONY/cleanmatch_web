from django.urls import path
from .views import api_job_detail, cancel_job, create_job, home, inspect_excel, job_detail

app_name = 'jobs'

urlpatterns = [
    path('', home, name='home'),
    path('jobs/new/', create_job, name='new'),
    path('jobs/inspect-excel/', inspect_excel, name='inspect_excel'),
    path('jobs/<uuid:job_id>/', job_detail, name='detail'),
    path('jobs/<uuid:job_id>/cancel/', cancel_job, name='cancel'),
    path('api/jobs/<uuid:job_id>/', api_job_detail, name='api_detail'),
]

from django.urls import path
from .views import api_job_detail, create_job, home, job_detail

app_name = 'jobs'

urlpatterns = [
    path('', home, name='home'),
    path('jobs/new/', create_job, name='new'),
    path('jobs/<uuid:job_id>/', job_detail, name='detail'),
    path('api/jobs/<uuid:job_id>/', api_job_detail, name='api_detail'),
]

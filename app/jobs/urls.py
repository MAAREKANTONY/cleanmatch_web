from django.urls import path

from . import views

app_name = 'jobs'

urlpatterns = [
    path('', views.home, name='home'),
    path('new/', views.create_job, name='new'),
    path('inspect-excel/', views.inspect_excel, name='inspect_excel'),
    path('inspect-matcher-file/', views.inspect_matcher_file, name='inspect_matcher_file'),
    path('inspect-geocoder-file/', views.inspect_geocoder, name='inspect_geocoder'),
    path('<uuid:job_id>/', views.job_detail, name='detail'),
    path('<uuid:job_id>/cancel/', views.cancel_job, name='cancel'),
    path('api/<uuid:job_id>/', views.api_job_detail, name='api_detail'),
]

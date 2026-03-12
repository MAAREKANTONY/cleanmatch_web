# Generated manually for iteration 1
from django.db import migrations, models
import uuid


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('job_type', models.CharField(choices=[('demo', 'Demo'), ('normalizer', 'Normalizer'), ('matcher', 'Matcher'), ('geocoder', 'Geocoder')], default='demo', max_length=32)),
                ('status', models.CharField(choices=[('pending', 'Pending'), ('queued', 'Queued'), ('running', 'Running'), ('success', 'Success'), ('failed', 'Failed'), ('cancelled', 'Cancelled')], default='pending', max_length=32)),
                ('progress_percent', models.PositiveSmallIntegerField(default=0)),
                ('progress_message', models.CharField(blank=True, max_length=255)),
                ('parameters_json', models.JSONField(blank=True, default=dict)),
                ('input_file_1', models.FileField(blank=True, null=True, upload_to='inputs/')),
                ('input_file_2', models.FileField(blank=True, null=True, upload_to='inputs/')),
                ('output_file', models.FileField(blank=True, null=True, upload_to='outputs/')),
                ('error_file', models.FileField(blank=True, null=True, upload_to='errors/')),
                ('log_text', models.TextField(blank=True)),
                ('error_message', models.TextField(blank=True)),
                ('celery_task_id', models.CharField(blank=True, max_length=255)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('started_at', models.DateTimeField(blank=True, null=True)),
                ('finished_at', models.DateTimeField(blank=True, null=True)),
            ],
            options={'ordering': ['-created_at']},
        ),
    ]

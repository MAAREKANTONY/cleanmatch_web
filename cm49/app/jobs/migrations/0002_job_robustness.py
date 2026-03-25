from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('jobs', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='cancellation_reason',
            field=models.CharField(blank=True, max_length=255),
        ),
        migrations.AddField(
            model_name='job',
            name='cancellation_requested',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='job',
            name='cancelled_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='job',
            name='last_heartbeat',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]

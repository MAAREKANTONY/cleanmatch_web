from rest_framework import serializers
from .models import Job


class JobSerializer(serializers.ModelSerializer):
    output_file_url = serializers.SerializerMethodField()
    error_file_url = serializers.SerializerMethodField()
    input_file_1_name = serializers.SerializerMethodField()
    input_file_2_name = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            'id', 'job_type', 'status', 'progress_percent', 'progress_message',
            'parameters_json', 'output_file_url', 'error_file_url', 'log_text',
            'error_message', 'celery_task_id', 'created_at', 'started_at', 'finished_at',
            'input_file_1_name', 'input_file_2_name', 'cancellation_requested',
            'cancellation_reason', 'cancelled_at', 'last_heartbeat'
        ]

    def get_output_file_url(self, obj):
        request = self.context.get('request')
        if obj.output_file and request:
            return request.build_absolute_uri(obj.output_file.url)
        if obj.output_file:
            return obj.output_file.url
        return None

    def get_error_file_url(self, obj):
        request = self.context.get('request')
        if obj.error_file and request:
            return request.build_absolute_uri(obj.error_file.url)
        if obj.error_file:
            return obj.error_file.url
        return None

    def get_input_file_1_name(self, obj):
        return obj.input_file_1.name if obj.input_file_1 else None

    def get_input_file_2_name(self, obj):
        return obj.input_file_2.name if obj.input_file_2 else None

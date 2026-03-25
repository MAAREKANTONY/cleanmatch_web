from django.http import JsonResponse
from django.utils import timezone


def health_view(request):
    return JsonResponse({
        'status': 'ok',
        'service': 'cleanmatch-web',
        'timestamp': timezone.now().isoformat(),
    })

from django.test import RequestFactory

from jobs.views import help_page


def test_help_page_renders():
    request = RequestFactory().get('/help/')
    response = help_page(request)
    assert response.status_code == 200

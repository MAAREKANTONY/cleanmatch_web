from ai_review.services.ai_review_service import AIReviewInput, AIReviewService


def _make_input(confidence: float):
    return AIReviewInput(
        canonical_context=None,
        sources_used_map={},
        profile_name='standard',
        enabled_capabilities=[],
        outlet_id='1',
        name='Demo',
        address='',
        country='',
        country_code='FR',
        google_id='g1',
        place_id='p1',
        website='',
        website_root_url='',
        website_title='',
        website_meta_description='',
        menu_urls=[],
        photo_urls=[],
        social_links=[],
        segmentation_confidence=confidence,
        initial_segments=['restaurant', 'traditional', '', ''],
        main_type='',
        all_types='',
        descriptions=[],
        reviews_tags='',
        characteristics='',
        hotel_additional_informations='',
        price_range='',
        customer_reported_price_range='',
        website_lang='',
        segmentation_reasons='',
    )


def test_below_min_confidence_forces_out_of_scope():
    service = AIReviewService()
    result = service._build_feature_extraction_result(_make_input(0.10), 0.65, 0.20, True)
    assert result.ai_selected_for_review == 'no'
    assert result.ai_review_status == 'forced_out_of_scope'
    assert result.ai_segment_suggested == 'hors cible'
    assert result.ai_segment_source == 'rules_below_min_threshold'


def test_between_thresholds_selects_ai_review():
    service = AIReviewService()
    result = service._build_feature_extraction_result(_make_input(0.40), 0.65, 0.20, True)
    assert result.ai_selected_for_review == 'yes'
    assert result.ai_review_status == 'selected'


def test_above_high_threshold_skips_ai_review_and_keeps_rules():
    service = AIReviewService()
    result = service._build_feature_extraction_result(_make_input(0.90), 0.65, 0.20, True)
    assert result.ai_selected_for_review == 'no'
    assert result.ai_review_status == 'skipped'
    assert result.ai_segment_source == 'rules_initial'



def test_above_high_threshold_does_not_call_llm(monkeypatch):
    service = AIReviewService()

    def _boom(*args, **kwargs):
        raise AssertionError('LLM should not be called for high-confidence rows')

    monkeypatch.setattr(service, '_run_llm_guardrails', _boom)
    result = service._build_feature_extraction_result(_make_input(0.90), 0.65, 0.20, True)
    assert result.ai_selected_for_review == 'no'
    assert result.ai_review_status == 'skipped'


def test_high_confidence_row_skips_hardening(monkeypatch):
    service = AIReviewService()
    review_input = AIReviewInput(
        name='Safe Cafe',
        segmentation_confidence=0.95,
        initial_segments=['restaurant'],
        enabled_capabilities=['homepage_fetch'],
        website='https://example.com',
        website_title='Safe Cafe',
        website_meta_description='Meta',
        profile_name='standard',
    )

    def _boom(*args, **kwargs):
        raise AssertionError('hardening should not run for high confidence skipped rows')

    monkeypatch.setattr(service, '_scan_keywords', _boom)
    monkeypatch.setattr(service, '_fetch_web_evidence', _boom)
    monkeypatch.setattr(service, '_run_llm_guardrails', _boom)

    result = service._build_feature_extraction_result(review_input, threshold=0.65, min_threshold=0.20, only_low_confidence=True)
    assert result.ai_selected_for_review == 'no'
    assert result.ai_review_status == 'skipped'
    assert result.ai_web_fetch_status == 'skipped_before_hardening'
    assert result.ai_segment_source == 'rules_initial'


def test_llm_taxonomy_validation_rejects_invalid_path(monkeypatch):
    service = AIReviewService()
    review_input = AIReviewInput(
        name='Demo',
        segmentation_confidence=0.40,
        initial_segments=['horeca', 'table_service', 'traditional_dining', ''],
        enabled_capabilities=['metadata_review'],
        website_title='Demo',
        website_meta_description='Demo meta',
        profile_name='standard',
    )

    monkeypatch.setattr(service, '_scan_keywords', lambda *args, **kwargs: [])
    monkeypatch.setattr(service, '_fetch_web_evidence', lambda *args, **kwargs: {'status': 'disabled', 'sources_fetched': [], 'homepage_text': '', 'homepage_title': '', 'homepage_meta_description': '', 'menu_text_excerpt': ''})
    monkeypatch.setattr(service, '_run_llm_guardrails', lambda *args, **kwargs: {
        'status': 'live_success',
        'reason': 'provider_success',
        'result_source': 'live',
        'llm_configured': True,
        'llm_live_call_ready': True,
        'live_call_attempted': True,
        'estimated_cost_eur': 0.001,
        'actual_cost_eur': 0.001,
        'budget_remaining_eur': 1.0,
        'cache_hit': False,
        'row_calls_used': 1,
        'provider': 'test',
        'model': 'test',
        'raw_content': '{}',
        'result_json': {
            'segment_type0': 'horeca',
            'segment_type1': 'table_service',
            'segment_type2': 'invented_segment',
            'segment_type3': '',
            'confidence': 0.93,
            'requires_human_review': True,
            'evidence': ['bad taxonomy'],
            'reasoning_short': 'bad taxonomy',
        },
    })

    result = service._build_feature_extraction_result(review_input, threshold=0.65, min_threshold=0.20, only_low_confidence=True)
    assert result.ai_segment_source == 'rules_initial'
    assert result.ai_segment_suggested == 'horeca > table_service > traditional_dining'
    assert 'llm_rejected_taxonomy=' in result.ai_evidence_summary

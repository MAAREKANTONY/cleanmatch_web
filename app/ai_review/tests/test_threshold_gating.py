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

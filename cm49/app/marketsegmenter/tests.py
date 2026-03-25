from marketsegmenter.services.marketsegmenter_service import MarketSegmenterOptions, MarketSegmenterService
import pandas as pd


def test_marketsegmenter_country_default_and_price_bias_fast_food(tmp_path):
    service = MarketSegmenterService()
    row = pd.Series({
        'name': 'Shawarma Express',
        'country': '',
        'country_code': '',
        'main_type': 'Lebanese restaurant',
        'all_types': 'Fast food restaurant, Restaurant',
        'description_1': 'shawarma wraps takeaway',
        'description_2': '',
        'description_3': '',
        'price_range_simplified': 'low',
        'customer_reported_price_range': '',
        'reviews_tags': 'quick bite',
        'characteristics': 'counter service',
        'hotel_additional_informations': '',
        'website_title': '',
        'website_meta_description': '',
    })
    result = service._classify_row(row, MarketSegmenterOptions(country_default='FR'))
    assert result['fyre_market_segment_type0'] == 'horeca'
    assert result['fyre_market_segment_type1'] == 'fast_food'
    assert result['resolved_country_code'] == 'FR'


def test_marketsegmenter_country_keyword_italian_table_service(tmp_path):
    service = MarketSegmenterService()
    row = pd.Series({
        'name': 'Trattoria Roma',
        'country': 'Italy',
        'country_code': '',
        'main_type': 'Restaurant',
        'all_types': 'Italian restaurant',
        'description_1': 'trattoria forno a legna antipasti',
        'description_2': '',
        'description_3': '',
        'price_range_simplified': 'high',
        'customer_reported_price_range': '',
        'reviews_tags': 'reservation',
        'characteristics': 'table service',
        'hotel_additional_informations': '',
        'website_title': '',
        'website_meta_description': '',
    })
    result = service._classify_row(row, MarketSegmenterOptions())
    assert result['fyre_market_segment_type0'] == 'horeca'
    assert result['fyre_market_segment_type1'] == 'table_service'
    assert result['resolved_country_code'] == 'IT'

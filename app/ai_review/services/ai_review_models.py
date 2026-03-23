from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AIReviewInput:
    canonical_context: dict[str, Any] = field(default_factory=dict)
    sources_used_map: dict[str, list[str]] = field(default_factory=dict)
    profile_name: str = ''
    enabled_capabilities: list[str] = field(default_factory=list)
    outlet_id: str = ''
    name: str = ''
    address: str = ''
    country: str = ''
    country_code: str = ''
    google_id: str = ''
    place_id: str = ''
    website: str = ''
    website_root_url: str = ''
    website_title: str = ''
    website_meta_description: str = ''
    menu_urls: list[str] = field(default_factory=list)
    photo_urls: list[str] = field(default_factory=list)
    social_links: list[str] = field(default_factory=list)
    segmentation_confidence: float | None = None
    initial_segments: list[str] = field(default_factory=list)
    main_type: str = ''
    all_types: str = ''
    descriptions: list[str] = field(default_factory=list)
    reviews_tags: str = ''
    characteristics: str = ''
    hotel_additional_informations: str = ''
    price_range: str = ''
    customer_reported_price_range: str = ''
    website_lang: str = ''
    segmentation_reasons: str = ''


@dataclass
class AIReviewResult:
    ai_review_status: str = 'pending'
    ai_confidence: str = ''
    ai_segment_suggested: str = ''
    ai_sources_used: str = ''
    ai_evidence_summary: str = ''
    ai_requires_human_review: str = 'yes'
    ai_input_pack_summary: str = ''
    ai_selected_for_review: str = 'yes'
    ai_detected_service_mode: str = ''
    ai_detected_cuisine: str = ''
    ai_detected_keywords: str = ''
    ai_detected_signals_json: str = ''
    ai_source_count: str = ''
    ai_web_fetch_status: str = ''
    ai_sources_fetched: str = ''
    ai_web_text_content: str = ''
    ai_menu_text_excerpt: str = ''
    ai_homepage_title: str = ''
    ai_homepage_meta_description: str = ''
    ai_action_profile: str = ''
    ai_enabled_capabilities: str = ''
    ai_capability_field_usage: str = ''
    ai_llm_status: str = ''
    ai_llm_reason: str = ''
    ai_llm_configured: str = ''
    ai_llm_live_ready: str = ''
    ai_llm_attempted: str = ''
    ai_llm_result_source: str = ''
    ai_llm_cost_estimated_eur: str = ''
    ai_llm_cost_actual_eur: str = ''
    ai_llm_budget_remaining_eur: str = ''
    ai_llm_cache_hit: str = ''
    ai_llm_calls_used: str = ''
    ai_llm_provider: str = ''
    ai_llm_model: str = ''
    ai_llm_result_json: str = ''
    ai_llm_raw_excerpt: str = ''

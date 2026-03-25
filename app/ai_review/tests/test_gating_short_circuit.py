import sys
import types
import unittest

fake_matcher_service = types.ModuleType('matcher.services.matcher_service')
fake_matcher_service.inspect_table_file = lambda *args, **kwargs: {}
sys.modules.setdefault('matcher.services.matcher_service', fake_matcher_service)

from ai_review.services.ai_review_models import AIReviewInput
from ai_review.services.ai_review_service import AIReviewService


class AIReviewGatingShortCircuitTests(unittest.TestCase):
    def test_high_confidence_rows_are_skipped_without_llm_or_web_processing(self):
        service = AIReviewService()
        review_input = AIReviewInput(
            profile_name='default',
            enabled_capabilities=['metadata_review', 'homepage_fetch', 'menu_fetch'],
            name='Confident Outlet',
            segmentation_confidence=0.91,
            initial_segments=['horeca', 'table_service'],
        )

        result = service._build_feature_extraction_result(review_input, threshold=0.65, only_low_confidence=True)

        self.assertEqual(result.ai_selected_for_review, 'no')
        self.assertEqual(result.ai_review_status, 'skipped')
        self.assertEqual(result.ai_segment_suggested, 'horeca > table_service')
        self.assertEqual(result.ai_segment_source, 'rules_initial')
        self.assertEqual(result.ai_web_fetch_status, 'skipped_high_confidence')
        self.assertEqual(result.ai_llm_status, 'skipped_high_confidence')
        self.assertEqual(result.ai_llm_attempted, 'no')
        self.assertEqual(result.ai_llm_calls_used, '0')
        self.assertIn('confidence=0.910', result.ai_evidence_summary)

    def test_job_threshold_is_the_only_threshold_used_for_skip_decision(self):
        service = AIReviewService()
        review_input = AIReviewInput(
            profile_name='default',
            enabled_capabilities=['metadata_review'],
            name='Medium Confidence Outlet',
            segmentation_confidence=0.81,
            initial_segments=['horeca'],
        )

        result = service._build_feature_extraction_result(review_input, threshold=0.85, only_low_confidence=True)

        self.assertEqual(result.ai_selected_for_review, 'yes')
        self.assertEqual(result.ai_review_status, 'selected')
        self.assertNotEqual(result.ai_llm_status, 'skipped_high_confidence')


if __name__ == '__main__':
    unittest.main()

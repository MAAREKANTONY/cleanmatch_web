import unittest

from ai_review.llm.llm_client import GuardedLLMClient


class LLMJsonParsingTests(unittest.TestCase):
    def test_parses_plain_json(self):
        payload = '{"service_mode":"table_service","segment_type0":"horeca"}'
        parsed = GuardedLLMClient._safe_json(payload)
        self.assertEqual(parsed['service_mode'], 'table_service')
        self.assertEqual(parsed['segment_type0'], 'horeca')

    def test_parses_markdown_fenced_json_without_newline(self):
        payload = '```json {"service_mode":"table_service","segment_type1":"table_service"}```'
        parsed = GuardedLLMClient._safe_json(payload)
        self.assertEqual(parsed['service_mode'], 'table_service')
        self.assertEqual(parsed['segment_type1'], 'table_service')

    def test_extracts_first_json_object_when_extra_text_exists(self):
        payload = 'Here is the result:\n```json\n{"service_mode":"table_service","segment_type2":"themed_dining_asian"}\n```\nThank you.'
        parsed = GuardedLLMClient._safe_json(payload)
        self.assertEqual(parsed['service_mode'], 'table_service')
        self.assertEqual(parsed['segment_type2'], 'themed_dining_asian')


    def test_recovers_truncated_markdown_json_with_structured_fields(self):
        payload = '```json {   "service_mode": "table_service",   "cuisine_type": "asian_multi",   "segment_type0": "horeca",   "segment_type1": "table_service",   "segment_type2": "themed_dining_asian",   "segment_type3": null,   "confidence": 0.78,   "evidence": "Asian street market with stalls and cocktails",   "requires_human_review": true,   "reasoning_short": "Multi-Asian concept with stalls but table-service cues'
        parsed = GuardedLLMClient._safe_json(payload)
        self.assertEqual(parsed['service_mode'], 'table_service')
        self.assertEqual(parsed['cuisine_type'], 'asian_multi')
        self.assertEqual(parsed['segment_type2'], 'themed_dining_asian')
        self.assertAlmostEqual(parsed['confidence'], 0.78)
        self.assertTrue(parsed['requires_human_review'])
        self.assertIn('partial_json_recovered', parsed['evidence'])

    def test_partial_single_field_json_is_recovered(self):
        payload = '```json {"service_mode":"table_service"'
        parsed = GuardedLLMClient._safe_json(payload)
        self.assertEqual(parsed['service_mode'], 'table_service')
        self.assertIn('partial_json_recovered', parsed['evidence'])


if __name__ == '__main__':
    unittest.main()

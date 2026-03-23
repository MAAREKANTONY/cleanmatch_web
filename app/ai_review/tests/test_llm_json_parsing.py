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

    def test_invalid_json_returns_guarded_fallback(self):
        payload = '```json {"service_mode":"table_service"'
        parsed = GuardedLLMClient._safe_json(payload)
        self.assertIn('invalid_json_response', parsed['evidence'])
        self.assertEqual(parsed['confidence'], 0.0)


if __name__ == '__main__':
    unittest.main()

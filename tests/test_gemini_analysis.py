import io
import json
import unittest
from unittest.mock import patch

from autoflow.gemini_analysis import (
    DEFAULT_MODEL,
    GEMINI_API_URL,
    GeminiAnalysisError,
    generate_overview,
    sanitize_profile,
)


class GeminiAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.profile = {
            "dataset": "private-dataset-name",
            "rows_loaded": 3,
            "rows_profiled": 3,
            "columns_count": 1,
            "exact": True,
            "sampled": False,
            "duplicate_rows": 0,
            "candidate_keys": ["email"],
            "columns": {
                "email": {
                    "inferred_type": "string",
                    "null_count": 0,
                    "examples": ["private@example.com"],
                    "top_values": {"private@example.com": 1},
                }
            },
        }

    def test_sanitization_excludes_dataset_name_and_cell_values(self):
        sanitized = sanitize_profile(self.profile)
        serialized = json.dumps(sanitized)
        self.assertNotIn("private", serialized)
        self.assertNotIn("examples", serialized)
        self.assertNotIn("top_values", serialized)
        self.assertEqual(sanitized["candidate_key_count"], 1)

    @patch("autoflow.gemini_analysis.urlopen")
    def test_sends_profile_only_to_gemini_and_extracts_text(self, mock_urlopen):
        mock_urlopen.return_value = io.BytesIO(
            b'{"candidates":[{"content":{"parts":[{"text":"Profile overview"}]}}]}'
        )
        overview = generate_overview(self.profile, "google_test_key", DEFAULT_MODEL)
        self.assertEqual(overview, "Profile overview")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            f"{GEMINI_API_URL}/{DEFAULT_MODEL}:generateContent",
        )
        self.assertEqual(request.get_header("X-goog-api-key"), "google_test_key")
        body = json.loads(request.data)
        serialized = json.dumps(body)
        self.assertNotIn("private", serialized)
        self.assertIn("systemInstruction", body)

    def test_requires_api_key(self):
        with self.assertRaisesRegex(ValueError, "Gemini API key"):
            generate_overview(self.profile, "", DEFAULT_MODEL)

    @patch("autoflow.gemini_analysis.urlopen")
    def test_reports_unexpected_response(self, mock_urlopen):
        mock_urlopen.return_value = io.BytesIO(b'{"candidates":[]}')
        with self.assertRaisesRegex(GeminiAnalysisError, "unexpected"):
            generate_overview(self.profile, "google_test_key", DEFAULT_MODEL)


if __name__ == "__main__":
    unittest.main()

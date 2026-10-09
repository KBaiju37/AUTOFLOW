"""Tests for the optional HTTP service. Never expose filesystem or connector controls."""
import unittest

try:
    from fastapi.testclient import TestClient
    from autoflow.service import create_app
    SERVICE_AVAILABLE = True
except ImportError:
    SERVICE_AVAILABLE = False


@unittest.skipUnless(SERVICE_AVAILABLE, "install autoflow[test] and autoflow[api]")
class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(create_app(max_records=2, max_columns=2, max_payload_bytes=2000))

    def test_health_and_connectors(self):
        self.assertEqual(self.client.get("/health").status_code, 200)
        response = self.client.get("/v1/connectors")
        self.assertEqual(response.status_code, 200)
        self.assertIn("csv", {x["type"] for x in response.json()["connectors"]})

    def test_profile_records(self):
        response = self.client.post("/v1/profile", json={"dataset_name": "sample", "records": [{"x": 1}, {"x": 2}]})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["dataset"], "sample")

    def test_validate_records_returns_pipeline_contract(self):
        response = self.client.post("/v1/validate", json={"dataset_name": "sample", "records": [{"x": 1}, {"x": 2}]})
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertIn("can_proceed", body)
        self.assertEqual(len(body["approved_records"]), 2)

    def test_record_limit(self):
        response = self.client.post("/v1/profile", json={"records": [{"x": 1}, {"x": 2}, {"x": 3}]})
        self.assertEqual(response.status_code, 413)

    def test_column_limit(self):
        response = self.client.post("/v1/profile", json={"records": [{"a": 1, "b": 2, "c": 3}]})
        self.assertEqual(response.status_code, 413)

    def test_payload_limit(self):
        response = self.client.post("/v1/profile", json={"records": [{"x": "z" * 3000}]})
        self.assertEqual(response.status_code, 413)


if __name__ == "__main__":
    unittest.main()

import unittest
from src.core.transform import deduplicate_latest, flatten_api_payload

class TransformTests(unittest.TestCase):
	def setUp(self):
		self.city = {"city_id": "pune", "city": "Pune", "country": "IN", "latitude": 18.52, "longitude": 73.85, "timezone":
"Asia/Kolkata"}
		self.payload = {"hourly": {"time": ["2026-09-01T10:00", "2026-09-01T11:00"], "pm10": [20.0, 21.0], "pm2_5": [10.0,
11.0], "european_aqi": [30, 32]}}
	def test_flatten(self):
		rows = flatten_api_payload(self.city, self.payload, "2026-09-01T12:00:00+00:00")
		self.assertEqual(2, len(rows))
		self.assertEqual("pune|2026-09-01T10:00", rows[0]["record_key"])
		self.assertEqual(10.0, rows[0]["pm2_5"])
	def test_deduplicate_keeps_latest(self):
		rows = flatten_api_payload(self.city, self.payload, "2026-09-01T12:00:00+00:00")
		newer = dict(rows[0], pm2_5=9.0, source_ingest_ts="2026-09-01T13:00:00+00:00")
		result = deduplicate_latest(rows + [newer])
		row = next(r for r in result if r["record_key"].endswith("10:00"))
		self.assertEqual(9.0, row["pm2_5"])
if __name__ == "__main__":
	unittest.main()

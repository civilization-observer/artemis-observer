from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


WORKER_PATH = Path(__file__).resolve().parents[1] / "scripts" / "launch_worker.py"
SPEC = importlib.util.spec_from_file_location("launch_worker", WORKER_PATH)
launch_worker = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(launch_worker)


class LaunchWorkerTests(unittest.TestCase):
    def test_semantic_payload_compare_ignores_operational_timestamps(self) -> None:
        left = {
            "generatedAt": "2026-05-02T09:00:00Z",
            "launches": [{"id": "a", "status": "go", "updatedAt": "old"}],
        }
        right = {
            "generatedAt": "2026-05-02T10:00:00Z",
            "launches": [{"id": "a", "status": "go", "updatedAt": "new"}],
        }

        self.assertTrue(launch_worker.semantically_equal(left, right, {"generatedAt", "updatedAt"}))

    def test_count_tle_satellites_counts_line_pairs(self) -> None:
        tle = "\n".join(
            [
                "ISS",
                "1 25544U 98067A   26122.50000000  .00000000  00000+0  00000+0 0  9991",
                "2 25544  51.6400 100.0000 0001000  10.0000 350.0000 15.50000000123456",
                "NOISE",
                "1 00001U 58002B   26122.50000000  .00000000  00000+0  00000+0 0  9991",
                "2 00001  34.2500 100.0000 0001000  10.0000 350.0000 10.50000000123456",
            ]
        )

        self.assertEqual(launch_worker.count_tle_satellites(tle), 2)
        self.assertEqual(launch_worker.active_tle_catalog_ids(tle), {"25544", "1"})

    def test_classify_launch_prefers_launch_library_status_ids(self) -> None:
        self.assertEqual(launch_worker.classify_launch({"status": {"id": 3}}), "success")
        self.assertEqual(launch_worker.classify_launch({"status": {"id": 4}}), "failure")
        self.assertEqual(launch_worker.classify_launch({"status": {"id": 8}}), "delayed")

    def test_satellite_profile_rules_mark_known_and_ambiguous_profiles(self) -> None:
        rules = launch_worker.load_satellite_profile_rules()
        qianfan = launch_worker.enriched_satellite_profile(
            {"OBJECT_NAME": "QIANFAN-1", "OBJECT_TYPE": "PAY", "OWNER": "PRC"},
            rules,
        )
        unknown = launch_worker.enriched_satellite_profile(
            {"OBJECT_NAME": "OBJECT A", "OBJECT_TYPE": "PAY", "OWNER": "US"},
            rules,
        )

        self.assertEqual(qianfan["operator"], "Shanghai Spacecom Satellite Technology (SSST)")
        self.assertFalse(qianfan["operatorAmbiguous"])
        self.assertEqual(unknown["operatorAmbiguous"], True)
        self.assertEqual(unknown["source"], "CelesTrak SATCAT geprüft; Betreiber nicht eindeutig")


    def test_satellite_group_stats_count_active_added_and_decayed(self) -> None:
        rules = launch_worker.load_satellite_profile_rules()
        records = [
            {
                "NORAD_CAT_ID": "1001",
                "OBJECT_NAME": "STARLINK-1001",
                "OBJECT_ID": "2026-001A",
                "OBJECT_TYPE": "PAY",
                "OWNER": "US",
                "LAUNCH_DATE": "2026-01-15",
            },
            {
                "NORAD_CAT_ID": "1002",
                "OBJECT_NAME": "STARLINK-1002",
                "OBJECT_ID": "2025-010A",
                "OBJECT_TYPE": "PAY",
                "OWNER": "US",
                "LAUNCH_DATE": "2025-12-01",
                "DECAY_DATE": "2026-02-20",
            },
        ]

        payload = launch_worker.build_satellite_group_stats(records, {"1001"}, rules)
        stats = payload["groups"]["starlink"]

        self.assertEqual(payload["total"]["activeCount"], 1)
        self.assertEqual(payload["total"]["decayedByDay"]["2026-02-20"], 1)
        self.assertEqual(stats["activeCount"], 1)
        self.assertEqual(stats["addedByDay"]["2026-01-15"], 1)
        self.assertEqual(stats["decayedByDay"]["2026-02-20"], 1)


if __name__ == "__main__":
    unittest.main()

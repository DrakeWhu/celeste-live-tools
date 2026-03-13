from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.pace_chart import (
    build_split_labels,
    build_attempt_cumulative_series,
    build_reference_series,
    collect_full_run_cumulative_reference_values,
    load_attempt_segments,
)
from src.schema import connect, initialize_schema


class PaceChartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "pace_chart_test.db"

        conn = connect(self.db_path)
        try:
            initialize_schema(conn)
            self._insert_attempt(
                conn,
                attempt_type="full_run_complete",
                rows=(
                    (1, 0, 0, "Forsaken City A / Start", 1000),
                    (1, 0, 1, "Forsaken City A / Crossing", 10000),
                    (1, 0, 2, "Forsaken City A / Chasm", 12000),
                ),
            )
            self._insert_attempt(
                conn,
                attempt_type="full_run_incomplete",
                rows=(
                    (1, 0, 0, "Forsaken City A / Start", 1500),
                    (1, 0, 1, "Forsaken City A / Crossing", 11000),
                ),
            )
            # This attempt hits a route key out of order -> should be ignored for references.
            self._insert_attempt(
                conn,
                attempt_type="full_run_complete",
                rows=(
                    (1, 0, 0, "Forsaken City A / Start", 999),
                    (1, 0, 2, "Forsaken City A / Chasm", 9999),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _insert_attempt(self, conn, *, attempt_type: str, rows) -> int:
        session_cursor = conn.execute(
            "INSERT INTO sessions (source_file) VALUES (?)",
            (f"/tmp/pace_chart_{attempt_type}_{id(rows)}.csv",),
        )
        session_id = int(session_cursor.lastrowid)
        attempt_cursor = conn.execute(
            "INSERT INTO attempts (session_id, attempt_index, attempt_type) VALUES (?, ?, ?)",
            (session_id, 0, attempt_type),
        )
        attempt_id = int(attempt_cursor.lastrowid)
        for seq_in_session, (chapter, mode, checkpoint_index, checkpoint_name, segment_ms) in enumerate(rows):
            conn.execute(
                """
                INSERT INTO segments (
                    session_id,
                    attempt_id,
                    seq_in_session,
                    chapter,
                    mode,
                    checkpoint_index,
                    checkpoint_name,
                    segment_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    attempt_id,
                    seq_in_session,
                    chapter,
                    mode,
                    checkpoint_index,
                    checkpoint_name,
                    segment_ms,
                ),
            )
        return attempt_id

    def test_build_attempt_cumulative_series_requires_route_order(self) -> None:
        rows = [
            ((1, 0, 0), 1000),
            ((1, 0, 2), 12000),
        ]
        series = build_attempt_cumulative_series(rows)
        self.assertIsNotNone(series)
        assert series is not None
        self.assertEqual(series.split_indices, [0])
        self.assertEqual(series.cumulative_ms, [1000])

    def test_reference_series_uses_real_attempt_prefix_cumulatives(self) -> None:
        conn = connect(self.db_path)
        try:
            values_by_split = collect_full_run_cumulative_reference_values(conn)
        finally:
            conn.close()

        # Split 0 has 2 valid samples: 1000, 1500
        self.assertEqual(sorted(values_by_split[0]), [999, 1000, 1500])
        # Split 1 has 2 valid samples: 11000, 12500
        self.assertEqual(sorted(values_by_split[1]), [11000, 12500])
        # Split 2 has only 1 valid sample (from the longer attempt): 23000
        self.assertEqual(values_by_split[2], [23000])

        pb, median = build_reference_series(values_by_split, split_indices=[0, 1, 2])
        self.assertEqual(pb, [999, 11000, 23000])
        self.assertEqual(median, [1000, 11750.0, 23000])

    def test_build_split_labels_uses_canonical_sections(self) -> None:
        labels = build_split_labels(split_indices=[0, 1, 2])
        self.assertEqual(labels, ["1A Start", "1A Crossing", "1A Chasm"])

    def test_load_attempt_segments_roundtrip(self) -> None:
        conn = connect(self.db_path)
        try:
            attempt_id = int(conn.execute("SELECT MIN(id) FROM attempts").fetchone()[0])
            rows = load_attempt_segments(conn, attempt_id=attempt_id)
        finally:
            conn.close()

        self.assertTrue(rows)


if __name__ == "__main__":
    unittest.main()

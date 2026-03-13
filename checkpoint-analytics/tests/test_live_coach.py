from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.live_coach import (
    build_segment_history_stats,
    get_cumulative_pace_stats,
    coach_feedback_for_segment,
    format_segment_feedback,
    get_run_prediction,
    get_segment_history_stats,
)
from src.schema import connect, initialize_schema


class LiveCoachTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "live_coach_test.db"
        self.session_counter = 0

        conn = connect(self.db_path)
        try:
            initialize_schema(conn)
            self._insert_sample(
                conn,
                attempt_type="full_run_complete",
                segment_ms=10000,
            )
            self._insert_sample(
                conn,
                attempt_type="full_run_incomplete",
                segment_ms=11000,
            )
            self._insert_sample(
                conn,
                attempt_type="chapter_practice",
                segment_ms=9000,
            )
            self._insert_sample(
                conn,
                attempt_type="checkpoint_grind",
                segment_ms=8000,
            )
            self._insert_sample(
                conn,
                attempt_type="full_run_complete",
                segment_ms=1000,
                checkpoint_index=99,
                checkpoint_name="Other Segment",
            )
            self._insert_sample(
                conn,
                attempt_type="full_run_complete",
                segment_ms=45000,
                chapter=7,
                mode=0,
                checkpoint_index=6,
                checkpoint_name="The Summit A / 3000M",
            )
            self._insert_sample(
                conn,
                attempt_type="full_run_incomplete",
                segment_ms=47000,
                chapter=7,
                mode=0,
                checkpoint_index=6,
                checkpoint_name="The Summit A / 3000M",
            )
            self._insert_attempt(
                conn,
                attempt_type="full_run_complete",
                rows=(
                    (1, 0, 0, "Forsaken City A / Start", 1000),
                    (1, 0, 1, "Forsaken City A / Crossing", 10000),
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
            conn.commit()
        finally:
            conn.close()

    def _insert_sample(
        self,
        conn,
        *,
        attempt_type: str,
        segment_ms: int,
        chapter: int = 1,
        mode: int = 0,
        checkpoint_index: int = 2,
        checkpoint_name: str = "Chasm",
    ) -> None:
        self.session_counter += 1
        source_file = f"/tmp/session_{self.session_counter}.csv"
        session_cursor = conn.execute(
            "INSERT INTO sessions (source_file) VALUES (?)",
            (source_file,),
        )
        session_id = int(session_cursor.lastrowid)
        attempt_cursor = conn.execute(
            "INSERT INTO attempts (session_id, attempt_index, attempt_type) VALUES (?, ?, ?)",
            (session_id, 0, attempt_type),
        )
        attempt_id = int(attempt_cursor.lastrowid)
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
                0,
                chapter,
                mode,
                checkpoint_index,
                checkpoint_name,
                segment_ms,
            ),
        )

    def _insert_attempt(self, conn, *, attempt_type: str, rows) -> None:
        self.session_counter += 1
        source_file = f"/tmp/session_{self.session_counter}.csv"
        session_cursor = conn.execute(
            "INSERT INTO sessions (source_file) VALUES (?)",
            (source_file,),
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

    def test_build_segment_history_stats_computes_expected_values(self) -> None:
        stats = build_segment_history_stats(
            chapter=1,
            mode=0,
            checkpoint_index=2,
            checkpoint_name="Chasm",
            current_segment_ms=9000,
            historical_segment_ms=[10000, 12000, 14000],
            context="any",
        )

        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertEqual(stats.sample_size, 3)
        self.assertEqual(stats.best_ms, 10000)
        self.assertEqual(stats.mean_ms, 12000.0)
        self.assertEqual(stats.median_ms, 12000)
        self.assertEqual(stats.delta_vs_best_ms, -1000)
        self.assertEqual(stats.delta_vs_mean_ms, -3000.0)
        self.assertEqual(stats.delta_vs_median_ms, -3000)
        self.assertTrue(stats.is_gold)

    def test_format_segment_feedback_with_historical_stats(self) -> None:
        stats = build_segment_history_stats(
            chapter=1,
            mode=0,
            checkpoint_index=2,
            checkpoint_name="Chasm",
            current_segment_ms=8500,
            historical_segment_ms=[9000, 10000, 11000],
            context="any",
        )

        feedback = format_segment_feedback(stats)

        self.assertEqual(
            feedback,
            "\n".join(
                (
                    "Segment: Chasm",
                    "Time: 8.50s",
                    "-0.50s vs best",
                    "-1.50s vs mean",
                    "-1.50s vs median",
                    "GOLD SPLIT",
                )
            ),
        )

    def test_format_segment_feedback_without_history(self) -> None:
        feedback = format_segment_feedback(
            None,
            checkpoint_name="Chasm",
            current_segment_ms=12420,
            context="full_run",
        )

        self.assertEqual(
            feedback,
            "\n".join(
                (
                    "Segment: Chasm",
                    "Time: 12.42s",
                    "No historical data for context=full_run",
                )
            ),
        )

    def test_get_segment_history_stats_filters_any_context(self) -> None:
        stats = get_segment_history_stats(
            chapter=1,
            mode=0,
            checkpoint_index=2,
            checkpoint_name="Chasm",
            current_segment_ms=9500,
            context="any",
            db_path=self.db_path,
        )

        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertEqual(stats.sample_size, 4)
        self.assertEqual(stats.best_ms, 8000)
        self.assertEqual(stats.mean_ms, 9500.0)
        self.assertEqual(stats.median_ms, 9500.0)

    def test_get_segment_history_stats_filters_full_run_context(self) -> None:
        stats = get_segment_history_stats(
            chapter=1,
            mode=0,
            checkpoint_index=2,
            checkpoint_name="Chasm",
            current_segment_ms=10500,
            context="full_run",
            db_path=self.db_path,
        )

        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertEqual(stats.sample_size, 2)
        self.assertEqual(stats.best_ms, 10000)
        self.assertEqual(stats.mean_ms, 10500.0)
        self.assertEqual(stats.median_ms, 10500.0)
        self.assertFalse(stats.is_gold)

    def test_get_segment_history_stats_filters_chapter_practice_context(self) -> None:
        stats = get_segment_history_stats(
            chapter=1,
            mode=0,
            checkpoint_index=2,
            checkpoint_name="Chasm",
            current_segment_ms=8500,
            context="chapter_practice",
            db_path=self.db_path,
        )

        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertEqual(stats.sample_size, 1)
        self.assertEqual(stats.best_ms, 9000)
        self.assertEqual(stats.mean_ms, 9000.0)
        self.assertEqual(stats.median_ms, 9000)
        self.assertTrue(stats.is_gold)

    def test_coach_feedback_for_segment_uses_temporary_database(self) -> None:
        feedback = coach_feedback_for_segment(
            chapter=1,
            mode=0,
            checkpoint_index=2,
            checkpoint_name="Chasm",
            segment_ms=10500,
            context="full_run",
            db_path=self.db_path,
        )

        self.assertEqual(
            feedback,
            "\n".join(
                (
                    "Segment: Chasm",
                    "Time: 10.50s",
                    "+0.50s vs best",
                    "+0.00s vs mean",
                    "+0.00s vs median",
                )
            ),
        )

    def test_get_run_prediction_uses_full_run_remaining_segment_medians(self) -> None:
        prediction = get_run_prediction(
            chapter=7,
            mode=0,
            checkpoint_index=5,
            current_cumulative_ms=100000,
            db_path=self.db_path,
        )

        self.assertIsNotNone(prediction)
        assert prediction is not None
        self.assertEqual(prediction.remaining_segment_count, 1)
        self.assertEqual(prediction.missing_segment_count, 0)
        self.assertEqual(prediction.predicted_final_ms, 146000)
        self.assertEqual(prediction.best_possible_ms, 145000)

    def test_get_cumulative_pace_stats_uses_real_full_run_attempt_prefixes(self) -> None:
        pace = get_cumulative_pace_stats(
            chapter=1,
            mode=0,
            checkpoint_index=1,
            current_cumulative_ms=12000,
            db_path=self.db_path,
        )

        self.assertIsNotNone(pace)
        assert pace is not None
        self.assertEqual(pace.sample_size, 2)
        self.assertEqual(pace.pb_cumulative_ms, 11000)
        self.assertEqual(pace.median_cumulative_ms, 11750.0)
        self.assertEqual(pace.delta_vs_pb_ms, 1000)
        self.assertEqual(pace.delta_vs_median_ms, 250.0)

    def test_get_run_prediction_is_unavailable_when_remaining_history_is_missing(self) -> None:
        prediction = get_run_prediction(
            chapter=7,
            mode=0,
            checkpoint_index=4,
            current_cumulative_ms=80000,
            db_path=self.db_path,
        )

        self.assertIsNotNone(prediction)
        assert prediction is not None
        self.assertEqual(prediction.remaining_segment_count, 2)
        self.assertEqual(prediction.missing_segment_count, 1)
        self.assertIsNone(prediction.predicted_final_ms)
        self.assertIsNone(prediction.best_possible_ms)

    def test_coach_feedback_for_segment_includes_prediction_when_available(self) -> None:
        feedback = coach_feedback_for_segment(
            chapter=7,
            mode=0,
            checkpoint_index=5,
            checkpoint_name="The Summit A / 2500M",
            segment_ms=30000,
            cumulative_ms=100000,
            context="full_run",
            db_path=self.db_path,
        )

        self.assertEqual(
            feedback,
            "\n".join(
                (
                    "Segment: The Summit A / 2500M",
                    "Time: 30.00s",
                    "No historical data for context=full_run",
                    "Predicted final: 2:26.000",
                    "Best possible: 2:25.000",
                )
            ),
        )

    def test_coach_feedback_for_segment_includes_cumulative_pace_for_full_run(self) -> None:
        feedback = coach_feedback_for_segment(
            chapter=1,
            mode=0,
            checkpoint_index=1,
            checkpoint_name="Forsaken City A / Crossing",
            segment_ms=9500,
            cumulative_ms=12000,
            context="full_run",
            db_path=self.db_path,
        )

        self.assertEqual(
            feedback,
            "\n".join(
                (
                    "Segment: Forsaken City A / Crossing",
                    "Time: 9.50s",
                    "-0.50s vs best",
                    "-1.00s vs mean",
                    "-1.00s vs median",
                    "Predicted final: unavailable (29 missing segments)",
                    "PB pace: +1.00s",
                    "Median pace: +0.25s",
                    "GOLD SPLIT",
                )
            ),
        )


if __name__ == "__main__":
    unittest.main()

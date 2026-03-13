"""Live segment coaching against historical SQLite segment data."""

from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .schema import DEFAULT_DB_PATH
from .seed_checkpoints import CHECKPOINT_SPEC

CoachContext = Literal["any", "full_run", "chapter_practice"]
SegmentKey = tuple[int, int, int]

CONTEXT_FILTERS: dict[CoachContext, tuple[str, tuple[object, ...]]] = {
    "any": ("", ()),
    "full_run": (
        "AND a.attempt_type IN (?, ?)",
        ("full_run_complete", "full_run_incomplete"),
    ),
    "chapter_practice": (
        "AND a.attempt_type = ?",
        ("chapter_practice",),
    ),
}
FULL_RUN_ATTEMPT_TYPES = ("full_run_complete", "full_run_incomplete")


@dataclass(frozen=True)
class SegmentHistoryStats:
    chapter: int
    mode: int
    checkpoint_index: int
    checkpoint_name: str
    context: CoachContext
    current_segment_ms: int
    sample_size: int
    best_ms: int
    mean_ms: float
    median_ms: float
    delta_vs_best_ms: int
    delta_vs_mean_ms: float
    delta_vs_median_ms: float
    is_gold: bool


@dataclass(frozen=True)
class RunPrediction:
    chapter: int
    mode: int
    checkpoint_index: int
    current_cumulative_ms: int
    remaining_segment_count: int
    missing_segment_count: int
    predicted_final_ms: int | None
    best_possible_ms: int | None


@dataclass(frozen=True)
class CumulativePaceStats:
    chapter: int
    mode: int
    checkpoint_index: int
    current_cumulative_ms: int
    sample_size: int
    pb_cumulative_ms: int
    median_cumulative_ms: float
    delta_vs_pb_ms: int
    delta_vs_median_ms: float


def _route_segment_keys(
    chapter: int,
    mode: int,
    *,
    limit: int | None = None,
) -> tuple[SegmentKey, ...]:
    names = CHECKPOINT_SPEC[(chapter, mode)]
    count = len(names) if limit is None else min(limit, len(names))
    return tuple((chapter, mode, checkpoint_index) for checkpoint_index in range(count))


# Full-run prediction follows the current any% route, including the 5A -> 5B transition.
FULL_RUN_PREDICTION_ROUTE: tuple[SegmentKey, ...] = (
    _route_segment_keys(1, 0)
    + _route_segment_keys(2, 0)
    + _route_segment_keys(3, 0)
    + _route_segment_keys(4, 0)
    + _route_segment_keys(5, 0, limit=2)
    + _route_segment_keys(5, 1)
    + _route_segment_keys(6, 0)
    + _route_segment_keys(7, 0)
)
FULL_RUN_PREDICTION_ROUTE_INDEX = {
    key: route_index for route_index, key in enumerate(FULL_RUN_PREDICTION_ROUTE)
}
FULL_RUN_ROUTE_KEY_SET = set(FULL_RUN_PREDICTION_ROUTE)


def _validate_context(context: str) -> CoachContext:
    if context not in CONTEXT_FILTERS:
        allowed = ", ".join(CONTEXT_FILTERS)
        raise ValueError(f"Unsupported coach context '{context}'. Allowed: {allowed}")
    return context


def _load_historical_segment_values(
    conn: sqlite3.Connection,
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    context: CoachContext,
) -> list[int]:
    where_filter, filter_params = CONTEXT_FILTERS[context]
    cursor = conn.execute(
        f"""
        SELECT s.segment_ms
        FROM segments AS s
        LEFT JOIN attempts AS a
          ON a.id = s.attempt_id
        WHERE s.chapter = ?
          AND s.mode = ?
          AND s.checkpoint_index = ?
          {where_filter}
        ORDER BY s.segment_ms ASC
        """,
        (chapter, mode, checkpoint_index, *filter_params),
    )
    return [int(segment_ms) for (segment_ms,) in cursor.fetchall()]


def _remaining_full_run_segment_keys(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
) -> tuple[SegmentKey, ...] | None:
    route_index = FULL_RUN_PREDICTION_ROUTE_INDEX.get((chapter, mode, checkpoint_index))
    if route_index is None:
        return None
    return FULL_RUN_PREDICTION_ROUTE[route_index + 1 :]


def _extract_attempt_cumulative_ms(
    attempt_rows: list[tuple[SegmentKey, int]],
    expected_prefix: tuple[SegmentKey, ...],
) -> int | None:
    if len(attempt_rows) < len(expected_prefix):
        return None
    observed_prefix = tuple(key for key, _segment_ms in attempt_rows[: len(expected_prefix)])
    if observed_prefix != expected_prefix:
        return None
    return sum(segment_ms for _key, segment_ms in attempt_rows[: len(expected_prefix)])


def _load_full_run_cumulative_values(
    conn: sqlite3.Connection,
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
) -> list[int]:
    route_index = FULL_RUN_PREDICTION_ROUTE_INDEX.get((chapter, mode, checkpoint_index))
    if route_index is None:
        return []

    expected_prefix = FULL_RUN_PREDICTION_ROUTE[: route_index + 1]
    cursor = conn.execute(
        """
        SELECT s.attempt_id,
               s.chapter,
               s.mode,
               s.checkpoint_index,
               s.segment_ms
        FROM segments AS s
        JOIN attempts AS a
          ON a.id = s.attempt_id
        WHERE a.attempt_type IN (?, ?)
        ORDER BY s.attempt_id ASC, s.seq_in_session ASC
        """,
        FULL_RUN_ATTEMPT_TYPES,
    )

    cumulative_values: list[int] = []
    current_attempt_id: int | None = None
    attempt_rows: list[tuple[SegmentKey, int]] = []

    def flush_attempt() -> None:
        if not attempt_rows:
            return
        cumulative_ms = _extract_attempt_cumulative_ms(attempt_rows, expected_prefix)
        if cumulative_ms is not None:
            cumulative_values.append(cumulative_ms)

    for attempt_id, row_chapter, row_mode, row_checkpoint_index, segment_ms in cursor:
        attempt_id = int(attempt_id) if attempt_id is not None else None
        if current_attempt_id is None:
            current_attempt_id = attempt_id
        elif attempt_id != current_attempt_id:
            flush_attempt()
            attempt_rows = []
            current_attempt_id = attempt_id

        key = (int(row_chapter), int(row_mode), int(row_checkpoint_index))
        if key in FULL_RUN_ROUTE_KEY_SET:
            attempt_rows.append((key, int(segment_ms)))

    flush_attempt()
    return cumulative_values


def build_segment_history_stats(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    checkpoint_name: str,
    current_segment_ms: int,
    historical_segment_ms: list[int],
    context: CoachContext,
) -> SegmentHistoryStats | None:
    if not historical_segment_ms:
        return None

    best_ms = min(historical_segment_ms)
    mean_ms = statistics.fmean(historical_segment_ms)
    median_ms = statistics.median(historical_segment_ms)

    return SegmentHistoryStats(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
        checkpoint_name=checkpoint_name,
        context=context,
        current_segment_ms=current_segment_ms,
        sample_size=len(historical_segment_ms),
        best_ms=best_ms,
        mean_ms=mean_ms,
        median_ms=median_ms,
        delta_vs_best_ms=current_segment_ms - best_ms,
        delta_vs_mean_ms=current_segment_ms - mean_ms,
        delta_vs_median_ms=current_segment_ms - median_ms,
        is_gold=current_segment_ms < best_ms,
    )


def build_run_prediction(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    current_cumulative_ms: int,
    remaining_segment_values: list[list[int]],
) -> RunPrediction:
    missing_segment_count = sum(1 for values in remaining_segment_values if not values)
    remaining_segment_count = len(remaining_segment_values)
    safe_cumulative_ms = max(0, current_cumulative_ms)

    if missing_segment_count:
        return RunPrediction(
            chapter=chapter,
            mode=mode,
            checkpoint_index=checkpoint_index,
            current_cumulative_ms=safe_cumulative_ms,
            remaining_segment_count=remaining_segment_count,
            missing_segment_count=missing_segment_count,
            predicted_final_ms=None,
            best_possible_ms=None,
        )

    remaining_median_ms = sum(statistics.median(values) for values in remaining_segment_values)
    remaining_best_ms = sum(min(values) for values in remaining_segment_values)
    return RunPrediction(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
        current_cumulative_ms=safe_cumulative_ms,
        remaining_segment_count=remaining_segment_count,
        missing_segment_count=0,
        predicted_final_ms=int(round(safe_cumulative_ms + remaining_median_ms)),
        best_possible_ms=safe_cumulative_ms + remaining_best_ms,
    )


def build_cumulative_pace_stats(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    current_cumulative_ms: int,
    historical_cumulative_ms: list[int],
) -> CumulativePaceStats | None:
    if not historical_cumulative_ms:
        return None

    safe_cumulative_ms = max(0, current_cumulative_ms)
    pb_cumulative_ms = min(historical_cumulative_ms)
    median_cumulative_ms = statistics.median(historical_cumulative_ms)
    return CumulativePaceStats(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
        current_cumulative_ms=safe_cumulative_ms,
        sample_size=len(historical_cumulative_ms),
        pb_cumulative_ms=pb_cumulative_ms,
        median_cumulative_ms=median_cumulative_ms,
        delta_vs_pb_ms=safe_cumulative_ms - pb_cumulative_ms,
        delta_vs_median_ms=safe_cumulative_ms - median_cumulative_ms,
    )


def get_segment_history_stats(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    checkpoint_name: str,
    current_segment_ms: int,
    context: CoachContext = "any",
    db_path: Path = DEFAULT_DB_PATH,
) -> SegmentHistoryStats | None:
    validated_context = _validate_context(context)
    resolved_db_path = Path(db_path).expanduser().resolve()
    if not resolved_db_path.exists():
        return None

    try:
        conn = sqlite3.connect(resolved_db_path)
        try:
            values = _load_historical_segment_values(
                conn,
                chapter=chapter,
                mode=mode,
                checkpoint_index=checkpoint_index,
                context=validated_context,
            )
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None

    return build_segment_history_stats(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
        checkpoint_name=checkpoint_name,
        current_segment_ms=current_segment_ms,
        historical_segment_ms=values,
        context=validated_context,
    )


def get_run_prediction(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    current_cumulative_ms: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> RunPrediction | None:
    remaining_keys = _remaining_full_run_segment_keys(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
    )
    if remaining_keys is None:
        return None

    resolved_db_path = Path(db_path).expanduser().resolve()
    if not resolved_db_path.exists():
        return None

    try:
        conn = sqlite3.connect(resolved_db_path)
        try:
            remaining_values = [
                _load_historical_segment_values(
                    conn,
                    chapter=remaining_chapter,
                    mode=remaining_mode,
                    checkpoint_index=remaining_checkpoint_index,
                    context="full_run",
                )
                for remaining_chapter, remaining_mode, remaining_checkpoint_index in remaining_keys
            ]
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None

    return build_run_prediction(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
        current_cumulative_ms=current_cumulative_ms,
        remaining_segment_values=remaining_values,
    )


def get_cumulative_pace_stats(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    current_cumulative_ms: int,
    db_path: Path = DEFAULT_DB_PATH,
) -> CumulativePaceStats | None:
    resolved_db_path = Path(db_path).expanduser().resolve()
    if not resolved_db_path.exists():
        return None

    try:
        conn = sqlite3.connect(resolved_db_path)
        try:
            values = _load_full_run_cumulative_values(
                conn,
                chapter=chapter,
                mode=mode,
                checkpoint_index=checkpoint_index,
            )
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None

    return build_cumulative_pace_stats(
        chapter=chapter,
        mode=mode,
        checkpoint_index=checkpoint_index,
        current_cumulative_ms=current_cumulative_ms,
        historical_cumulative_ms=values,
    )


def _format_seconds(milliseconds: float) -> str:
    return f"{milliseconds / 1000:.2f}s"


def _format_signed_seconds(milliseconds: float) -> str:
    seconds = milliseconds / 1000
    return f"{seconds:+.2f}s"


def _format_run_time(milliseconds: int) -> str:
    total_ms = max(0, int(round(milliseconds)))
    total_seconds, ms = divmod(total_ms, 1000)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}.{ms:03d}"
    return f"{minutes}:{seconds:02d}.{ms:03d}"


def format_segment_feedback(
    stats: SegmentHistoryStats | None,
    *,
    checkpoint_name: str | None = None,
    current_segment_ms: int | None = None,
    context: CoachContext | None = None,
    prediction: RunPrediction | None = None,
    cumulative_pace: CumulativePaceStats | None = None,
) -> str:
    if stats is None:
        if checkpoint_name is None or current_segment_ms is None or context is None:
            raise ValueError(
                "checkpoint_name, current_segment_ms, and context are required when stats is None"
            )
        lines = [
            f"Segment: {checkpoint_name}",
            f"Time: {_format_seconds(current_segment_ms)}",
            f"No historical data for context={context}",
        ]
        if prediction is not None:
            if prediction.predicted_final_ms is None:
                lines.append(
                    "Predicted final: unavailable "
                    f"({prediction.missing_segment_count} missing segments)"
                )
            else:
                lines.append(f"Predicted final: {_format_run_time(prediction.predicted_final_ms)}")
                if prediction.best_possible_ms is not None:
                    lines.append(f"Best possible: {_format_run_time(prediction.best_possible_ms)}")
        if cumulative_pace is not None:
            lines.append(f"PB pace: {_format_signed_seconds(cumulative_pace.delta_vs_pb_ms)}")
            lines.append(f"Median pace: {_format_signed_seconds(cumulative_pace.delta_vs_median_ms)}")
        return "\n".join(lines)

    lines = [
        f"Segment: {stats.checkpoint_name}",
        f"Time: {_format_seconds(stats.current_segment_ms)}",
        f"{_format_signed_seconds(stats.delta_vs_best_ms)} vs best",
        f"{_format_signed_seconds(stats.delta_vs_mean_ms)} vs mean",
        f"{_format_signed_seconds(stats.delta_vs_median_ms)} vs median",
    ]
    if prediction is not None:
        if prediction.predicted_final_ms is None:
            lines.append(
                "Predicted final: unavailable "
                f"({prediction.missing_segment_count} missing segments)"
            )
        else:
            lines.append(f"Predicted final: {_format_run_time(prediction.predicted_final_ms)}")
            if prediction.best_possible_ms is not None:
                lines.append(f"Best possible: {_format_run_time(prediction.best_possible_ms)}")
    if cumulative_pace is not None:
        lines.append(f"PB pace: {_format_signed_seconds(cumulative_pace.delta_vs_pb_ms)}")
        lines.append(f"Median pace: {_format_signed_seconds(cumulative_pace.delta_vs_median_ms)}")
    if stats.is_gold:
        lines.append("GOLD SPLIT")
    return "\n".join(lines)


def coach_feedback_for_segment(
    *,
    chapter: int,
    mode: int,
    checkpoint_index: int,
    checkpoint_name: str,
    segment_ms: int,
    cumulative_ms: int | None = None,
    context: CoachContext = "any",
    db_path: Path = DEFAULT_DB_PATH,
) -> str | None:
    try:
        validated_context = _validate_context(context)
        stats = get_segment_history_stats(
            chapter=chapter,
            mode=mode,
            checkpoint_index=checkpoint_index,
            checkpoint_name=checkpoint_name,
            current_segment_ms=segment_ms,
            context=validated_context,
            db_path=db_path,
        )
        prediction = None
        cumulative_pace = None
        if validated_context == "full_run" and cumulative_ms is not None:
            prediction = get_run_prediction(
                chapter=chapter,
                mode=mode,
                checkpoint_index=checkpoint_index,
                current_cumulative_ms=cumulative_ms,
                db_path=db_path,
            )
            cumulative_pace = get_cumulative_pace_stats(
                chapter=chapter,
                mode=mode,
                checkpoint_index=checkpoint_index,
                current_cumulative_ms=cumulative_ms,
                db_path=db_path,
            )
        return format_segment_feedback(
            stats,
            checkpoint_name=checkpoint_name,
            current_segment_ms=segment_ms,
            context=validated_context,
            prediction=prediction,
            cumulative_pace=cumulative_pace,
        )
    except (OSError, sqlite3.Error, statistics.StatisticsError, ValueError):
        return None

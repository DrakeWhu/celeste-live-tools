"""Simple stats CLI for Celeste checkpoint analytics."""

from __future__ import annotations

import argparse
import math
import sqlite3
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

from .schema import DEFAULT_DB_PATH, connect, initialize_schema


def format_table(headers: Iterable[str], rows: Iterable[Sequence[object]]) -> str:
    headers = list(headers)
    string_rows = [list(map(str, row)) for row in rows]
    widths = [len(h) for h in headers]
    for row in string_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def format_row(values: Iterable[str]) -> str:
        return " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(values))

    lines = [format_row(headers), " | ".join("-" * width for width in widths)]
    lines.extend(format_row(row) for row in string_rows)
    return "\n".join(lines)


def show_sessions(conn, limit: int) -> None:
    cursor = conn.execute(
        """
        SELECT id,
               imported_at,
               started_at,
               ended_at,
               session_type,
               category,
               notes,
               source_file
        FROM sessions
        ORDER BY imported_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cursor.fetchall()
    if not rows:
        print("No sessions imported.")
        return

    headers = (
        "id",
        "imported_at",
        "started_at",
        "ended_at",
        "session_type",
        "category",
        "notes",
        "source_file",
    )
    display_rows = [
        (
            session_id,
            imported_at,
            started_at,
            ended_at,
            session_type or "",
            category or "",
            notes or "",
            source_file,
        )
        for (
            session_id,
            imported_at,
            started_at,
            ended_at,
            session_type,
            category,
            notes,
            source_file,
        ) in rows
    ]
    print(format_table(headers, display_rows))


def show_attempts(conn, limit: int | None) -> None:
    sql = """
        SELECT a.session_id,
               a.attempt_index,
               a.attempt_type,
               COUNT(s.id) AS segment_count,
               a.started_at,
               a.ended_at,

               (SELECT chapter
                  FROM segments AS s2
                 WHERE s2.attempt_id = a.id
                 ORDER BY s2.seq_in_session ASC
                 LIMIT 1) AS first_chapter,
               (SELECT mode
                  FROM segments AS s2
                 WHERE s2.attempt_id = a.id
                 ORDER BY s2.seq_in_session ASC
                 LIMIT 1) AS first_mode,
               (SELECT checkpoint_index
                  FROM segments AS s2
                 WHERE s2.attempt_id = a.id
                 ORDER BY s2.seq_in_session ASC
                 LIMIT 1) AS first_checkpoint_index,
               (SELECT checkpoint_name
                  FROM segments AS s2
                 WHERE s2.attempt_id = a.id
                 ORDER BY s2.seq_in_session ASC
                 LIMIT 1) AS first_checkpoint_name,

               (SELECT chapter
                  FROM segments AS s3
                 WHERE s3.attempt_id = a.id
                 ORDER BY s3.seq_in_session DESC
                 LIMIT 1) AS last_chapter,
               (SELECT mode
                  FROM segments AS s3
                 WHERE s3.attempt_id = a.id
                 ORDER BY s3.seq_in_session DESC
                 LIMIT 1) AS last_mode,
               (SELECT checkpoint_index
                  FROM segments AS s3
                 WHERE s3.attempt_id = a.id
                 ORDER BY s3.seq_in_session DESC
                 LIMIT 1) AS last_checkpoint_index,
               (SELECT checkpoint_name
                  FROM segments AS s3
                 WHERE s3.attempt_id = a.id
                 ORDER BY s3.seq_in_session DESC
                 LIMIT 1) AS last_checkpoint_name,

               (SELECT group_concat(chapter, ",")
                  FROM (
                        SELECT s4.chapter AS chapter
                          FROM segments AS s4
                         WHERE s4.attempt_id = a.id
                         GROUP BY s4.chapter
                         ORDER BY MIN(s4.seq_in_session)
                  )) AS chapter_path
          FROM attempts AS a
          LEFT JOIN segments AS s
            ON s.attempt_id = a.id
         GROUP BY a.id
         ORDER BY a.session_id DESC, a.attempt_index DESC
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        sql = sql + "\n        LIMIT ?"
        params = (limit,)

    try:
        cursor = conn.execute(sql, params)
        rows = cursor.fetchall()
    except sqlite3.OperationalError as exc:
        print(f"Attempt inspection unavailable: {exc}")
        return
    if not rows:
        print("No attempts found.")
        return

    headers = (
        "session_id",
        "attempt_index",
        "attempt_type",
        "segment_count",
        "started_at",
        "ended_at",
        "first_chapter",
        "first_mode",
        "first_checkpoint_index",
        "first_checkpoint_name",
        "last_chapter",
        "last_mode",
        "last_checkpoint_index",
        "last_checkpoint_name",
        "chapter_path",
    )
    display_rows = [
        (
            session_id,
            attempt_index,
            attempt_type or "",
            segment_count,
            started_at,
            ended_at,
            first_chapter,
            first_mode,
            first_checkpoint_index,
            first_checkpoint_name,
            last_chapter,
            last_mode,
            last_checkpoint_index,
            last_checkpoint_name,
            chapter_path or "",
        )
        for (
            session_id,
            attempt_index,
            attempt_type,
            segment_count,
            started_at,
            ended_at,
            first_chapter,
            first_mode,
            first_checkpoint_index,
            first_checkpoint_name,
            last_chapter,
            last_mode,
            last_checkpoint_index,
            last_checkpoint_name,
            chapter_path,
        ) in rows
    ]
    print(format_table(headers, display_rows))


Key = Tuple[int, int, int]


def collect_segment_samples(conn) -> Dict[Key, List[int]]:
    """Return sorted segment durations per (chapter, mode, checkpoint_index)."""
    cursor = conn.execute(
        """
        SELECT chapter,
               mode,
               checkpoint_index,
               segment_ms
        FROM segments
        WHERE is_complete = 1
        ORDER BY chapter, mode, checkpoint_index, segment_ms
        """
    )
    samples: Dict[Key, List[int]] = {}
    for chapter, mode, checkpoint_index, segment_ms in cursor:
        key = (chapter, mode, checkpoint_index)
        samples.setdefault(key, []).append(segment_ms)
    return samples


def sample_stddev(n: int, sum_ms: float, sum_sq_ms: float) -> float:
    """Return Bessel-corrected sample stddev; n <= 1 yields 0.0 for convenience."""
    if n <= 1:
        return 0.0
    mean_component = (sum_ms * sum_ms) / n
    variance = (sum_sq_ms - mean_component) / (n - 1)
    if variance <= 0:
        return 0.0
    return math.sqrt(variance)


def interpolate_percentile(sorted_values: List[int], percentile: float) -> float:
    """Linear-interpolate percentile on sorted samples."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = (len(sorted_values) - 1) * (percentile / 100.0)
    lower_idx = math.floor(position)
    upper_idx = math.ceil(position)
    if lower_idx == upper_idx:
        return float(sorted_values[int(position)])
    lower_value = sorted_values[lower_idx]
    upper_value = sorted_values[upper_idx]
    weight = position - lower_idx
    return lower_value + (upper_value - lower_value) * weight


SummaryRow = Tuple[
    int,
    int,
    int,
    str,
    int,
    float,
    float,
    int,
    float,
    float,
    float,
    float,
    float,
    int,
    float,
]


def build_checkpoint_summary_rows(conn, canonical: bool) -> List[SummaryRow]:
    samples_by_key = collect_segment_samples(conn)
    if not samples_by_key:
        return []

    if canonical:
        # Canonical naming affects only display labeling; grouping remains chapter/mode/index.
        cursor = conn.execute(
            """
            SELECT s.chapter,
                   s.mode,
                   s.checkpoint_index,
                   COALESCE(MIN(a.canonical_name), MIN(d.canonical_name), MIN(s.checkpoint_name)) AS display_name,
                   COUNT(*) AS n,
                   SUM(s.segment_ms) AS sum_ms,
                   SUM(s.segment_ms * s.segment_ms) AS sum_sq_ms,
                   MIN(s.segment_ms) AS min_ms,
                   MAX(s.segment_ms) AS max_ms
            FROM segments AS s
            LEFT JOIN checkpoint_aliases AS a
                ON a.alias_name = s.checkpoint_name
            LEFT JOIN checkpoint_defs AS d
                ON d.chapter = s.chapter
               AND d.mode = s.mode
               AND d.checkpoint_index = s.checkpoint_index
            WHERE s.is_complete = 1
            GROUP BY s.chapter, s.mode, s.checkpoint_index
            ORDER BY s.chapter, s.mode, s.checkpoint_index
            """
        )
    else:
        # MIN(checkpoint_name) serves as a stable display fallback when multiple raw names appear.
        cursor = conn.execute(
            """
            SELECT chapter,
                   mode,
                   checkpoint_index,
                   MIN(checkpoint_name) AS checkpoint_name,
                   COUNT(*) AS n,
                   SUM(segment_ms) AS sum_ms,
                   SUM(segment_ms * segment_ms) AS sum_sq_ms,
                   MIN(segment_ms) AS min_ms,
                   MAX(segment_ms) AS max_ms
            FROM segments
            WHERE is_complete = 1
            GROUP BY chapter, mode, checkpoint_index
            ORDER BY chapter, mode, checkpoint_index
            """
        )

    percentiles = (25, 50, 75, 90, 95)
    rows: List[SummaryRow] = []
    for chapter, mode, checkpoint_index, name, n, sum_ms, sum_sq_ms, min_ms, max_ms in cursor:
        key = (chapter, mode, checkpoint_index)
        values = samples_by_key.get(key)
        if not values:
            continue
        n_int = int(n)
        sum_float = float(sum_ms)
        sum_sq_float = float(sum_sq_ms)
        mean_ms = sum_float / n_int if n_int else 0.0
        stddev = sample_stddev(n_int, sum_float, sum_sq_float)
        cv = stddev / mean_ms if mean_ms else 0.0
        percentile_values = [interpolate_percentile(values, p) for p in percentiles]
        rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                n_int,
                mean_ms,
                stddev,
                int(min_ms),
                percentile_values[0],
                percentile_values[1],
                percentile_values[2],
                percentile_values[3],
                percentile_values[4],
                int(max_ms),
                cv,
            )
        )
    return rows


def show_checkpoint_summary(conn) -> None:
    rows = build_checkpoint_summary_rows(conn, canonical=False)
    if not rows:
        print("No segment data available.")
        return

    headers = (
        "chapter",
        "mode",
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "stddev_ms",
        "min_ms",
        "p25_ms",
        "median_ms",
        "p75_ms",
        "p90_ms",
        "p95_ms",
        "max_ms",
        "cv",
    )

    display_rows = []
    for row in rows:
        (
            chapter,
            mode,
            checkpoint_index,
            name,
            n,
            mean_ms,
            stddev_ms,
            min_ms,
            p25,
            median,
            p75,
            p90,
            p95,
            max_ms,
            cv,
        ) = row
        # MIN(checkpoint_name) serves purely as a display fallback when multiple raw names exist.
        display_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                n,
                f"{mean_ms:.2f}",
                f"{stddev_ms:.2f}",
                min_ms,
                int(round(p25)),
                int(round(median)),
                int(round(p75)),
                int(round(p90)),
                int(round(p95)),
                max_ms,
                f"{cv:.3f}",
            )
        )

    print(format_table(headers, display_rows))


def show_canonical_checkpoint_summary(conn) -> None:
    rows = build_checkpoint_summary_rows(conn, canonical=True)
    if not rows:
        print("No segment data available.")
        return

    headers = (
        "chapter",
        "mode",
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "stddev_ms",
        "min_ms",
        "p25_ms",
        "median_ms",
        "p75_ms",
        "p90_ms",
        "p95_ms",
        "max_ms",
        "cv",
    )

    display_rows = []
    for row in rows:
        (
            chapter,
            mode,
            checkpoint_index,
            name,
            n,
            mean_ms,
            stddev_ms,
            min_ms,
            p25,
            median,
            p75,
            p90,
            p95,
            max_ms,
            cv,
        ) = row
        # Canonical summary keeps the same grouping identity; only the display name prefers canonical strings.
        display_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                n,
                f"{mean_ms:.2f}",
                f"{stddev_ms:.2f}",
                min_ms,
                int(round(p25)),
                int(round(median)),
                int(round(p75)),
                int(round(p90)),
                int(round(p95)),
                max_ms,
                f"{cv:.3f}",
            )
        )

    print(format_table(headers, display_rows))


def build_checkpoint_consistency_rows(conn, canonical: bool) -> List[Tuple[object, ...]]:
    summary_rows = build_checkpoint_summary_rows(conn, canonical=canonical)
    if not summary_rows:
        return []

    metrics_rows: List[Tuple[object, ...]] = []
    for (
        chapter,
        mode,
        checkpoint_index,
        name,
        n,
        mean_ms,
        stddev_ms,
        min_ms,
        p25,
        median,
        p75,
        _p90,
        _p95,
        max_ms,
        cv,
    ) in summary_rows:
        iqr = p75 - p25
        value_range = max_ms - min_ms
        # Guard against zero or missing medians to keep ratios safe for display.
        relative_iqr = iqr / median if median else 0.0
        relative_range = value_range / median if median else 0.0
        pb_gap = mean_ms - min_ms
        metrics_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                n,
                mean_ms,
                min_ms,
                median,
                max_ms,
                iqr,
                value_range,
                relative_iqr,
                relative_range,
                pb_gap,
                cv,
            )
        )

    metrics_rows.sort(
        key=lambda row: (
            -row[11],  # relative_iqr
            -row[12],  # relative_range
            -row[14],  # cv
            row[0],
            row[1],
            row[2],
        )
    )
    return metrics_rows


def show_checkpoint_consistency(conn) -> None:
    rows = build_checkpoint_consistency_rows(conn, canonical=False)
    if not rows:
        print("No segment data available.")
        return

    headers = (
        "chapter",
        "mode",
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "min_ms",
        "median_ms",
        "max_ms",
        "iqr_ms",
        "range_ms",
        "relative_iqr",
        "relative_range",
        "pb_gap_ms",
        "cv",
    )

    display_rows = []
    for (
        chapter,
        mode,
        checkpoint_index,
        name,
        n,
        mean_ms,
        min_ms,
        median,
        max_ms,
        iqr,
        value_range,
        relative_iqr,
        relative_range,
        pb_gap,
        cv,
    ) in rows:
        display_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                n,
                f"{mean_ms:.2f}",
                min_ms,
                int(round(median)),
                max_ms,
                int(round(iqr)),
                int(round(value_range)),
                f"{relative_iqr:.3f}",
                f"{relative_range:.3f}",
                int(round(pb_gap)),
                f"{cv:.3f}",
            )
        )

    print(format_table(headers, display_rows))


def show_canonical_checkpoint_consistency(conn) -> None:
    rows = build_checkpoint_consistency_rows(conn, canonical=True)
    if not rows:
        print("No segment data available.")
        return

    headers = (
        "chapter",
        "mode",
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "min_ms",
        "median_ms",
        "max_ms",
        "iqr_ms",
        "range_ms",
        "relative_iqr",
        "relative_range",
        "pb_gap_ms",
        "cv",
    )

    display_rows = []
    for (
        chapter,
        mode,
        checkpoint_index,
        name,
        n,
        mean_ms,
        min_ms,
        median,
        max_ms,
        iqr,
        value_range,
        relative_iqr,
        relative_range,
        pb_gap,
        cv,
    ) in rows:
        display_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                n,
                f"{mean_ms:.2f}",
                min_ms,
                int(round(median)),
                max_ms,
                int(round(iqr)),
                int(round(value_range)),
                f"{relative_iqr:.3f}",
                f"{relative_range:.3f}",
                int(round(pb_gap)),
                f"{cv:.3f}",
            )
        )

    print(format_table(headers, display_rows))


def build_chapter_expected_rows(
    conn,
    chapter: int,
    mode: int,
    canonical: bool,
) -> Tuple[List[Tuple[object, ...]], Tuple[int, float, float, float, float, float]]:
    summary_rows = build_checkpoint_summary_rows(conn, canonical=canonical)
    chapter_rows: List[Tuple[object, ...]] = []
    total_mean = 0.0
    total_median = 0.0
    total_p25 = 0.0
    total_p75 = 0.0
    total_pb = 0.0
    for row in summary_rows:
        (
            row_chapter,
            row_mode,
            checkpoint_index,
            name,
            n,
            mean_ms,
            _stddev_ms,
            min_ms,
            p25,
            median,
            p75,
            _p90,
            _p95,
            _max_ms,
            cv,
        ) = row
        try:
            row_chapter_int = int(row_chapter)
            row_mode_int = int(row_mode)
        except (TypeError, ValueError):
            continue
        if row_chapter_int != chapter or row_mode_int != mode:
            continue
        chapter_rows.append(
            (
                checkpoint_index,
                name,
                n,
                mean_ms,
                median,
                p25,
                p75,
                cv,
                min_ms,
            )
        )
        total_mean += mean_ms
        total_median += median
        total_p25 += p25
        total_p75 += p75
        total_pb += min_ms
    chapter_rows.sort(key=lambda item: item[0])
    totals = (len(chapter_rows), total_mean, total_median, total_p25, total_p75, total_pb)
    return chapter_rows, totals


def show_chapter_expected(conn, chapter: int, mode: int) -> None:
    rows, totals = build_chapter_expected_rows(conn, chapter, mode, canonical=False)
    if not rows:
        print(f"No data for chapter {chapter} mode {mode}.")
        return
    headers = (
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "median_ms",
        "p25_ms",
        "p75_ms",
        "cv",
        "pb_ms",
    )
    display_rows = []
    for (
        checkpoint_index,
        name,
        n,
        mean_ms,
        median,
        p25,
        p75,
        cv,
        min_ms,
    ) in rows:
        display_rows.append(
            (
                checkpoint_index,
                name,
                n,
                f"{mean_ms:.2f}",
                int(round(median)),
                int(round(p25)),
                int(round(p75)),
                f"{cv:.3f}",
                min_ms,
            )
        )

    checkpoint_count, total_mean, total_median, total_p25, total_p75, total_pb = totals
    display_rows.append(
        (
            "TOTAL",
            "",
            checkpoint_count,
            f"{total_mean:.2f}",
            int(round(total_median)),
            int(round(total_p25)),
            int(round(total_p75)),
            "",
            int(round(total_pb)),
        )
    )

    print(format_table(headers, display_rows))


def show_canonical_chapter_expected(conn, chapter: int, mode: int) -> None:
    rows, totals = build_chapter_expected_rows(conn, chapter, mode, canonical=True)
    if not rows:
        print(f"No data for chapter {chapter} mode {mode}.")
        return
    headers = (
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "median_ms",
        "p25_ms",
        "p75_ms",
        "cv",
        "pb_ms",
    )
    display_rows = []
    for (
        checkpoint_index,
        name,
        n,
        mean_ms,
        median,
        p25,
        p75,
        cv,
        min_ms,
    ) in rows:
        display_rows.append(
            (
                checkpoint_index,
                name,
                n,
                f"{mean_ms:.2f}",
                int(round(median)),
                int(round(p25)),
                int(round(p75)),
                f"{cv:.3f}",
                min_ms,
            )
        )

    checkpoint_count, total_mean, total_median, total_p25, total_p75, total_pb = totals
    display_rows.append(
        (
            "TOTAL",
            "",
            checkpoint_count,
            f"{total_mean:.2f}",
            int(round(total_median)),
            int(round(total_p25)),
            int(round(total_p75)),
            "",
            int(round(total_pb)),
        )
    )

    print(format_table(headers, display_rows))


def build_priority_rows(
    conn,
    canonical: bool,
    *,
    chapter: int | None = None,
    mode: int | None = None,
    sort_key: Callable[[Tuple[object, ...]], object] | None = None,
) -> Tuple[List[Tuple[object, ...]], Tuple[int, float, float]]:
    summary_rows = build_checkpoint_summary_rows(conn, canonical=canonical)
    entries: List[Tuple[int, int, int, str, int, float, int, float, float, float, float]] = []
    total_mean = 0.0
    total_pb_gap = 0.0

    for row in summary_rows:
        (
            row_chapter,
            row_mode,
            checkpoint_index,
            name,
            n,
            mean_ms,
            _stddev_ms,
            min_ms,
            p25,
            median,
            p75,
            _p90,
            _p95,
            _max_ms,
            cv,
        ) = row
        try:
            chapter_int = int(row_chapter)
            mode_int = int(row_mode)
        except (TypeError, ValueError):
            continue
        if chapter is not None and chapter_int != chapter:
            continue
        if mode is not None and mode_int != mode:
            continue
        iqr = p75 - p25
        relative_iqr = iqr / median if median else 0.0
        pb_gap = mean_ms - min_ms
        entries.append(
            (
                chapter_int,
                mode_int,
                checkpoint_index,
                name,
                int(n),
                mean_ms,
                int(min_ms),
                pb_gap,
                median,
                iqr,
                relative_iqr,
                cv,
            )
        )
        total_mean += mean_ms
        total_pb_gap += pb_gap

    if not entries:
        return [], (0, 0.0, 0.0)

    prioritized: List[Tuple[object, ...]] = []
    for entry in entries:
        (
            chapter_int,
            mode_int,
            checkpoint_index,
            name,
            attempts,
            mean_ms,
            min_ms,
            pb_gap,
            median,
            iqr,
            relative_iqr,
            cv,
        ) = entry
        mean_share = mean_ms / total_mean if total_mean else 0.0
        prioritized.append(
            (
                chapter_int,
                mode_int,
                checkpoint_index,
                name,
                attempts,
                mean_ms,
                min_ms,
                pb_gap,
                median,
                iqr,
                relative_iqr,
                cv,
                mean_share,
            )
        )

    if sort_key is None:
        prioritized.sort(
            key=lambda item: (
                -item[7],  # pb_gap_ms
                -item[10],  # relative_iqr
                -item[11],  # cv
                -item[5],  # mean_ms
                item[2],   # checkpoint_index
            )
        )
    else:
        prioritized.sort(key=sort_key)

    totals = (len(prioritized), total_mean, total_pb_gap)
    return prioritized, totals


def show_chapter_priority(conn, chapter: int, mode: int) -> None:
    rows, totals = build_priority_rows(conn, canonical=False, chapter=chapter, mode=mode)
    if not rows:
        print(f"No data for chapter {chapter} mode {mode}.")
        return

    headers = (
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "min_ms",
        "pb_gap_ms",
        "median_ms",
        "iqr_ms",
        "relative_iqr",
        "cv",
        "mean_share",
    )

    display_rows = []
    for (
        _chapter,
        _mode,
        checkpoint_index,
        name,
        attempts,
        mean_ms,
        min_ms,
        pb_gap,
        median,
        iqr,
        relative_iqr,
        cv,
        mean_share,
    ) in rows:
        display_rows.append(
            (
                checkpoint_index,
                name,
                attempts,
                f"{mean_ms:.2f}",
                min_ms,
                int(round(pb_gap)),
                int(round(median)),
                int(round(iqr)),
                f"{relative_iqr:.3f}",
                f"{cv:.3f}",
                f"{mean_share:.3f}",
            )
        )

    checkpoint_count, total_mean, total_pb_gap = totals
    display_rows.append(
        (
            "TOTAL",
            "",
            checkpoint_count,
            f"{total_mean:.2f}",
            "",
            int(round(total_pb_gap)),
            "",
            "",
            "",
            "",
            "",
        )
    )

    print(format_table(headers, display_rows))


def show_canonical_chapter_priority(conn, chapter: int, mode: int) -> None:
    rows, totals = build_priority_rows(conn, canonical=True, chapter=chapter, mode=mode)
    if not rows:
        print(f"No data for chapter {chapter} mode {mode}.")
        return

    headers = (
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "min_ms",
        "pb_gap_ms",
        "median_ms",
        "iqr_ms",
        "relative_iqr",
        "cv",
        "mean_share",
    )

    display_rows = []
    for (
        _chapter,
        _mode,
        checkpoint_index,
        name,
        attempts,
        mean_ms,
        min_ms,
        pb_gap,
        median,
        iqr,
        relative_iqr,
        cv,
        mean_share,
    ) in rows:
        display_rows.append(
            (
                checkpoint_index,
                name,
                attempts,
                f"{mean_ms:.2f}",
                min_ms,
                int(round(pb_gap)),
                int(round(median)),
                int(round(iqr)),
                f"{relative_iqr:.3f}",
                f"{cv:.3f}",
                f"{mean_share:.3f}",
            )
        )

    checkpoint_count, total_mean, total_pb_gap = totals
    display_rows.append(
        (
            "TOTAL",
            "",
            checkpoint_count,
            f"{total_mean:.2f}",
            "",
            int(round(total_pb_gap)),
            "",
            "",
            "",
            "",
            "",
        )
    )

    print(format_table(headers, display_rows))


def show_run_priority(conn) -> None:
    sort_key = lambda item: (
        -item[7],
        -item[10],
        -item[11],
        -item[5],
        item[0],
        item[1],
        item[2],
    )
    rows, totals = build_priority_rows(conn, canonical=False, sort_key=sort_key)
    if not rows:
        print("No segment data available.")
        return

    headers = (
        "chapter",
        "mode",
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "min_ms",
        "pb_gap_ms",
        "median_ms",
        "iqr_ms",
        "relative_iqr",
        "cv",
        "global_mean_share",
    )

    display_rows = []
    for (
        chapter,
        mode,
        checkpoint_index,
        name,
        attempts,
        mean_ms,
        min_ms,
        pb_gap,
        median,
        iqr,
        relative_iqr,
        cv,
        mean_share,
    ) in rows:
        display_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                attempts,
                f"{mean_ms:.2f}",
                min_ms,
                int(round(pb_gap)),
                int(round(median)),
                int(round(iqr)),
                f"{relative_iqr:.3f}",
                f"{cv:.3f}",
                f"{mean_share:.3f}",
            )
        )

    checkpoint_count, total_mean, total_pb_gap = totals
    display_rows.append(
        (
            "TOTAL",
            "",
            "",
            "",
            checkpoint_count,
            f"{total_mean:.2f}",
            "",
            int(round(total_pb_gap)),
            "",
            "",
            "",
            "",
            "",
        )
    )

    print(format_table(headers, display_rows))


def show_canonical_run_priority(conn) -> None:
    sort_key = lambda item: (
        -item[7],
        -item[10],
        -item[11],
        -item[5],
        item[0],
        item[1],
        item[2],
    )
    rows, totals = build_priority_rows(conn, canonical=True, sort_key=sort_key)
    if not rows:
        print("No segment data available.")
        return

    headers = (
        "chapter",
        "mode",
        "checkpoint_index",
        "checkpoint_name",
        "n",
        "mean_ms",
        "min_ms",
        "pb_gap_ms",
        "median_ms",
        "iqr_ms",
        "relative_iqr",
        "cv",
        "global_mean_share",
    )

    display_rows = []
    for (
        chapter,
        mode,
        checkpoint_index,
        name,
        attempts,
        mean_ms,
        min_ms,
        pb_gap,
        median,
        iqr,
        relative_iqr,
        cv,
        mean_share,
    ) in rows:
        display_rows.append(
            (
                chapter,
                mode,
                checkpoint_index,
                name,
                attempts,
                f"{mean_ms:.2f}",
                min_ms,
                int(round(pb_gap)),
                int(round(median)),
                int(round(iqr)),
                f"{relative_iqr:.3f}",
                f"{cv:.3f}",
                f"{mean_share:.3f}",
            )
        )

    checkpoint_count, total_mean, total_pb_gap = totals
    display_rows.append(
        (
            "TOTAL",
            "",
            "",
            "",
            checkpoint_count,
            f"{total_mean:.2f}",
            "",
            int(round(total_pb_gap)),
            "",
            "",
            "",
            "",
            "",
        )
    )

    print(format_table(headers, display_rows))
def show_pb(conn) -> None:
    cursor = conn.execute(
        """
        SELECT checkpoint_name,
               MIN(segment_ms) AS pb_ms,
               COUNT(*) AS attempts
        FROM segments
        WHERE is_complete = 1
        GROUP BY chapter, mode, checkpoint_index, checkpoint_name
        ORDER BY chapter, mode, checkpoint_index
        """
    )
    rows = cursor.fetchall()
    if not rows:
        print("No segment data available.")
        return
    print(format_table(("checkpoint_name", "pb_ms", "attempts"), rows))


def show_canonical_pb(conn) -> None:
    cursor = conn.execute(
        """
        SELECT COALESCE(a.canonical_name, d.canonical_name, s.checkpoint_name) AS display_name,
               MIN(s.segment_ms) AS pb_ms,
               COUNT(*) AS attempts,
               COALESCE(MIN(d.chapter), MIN(s.chapter)) AS ord_chapter,
               COALESCE(MIN(d.mode), MIN(s.mode)) AS ord_mode,
               COALESCE(MIN(d.checkpoint_index), MIN(s.checkpoint_index)) AS ord_index
        FROM segments AS s
        LEFT JOIN checkpoint_aliases AS a
            ON a.alias_name = s.checkpoint_name
        LEFT JOIN checkpoint_defs AS d
            ON d.chapter = s.chapter
           AND d.mode = s.mode
           AND d.checkpoint_index = s.checkpoint_index
        WHERE s.is_complete = 1
        GROUP BY display_name
        ORDER BY ord_chapter, ord_mode, ord_index, display_name
        """
    )
    rows = cursor.fetchall()
    if not rows:
        print("No segment data available.")
        return
    display_rows = [(name, pb, attempts) for name, pb, attempts, *_ in rows]
    print(format_table(("canonical_name", "pb_ms", "attempts"), display_rows))


def show_checkpoint(conn, checkpoint_name: str) -> None:
    cursor = conn.execute(
        """
        SELECT COUNT(*) AS attempts,
               MIN(segment_ms) AS pb_ms,
               AVG(segment_ms) AS avg_ms,
               MAX(segment_ms) AS worst_ms,
               SUM(deaths) AS death_total
        FROM segments
        WHERE checkpoint_name = ? AND is_complete = 1
        """,
        (checkpoint_name,),
    )
    row = cursor.fetchone()
    if not row or row[0] == 0:
        print(f"No data for checkpoint '{checkpoint_name}'.")
        return
    headers = ("checkpoint_name", "attempts", "pb_ms", "avg_ms", "worst_ms", "death_total")
    data_row = (
        checkpoint_name,
        row[0],
        int(row[1]) if row[1] is not None else None,
        f"{row[2]:.2f}" if row[2] is not None else None,
        int(row[3]) if row[3] is not None else None,
        int(row[4]) if row[4] is not None else 0,
    )
    print(format_table(headers, [data_row]))


def show_chapter(conn, chapter: int, mode: int) -> None:
    cursor = conn.execute(
        """
        SELECT checkpoint_index,
               checkpoint_name,
               COUNT(*) AS attempts,
               MIN(segment_ms) AS pb_ms,
               AVG(segment_ms) AS avg_ms
        FROM segments
        WHERE chapter = ? AND mode = ? AND is_complete = 1
        GROUP BY checkpoint_index, checkpoint_name
        ORDER BY checkpoint_index
        """,
        (chapter, mode),
    )
    rows = cursor.fetchall()
    if not rows:
        print(f"No data for chapter {chapter} mode {mode}.")
        return
    formatted_rows = [
        (name, attempts, pb, f"{avg:.2f}")
        for _, name, attempts, pb, avg in rows
    ]
    print(format_table(("checkpoint_name", "attempts", "pb_ms", "avg_ms"), formatted_rows))


def show_canonical_chapter(conn, chapter: int, mode: int) -> None:
    cursor = conn.execute(
        """
        SELECT COALESCE(a.canonical_name, d.canonical_name, s.checkpoint_name) AS display_name,
               COUNT(*) AS attempts,
               MIN(s.segment_ms) AS pb_ms,
               AVG(s.segment_ms) AS avg_ms,
               COALESCE(MIN(d.chapter), MIN(s.chapter)) AS ord_chapter,
               COALESCE(MIN(d.mode), MIN(s.mode)) AS ord_mode,
               COALESCE(MIN(d.checkpoint_index), MIN(s.checkpoint_index)) AS ord_index
        FROM segments AS s
        LEFT JOIN checkpoint_aliases AS a
            ON a.alias_name = s.checkpoint_name
        LEFT JOIN checkpoint_defs AS d
            ON d.chapter = s.chapter
           AND d.mode = s.mode
           AND d.checkpoint_index = s.checkpoint_index
        WHERE s.chapter = ? AND s.mode = ? AND s.is_complete = 1
        GROUP BY display_name
        ORDER BY ord_chapter, ord_mode, ord_index, display_name
        """,
        (chapter, mode),
    )
    rows = cursor.fetchall()
    if not rows:
        print(f"No data for chapter {chapter} mode {mode}.")
        return
    formatted_rows = [
        (name, attempts, pb, f"{avg:.2f}")
        for name, attempts, pb, avg, *_ in rows
    ]
    print(format_table(("checkpoint_name", "attempts", "pb_ms", "avg_ms"), formatted_rows))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Basic stats queries")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite database path (default: %(default)s)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    sessions_parser = subparsers.add_parser("sessions", help="List imported sessions")
    sessions_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max sessions to show (default: %(default)s)",
    )

    attempts_parser = subparsers.add_parser(
        "attempts",
        help="List inferred attempts (debug/inspection)",
    )
    attempts_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max attempts to show (default: %(default)s)",
    )

    subparsers.add_parser("pb", help="Show PB per checkpoint")
    subparsers.add_parser(
        "canonical-pb",
        help="Show PB per checkpoint using canonical/alias mapping",
    )

    checkpoint_parser = subparsers.add_parser("checkpoint", help="Stats for one checkpoint")
    checkpoint_parser.add_argument("checkpoint_name", help="Exact checkpoint name")

    chapter_parser = subparsers.add_parser("chapter", help="Stats for a chapter/mode")
    chapter_parser.add_argument("chapter", type=int, help="Chapter number")
    chapter_parser.add_argument("mode", type=int, help="Mode id (e.g., 0=A,1=B,2=C)")

    canonical_chapter_parser = subparsers.add_parser(
        "canonical-chapter",
        help="Chapter stats grouped by canonical checkpoint names",
    )
    canonical_chapter_parser.add_argument("chapter", type=int, help="Chapter number")
    canonical_chapter_parser.add_argument("mode", type=int, help="Mode id")

    subparsers.add_parser(
        "checkpoint-summary",
        help="Summary statistics per checkpoint",
    )

    subparsers.add_parser(
        "canonical-checkpoint-summary",
        help="Summary statistics per checkpoint using canonical names",
    )

    subparsers.add_parser(
        "checkpoint-consistency",
        help="Consistency metrics per checkpoint",
    )

    subparsers.add_parser(
        "canonical-checkpoint-consistency",
        help="Consistency metrics per checkpoint using canonical names",
    )

    chapter_expected_parser = subparsers.add_parser(
        "chapter-expected",
        help="Expected chapter times from checkpoint summaries",
    )
    chapter_expected_parser.add_argument("chapter", type=int, help="Chapter number")
    chapter_expected_parser.add_argument("mode", type=int, help="Mode id")

    canonical_chapter_expected_parser = subparsers.add_parser(
        "canonical-chapter-expected",
        help="Expected chapter times using canonical names",
    )
    canonical_chapter_expected_parser.add_argument("chapter", type=int, help="Chapter number")
    canonical_chapter_expected_parser.add_argument("mode", type=int, help="Mode id")

    chapter_priority_parser = subparsers.add_parser(
        "chapter-priority",
        help="Prioritize checkpoint practice within a chapter",
    )
    chapter_priority_parser.add_argument("chapter", type=int, help="Chapter number")
    chapter_priority_parser.add_argument("mode", type=int, help="Mode id")

    canonical_chapter_priority_parser = subparsers.add_parser(
        "canonical-chapter-priority",
        help="Prioritize practice using canonical checkpoint names",
    )
    canonical_chapter_priority_parser.add_argument("chapter", type=int, help="Chapter number")
    canonical_chapter_priority_parser.add_argument("mode", type=int, help="Mode id")

    subparsers.add_parser(
        "run-priority",
        help="Global practice priority across all checkpoints",
    )

    subparsers.add_parser(
        "canonical-run-priority",
        help="Global practice priority using canonical checkpoint names",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = connect(args.db)
    try:
        initialize_schema(conn)
        if args.command == "sessions":
            show_sessions(conn, args.limit)
        elif args.command == "attempts":
            show_attempts(conn, args.limit)
        elif args.command == "pb":
            show_pb(conn)
        elif args.command == "canonical-pb":
            show_canonical_pb(conn)
        elif args.command == "checkpoint":
            show_checkpoint(conn, args.checkpoint_name)
        elif args.command == "chapter":
            show_chapter(conn, args.chapter, args.mode)
        elif args.command == "canonical-chapter":
            show_canonical_chapter(conn, args.chapter, args.mode)
        elif args.command == "checkpoint-summary":
            show_checkpoint_summary(conn)
        elif args.command == "canonical-checkpoint-summary":
            show_canonical_checkpoint_summary(conn)
        elif args.command == "checkpoint-consistency":
            show_checkpoint_consistency(conn)
        elif args.command == "canonical-checkpoint-consistency":
            show_canonical_checkpoint_consistency(conn)
        elif args.command == "chapter-expected":
            show_chapter_expected(conn, args.chapter, args.mode)
        elif args.command == "canonical-chapter-expected":
            show_canonical_chapter_expected(conn, args.chapter, args.mode)
        elif args.command == "chapter-priority":
            show_chapter_priority(conn, args.chapter, args.mode)
        elif args.command == "canonical-chapter-priority":
            show_canonical_chapter_priority(conn, args.chapter, args.mode)
        elif args.command == "run-priority":
            show_run_priority(conn)
        elif args.command == "canonical-run-priority":
            show_canonical_run_priority(conn)
        else:
            raise SystemExit(f"Unknown command: {args.command}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""Post-run cumulative pace chart.

This is a lightweight reporting tool that plots one attempt's cumulative time by split,
alongside historical full-run cumulative references (best-at-split and median).

Usage:
    python -m src.pace_chart --attempt-id 123
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import statistics
from dataclasses import dataclass
from pathlib import Path

from .live_coach import FULL_RUN_PREDICTION_ROUTE, FULL_RUN_ROUTE_KEY_SET, FULL_RUN_ATTEMPT_TYPES
from .seed_checkpoints import CHECKPOINT_SPEC
from .schema import DEFAULT_DB_PATH, connect, initialize_schema

SegmentKey = tuple[int, int, int]


@dataclass(frozen=True)
class AttemptInfo:
    attempt_id: int
    session_id: int
    attempt_index: int
    attempt_type: str | None


@dataclass(frozen=True)
class CumulativeSeries:
    split_indices: list[int]
    cumulative_ms: list[int]


MODE_LETTERS = {0: "A", 1: "B", 2: "C"}


def _format_run_time(milliseconds: int) -> str:
    total_ms = max(0, int(round(milliseconds)))
    total_seconds, ms = divmod(total_ms, 1000)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}.{ms:03d}"
    return f"{minutes}:{seconds:02d}.{ms:03d}"


def build_split_labels(*, split_indices: list[int]) -> list[str]:
    labels: list[str] = []
    for route_index in split_indices:
        key = FULL_RUN_PREDICTION_ROUTE[int(route_index)]
        chapter, mode, checkpoint_index = key
        canonical = CHECKPOINT_SPEC[(chapter, mode)][checkpoint_index]
        section = canonical.split(" / ", 1)[1] if " / " in canonical else canonical
        mode_letter = MODE_LETTERS.get(mode, "?")
        prefix = "P" if chapter == 0 else f"{chapter}{mode_letter}"
        labels.append(f"{prefix} {section}".strip())
    return labels


def _pick_tick_positions(split_indices: list[int], *, max_ticks: int = 12) -> list[int]:
    if not split_indices:
        return []
    if len(split_indices) <= max_ticks:
        return split_indices
    step = max(1, int(math.ceil(len(split_indices) / max_ticks)))
    picked = split_indices[::step]
    if picked[-1] != split_indices[-1]:
        picked.append(split_indices[-1])
    return picked


def resolve_attempt_id(
    conn: sqlite3.Connection,
    *,
    attempt_id: int | None,
    session_id: int | None,
    attempt_index: int | None,
) -> int:
    if attempt_id is not None:
        row = conn.execute("SELECT id FROM attempts WHERE id = ?", (attempt_id,)).fetchone()
        if not row:
            raise SystemExit(f"Attempt id not found: {attempt_id}")
        return int(attempt_id)

    if session_id is None or attempt_index is None:
        raise SystemExit("Provide either --attempt-id or both --session-id and --attempt-index")

    row = conn.execute(
        "SELECT id FROM attempts WHERE session_id = ? AND attempt_index = ?",
        (session_id, attempt_index),
    ).fetchone()
    if not row:
        raise SystemExit(f"Attempt not found: session_id={session_id} attempt_index={attempt_index}")
    return int(row[0])


def load_attempt_info(conn: sqlite3.Connection, *, attempt_id: int) -> AttemptInfo:
    row = conn.execute(
        "SELECT id, session_id, attempt_index, attempt_type FROM attempts WHERE id = ?",
        (attempt_id,),
    ).fetchone()
    if not row:
        raise SystemExit(f"Attempt id not found: {attempt_id}")
    return AttemptInfo(
        attempt_id=int(row[0]),
        session_id=int(row[1]),
        attempt_index=int(row[2]),
        attempt_type=row[3],
    )


def load_attempt_segments(conn: sqlite3.Connection, *, attempt_id: int) -> list[tuple[SegmentKey, int]]:
    cursor = conn.execute(
        """
        SELECT chapter,
               mode,
               checkpoint_index,
               segment_ms
        FROM segments
        WHERE attempt_id = ?
          AND is_complete = 1
        ORDER BY seq_in_session ASC
        """,
        (attempt_id,),
    )
    rows: list[tuple[SegmentKey, int]] = []
    for chapter, mode, checkpoint_index, segment_ms in cursor:
        key = (int(chapter), int(mode), int(checkpoint_index))
        rows.append((key, int(segment_ms)))
    return rows


def build_attempt_cumulative_series(rows: list[tuple[SegmentKey, int]]) -> CumulativeSeries | None:
    """Align an attempt's segment list onto the full-run route prefix.

    Conservative behavior: if route keys appear out of order (relative to the expected
    next key in the route), stop extending the series at the first mismatch.
    If no aligned points exist, return None.
    """

    if not rows:
        return None

    split_indices: list[int] = []
    cumulative_ms: list[int] = []
    route_pos = 0
    total = 0
    route = FULL_RUN_PREDICTION_ROUTE

    for key, segment_ms in rows:
        if key not in FULL_RUN_ROUTE_KEY_SET:
            continue
        if route_pos >= len(route):
            break
        expected = route[route_pos]
        if key != expected:
            break
        total += max(0, int(segment_ms))
        split_indices.append(route_pos)
        cumulative_ms.append(total)
        route_pos += 1

    if not split_indices:
        return None
    return CumulativeSeries(split_indices=split_indices, cumulative_ms=cumulative_ms)


def collect_full_run_cumulative_reference_values(
    conn: sqlite3.Connection,
) -> dict[int, list[int]]:
    """Return cumulative-at-split samples per route index from full-run attempts.

    Each sample comes from a real attempt prefix that matches the canonical route prefix.
    """

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
          AND s.is_complete = 1
        ORDER BY s.attempt_id ASC, s.seq_in_session ASC
        """,
        FULL_RUN_ATTEMPT_TYPES,
    )

    values_by_split: dict[int, list[int]] = {i: [] for i in range(len(FULL_RUN_PREDICTION_ROUTE))}

    current_attempt_id: int | None = None
    attempt_rows: list[tuple[SegmentKey, int]] = []

    def flush_attempt() -> None:
        if not attempt_rows:
            return
        series = build_attempt_cumulative_series(attempt_rows)
        if series is None:
            return
        for split_idx, cum_ms in zip(series.split_indices, series.cumulative_ms):
            values_by_split[split_idx].append(cum_ms)

    for attempt_id, chapter, mode, checkpoint_index, segment_ms in cursor:
        attempt_id = int(attempt_id) if attempt_id is not None else None
        if current_attempt_id is None:
            current_attempt_id = attempt_id
        elif attempt_id != current_attempt_id:
            flush_attempt()
            attempt_rows = []
            current_attempt_id = attempt_id

        key = (int(chapter), int(mode), int(checkpoint_index))
        if key in FULL_RUN_ROUTE_KEY_SET:
            attempt_rows.append((key, int(segment_ms)))

    flush_attempt()
    return values_by_split


def build_reference_series(
    values_by_split: dict[int, list[int]],
    *,
    split_indices: list[int],
) -> tuple[list[int | None], list[float | None]]:
    pb: list[int | None] = []
    median: list[float | None] = []
    for split_idx in split_indices:
        values = values_by_split.get(split_idx) or []
        if not values:
            pb.append(None)
            median.append(None)
            continue
        pb.append(min(values))
        median.append(statistics.median(values))
    return pb, median


def plot_cumulative_pace_chart(
    *,
    attempt_info: AttemptInfo,
    run_series: CumulativeSeries,
    pb_series: list[int | None],
    median_series: list[float | None],
    out_path: Path,
) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit(
            "matplotlib is required for plotting. Install it (e.g. pip install matplotlib)"
        ) from exc

    x = run_series.split_indices
    run_y = run_series.cumulative_ms

    def to_y(values):
        return [math.nan if v is None else float(v) for v in values]

    pb_y = to_y(pb_series)
    median_y = to_y(median_series)

    fig, ax = plt.subplots(figsize=(10, 5), dpi=130)
    ax.plot(x, run_y, label="Run", linewidth=2.25)

    if any(not math.isnan(v) for v in pb_y):
        ax.plot(x, pb_y, label="PB", linewidth=1.75)
    if any(not math.isnan(v) for v in median_y):
        ax.plot(x, median_y, label="Median", linewidth=1.75)

    title = f"Attempt {attempt_info.attempt_id}"
    if attempt_info.attempt_type:
        title += f" ({attempt_info.attempt_type})"
    ax.set_title(title)
    ax.set_xlabel("Split")
    ax.set_ylabel("Cumulative time (ms)")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best")

    tick_positions = _pick_tick_positions(run_series.split_indices)
    tick_labels_by_pos = dict(zip(run_series.split_indices, build_split_labels(split_indices=run_series.split_indices)))
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([tick_labels_by_pos.get(pos, str(pos)) for pos in tick_positions], rotation=35, ha="right")

    final_ms = run_y[-1] if run_y else 0
    covers_full_route = bool(run_series.split_indices) and run_series.split_indices[-1] == len(FULL_RUN_PREDICTION_ROUTE) - 1
    annotation_label = "Final" if covers_full_route else "Aligned prefix total"
    ax.text(
        0.01,
        0.01,
        f"{annotation_label}: {_format_run_time(final_ms)}",
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment="bottom",
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot cumulative pace chart for one attempt")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite database path (default: %(default)s)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--attempt-id", type=int, help="Attempt id from the attempts table")
    group.add_argument("--session-id", type=int, help="Session id (requires --attempt-index)")
    parser.add_argument(
        "--attempt-index",
        type=int,
        default=None,
        help="Attempt index within the session (0-based)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output PNG path (default: ./pace_attempt_<id>.png)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = connect(args.db)
    try:
        initialize_schema(conn)
        attempt_id = resolve_attempt_id(
            conn,
            attempt_id=args.attempt_id,
            session_id=args.session_id,
            attempt_index=args.attempt_index,
        )
        attempt_info = load_attempt_info(conn, attempt_id=attempt_id)
        rows = load_attempt_segments(conn, attempt_id=attempt_id)
        run_series = build_attempt_cumulative_series(rows)
        if run_series is None:
            raise SystemExit(
                "Attempt does not align to the full-run route prefix; cannot chart cumulative pace"
            )

        values_by_split = collect_full_run_cumulative_reference_values(conn)
        pb_series, median_series = build_reference_series(
            values_by_split,
            split_indices=run_series.split_indices,
        )

        out_path = args.out
        if out_path is None:
            out_path = Path.cwd() / f"pace_attempt_{attempt_id}.png"

        plot_cumulative_pace_chart(
            attempt_info=attempt_info,
            run_series=run_series,
            pb_series=pb_series,
            median_series=median_series,
            out_path=out_path,
        )
        print(f"Wrote {out_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

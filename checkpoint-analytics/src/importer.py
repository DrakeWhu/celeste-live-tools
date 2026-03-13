"""CSV importer for Celeste checkpoint sessions."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable

from .schema import DEFAULT_DB_PATH, connect, initialize_schema


class ImporterError(Exception):
    """Base exception for importer issues."""


class DuplicateSessionError(ImporterError):
    """Raised when attempting to re-import an existing source file."""


EXPECTED_COLUMNS = (
    "chapter",
    "mode",
    "mode_label",
    "checkpoint_index",
    "checkpoint_name",
    "start_time_iso",
    "end_time_iso",
    "segment_ms",
    "deaths",
)

SESSION_TYPES = ("full_run", "chapter_practice", "mixed_practice")

ATTEMPT_TYPES = (
    "full_run_complete",
    "full_run_incomplete",
    "chapter_practice",
    "checkpoint_grind",
    "undefined",
)


def normalize_session_type(value: str) -> str:
    normalized = value.strip()
    # Backwards-compatible alias for earlier docs/scripts.
    if normalized == "practice":
        return "mixed_practice"
    if normalized not in SESSION_TYPES:
        allowed = ", ".join(SESSION_TYPES)
        raise ImporterError(f"Invalid session_type '{value}'. Allowed: {allowed}")
    return normalized


def classify_session_type(
    segments: Iterable[dict[str, str]],
    *,
    dominance_threshold: float = 0.80,
    full_run_min_chapters: int = 5,
) -> str:
    """Heuristic session classification.

    Rules (MVP):
    - chapter_practice: a single (chapter, mode) accounts for >= dominance_threshold
    - full_run: >= full_run_min_chapters distinct chapters AND no chapter reaches dominance_threshold
    - mixed_practice: fallback
    """

    total = 0
    pair_counts: dict[tuple[int, int], int] = {}
    chapter_counts: dict[int, int] = {}

    for row in segments:
        total += 1
        chapter = int(row["chapter"])
        mode = int(row["mode"])
        pair_counts[(chapter, mode)] = pair_counts.get((chapter, mode), 0) + 1
        chapter_counts[chapter] = chapter_counts.get(chapter, 0) + 1

    if total <= 0:
        # read_csv_rows guards against empty CSVs, but keep a safe fallback.
        return "mixed_practice"

    max_pair_share = max(pair_counts.values()) / total
    if max_pair_share >= dominance_threshold:
        return "chapter_practice"

    distinct_chapters = len(chapter_counts)
    max_chapter_share = max(chapter_counts.values()) / total
    if distinct_chapters >= full_run_min_chapters and max_chapter_share < dominance_threshold:
        return "full_run"

    return "mixed_practice"


def should_start_new_attempt(
    *,
    prev_chapter: int,
    prev_mode: int,
    prev_checkpoint_index: int,
    curr_chapter: int,
    curr_mode: int,
    curr_checkpoint_index: int,
    max_chapter_seen_in_attempt: int,
) -> bool:
    """Return True when a boundary strongly suggests a reset/new attempt."""

    start_of_chapter = curr_checkpoint_index == 0
    large_backward_chapter_jump = curr_chapter <= prev_chapter - 2
    earlier_chapter_start = start_of_chapter and curr_chapter < prev_chapter
    restart_same_chapter = (
        start_of_chapter
        and curr_chapter == prev_chapter
        and curr_mode == prev_mode
        # Even back-to-back start segments are treated as separate attempts.
    )
    back_to_earlier_start_after_progress = start_of_chapter and max_chapter_seen_in_attempt > curr_chapter
    early_checkpoint_reset = (
        curr_chapter == prev_chapter
        and curr_mode == prev_mode
        and curr_checkpoint_index <= 1
        and (prev_checkpoint_index - curr_checkpoint_index) >= 2
    )

    return any(
        (
            large_backward_chapter_jump,
            earlier_chapter_start,
            restart_same_chapter,
            back_to_earlier_start_after_progress,
            early_checkpoint_reset,
        )
    )


def infer_attempt_spans(rows: list[dict[str, str]]) -> list[tuple[int, int]]:
    """Infer attempt spans as (start_index, end_index) over ordered rows."""

    if not rows:
        return []

    starts = [0]
    max_chapter_seen = int(rows[0]["chapter"])

    for i in range(1, len(rows)):
        prev = rows[i - 1]
        curr = rows[i]
        prev_chapter = int(prev["chapter"])
        prev_mode = int(prev["mode"])
        prev_idx = int(prev["checkpoint_index"])
        curr_chapter = int(curr["chapter"])
        curr_mode = int(curr["mode"])
        curr_idx = int(curr["checkpoint_index"])

        max_chapter_seen = max(max_chapter_seen, prev_chapter)

        if should_start_new_attempt(
            prev_chapter=prev_chapter,
            prev_mode=prev_mode,
            prev_checkpoint_index=prev_idx,
            curr_chapter=curr_chapter,
            curr_mode=curr_mode,
            curr_checkpoint_index=curr_idx,
            max_chapter_seen_in_attempt=max_chapter_seen,
        ):
            starts.append(i)
            max_chapter_seen = curr_chapter

    spans: list[tuple[int, int]] = []
    for start, end in zip(starts, starts[1:] + [len(rows)]):
        if start < end:
            spans.append((start, end))
    return spans


def classify_attempt_type(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "undefined"

    chapters = [int(r["chapter"]) for r in rows]
    modes = [int(r["mode"]) for r in rows]
    checkpoints = [int(r["checkpoint_index"]) for r in rows]
    total = len(rows)

    distinct_chapters_set = set(chapters)
    reached_summit_final = any(
        int(r["chapter"]) == 7
        and int(r["mode"]) == 0
        and int(r["checkpoint_index"]) == 6
        for r in rows
    )
    if reached_summit_final and {1, 2, 3, 4, 5, 6, 7}.issubset(distinct_chapters_set):
        return "full_run_complete"

    pair_counts: dict[tuple[int, int], int] = {}
    for ch, md in zip(chapters, modes):
        pair_counts[(ch, md)] = pair_counts.get((ch, md), 0) + 1

    if len(pair_counts) == 1:
        unique_checkpoints = len(set(checkpoints))
        if total >= 3 and unique_checkpoints <= 2:
            return "checkpoint_grind"
        return "chapter_practice"

    distinct_chapters = len(distinct_chapters_set)
    if distinct_chapters >= 5:
        return "full_run_incomplete"

    return "undefined"


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = [col for col in EXPECTED_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise ImporterError(f"CSV missing expected columns: {', '.join(missing)}")
        rows = list(reader)
        if not rows:
            raise ImporterError("CSV contains no rows; nothing to import")
        return rows


def get_session_bounds(rows: Iterable[dict[str, str]]) -> tuple[str | None, str | None]:
    start_times = [row["start_time_iso"] for row in rows if row.get("start_time_iso")]
    end_times = [row["end_time_iso"] for row in rows if row.get("end_time_iso")]
    started_at = min(start_times) if start_times else None
    ended_at = max(end_times) if end_times else None
    return started_at, ended_at


def insert_session(
    conn,
    *,
    source_file: str,
    started_at: str | None,
    ended_at: str | None,
    session_type: str | None,
    category: str | None,
    notes: str | None,
) -> int:
    existing = conn.execute(
        "SELECT id FROM sessions WHERE source_file = ?",
        (source_file,),
    ).fetchone()
    if existing:
        raise DuplicateSessionError(
            f"Session for '{source_file}' already exists (id={existing[0]})"
        )

    cursor = conn.execute(
        """
        INSERT INTO sessions (
            source_file,
            started_at,
            ended_at,
            session_type,
            category,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source_file, started_at, ended_at, session_type, category, notes),
    )
    return cursor.lastrowid


def insert_attempts(
    conn,
    session_id: int,
    rows: list[dict[str, str]],
    *,
    spans: list[tuple[int, int]],
) -> list[int]:
    attempt_ids: list[int] = []

    for attempt_index, (start, end) in enumerate(spans):
        attempt_rows = rows[start:end]
        started_at = attempt_rows[0].get("start_time_iso")
        ended_at = attempt_rows[-1].get("end_time_iso")
        attempt_type = classify_attempt_type(attempt_rows)
        cursor = conn.execute(
            """
            INSERT INTO attempts (
                session_id,
                attempt_index,
                started_at,
                ended_at,
                attempt_type
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (session_id, attempt_index, started_at, ended_at, attempt_type),
        )
        attempt_ids.append(int(cursor.lastrowid))

    return attempt_ids


def insert_segments(
    conn,
    session_id: int,
    rows: list[dict[str, str]],
    *,
    spans: list[tuple[int, int]],
    attempt_ids: list[int],
) -> None:
    if not spans:
        raise ImporterError("No attempt spans inferred; cannot import segments")

    if len(attempt_ids) != len(spans):
        raise ImporterError(
            "Attempt inference mismatch: attempt_ids and spans length differ"
        )

    attempt_id_by_row_index: list[int] = [attempt_ids[0]] * len(rows)
    for attempt_index, (start, end) in enumerate(spans):
        attempt_id = attempt_ids[attempt_index]
        for row_idx in range(start, end):
            attempt_id_by_row_index[row_idx] = attempt_id

    payload = []
    for seq, row in enumerate(rows):
        payload.append(
            (
                session_id,
                attempt_id_by_row_index[seq],
                seq,
                int(row["chapter"]),
                int(row["mode"]),
                row.get("mode_label"),
                int(row["checkpoint_index"]),
                row["checkpoint_name"],
                row.get("start_time_iso"),
                row.get("end_time_iso"),
                int(row["segment_ms"]),
                int(row.get("deaths", 0) or 0),
                1,
                row["checkpoint_name"],
            )
        )

    conn.executemany(
        """
        INSERT INTO segments (
            session_id,
            attempt_id,
            seq_in_session,
            chapter,
            mode,
            mode_label,
            checkpoint_index,
            checkpoint_name,
            start_time_iso,
            end_time_iso,
            segment_ms,
            deaths,
            is_complete,
            raw_checkpoint_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


def import_csv(
    csv_path: Path,
    *,
    db_path: Path = DEFAULT_DB_PATH,
    session_type: str | None = None,
    category: str | None = None,
    notes: str | None = None,
) -> int:
    csv_path = csv_path.expanduser().resolve()
    if not csv_path.exists():
        raise ImporterError(f"CSV file not found: {csv_path}")

    rows = read_csv_rows(csv_path)
    started_at, ended_at = get_session_bounds(rows)

    computed_session_type = (
        normalize_session_type(session_type)
        if session_type is not None
        else classify_session_type(rows)
    )

    conn = connect(db_path)
    try:
        initialize_schema(conn)
        with conn:
            session_id = insert_session(
                conn,
                source_file=str(csv_path),
                started_at=started_at,
                ended_at=ended_at,
                session_type=computed_session_type,
                category=category,
                notes=notes,
            )
            spans = infer_attempt_spans(rows)
            attempt_ids = insert_attempts(conn, session_id, rows, spans=spans)
            insert_segments(conn, session_id, rows, spans=spans, attempt_ids=attempt_ids)
        return session_id
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import a checkpoint CSV into SQLite")
    parser.add_argument("csv_path", type=Path, help="Path to the checkpoint CSV file")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite database path (default: %(default)s)",
    )
    parser.add_argument(
        "--session-type",
        dest="session_type",
        help=(
            "Session type override (default: auto). Allowed: "
            + ", ".join(SESSION_TYPES)
        ),
    )
    parser.add_argument("--category", help="Category label (chapter/run descriptor)")
    parser.add_argument("--notes", help="Optional notes to store on the session")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        import_csv(
            args.csv_path,
            db_path=args.db,
            session_type=args.session_type,
            category=args.category,
            notes=args.notes,
        )
    except ImporterError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()

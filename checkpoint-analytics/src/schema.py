"""SQLite schema initialization for Celeste checkpoint analytics."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "celeste.db"


SCHEMA_STATEMENTS: Iterable[str] = (
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY,
        source_file TEXT NOT NULL UNIQUE,
        imported_at TEXT NOT NULL DEFAULT (datetime('now')),
        started_at TEXT,
        ended_at TEXT,
        session_type TEXT,
        category TEXT,
        notes TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS attempts (
        id INTEGER PRIMARY KEY,
        session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
        attempt_index INTEGER NOT NULL,
        started_at TEXT,
        ended_at TEXT,
        attempt_type TEXT,
        UNIQUE(session_id, attempt_index)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS segments (
        id INTEGER PRIMARY KEY,
        session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
        attempt_id INTEGER REFERENCES attempts(id) ON DELETE CASCADE,
        seq_in_session INTEGER NOT NULL,
        chapter INTEGER NOT NULL,
        mode INTEGER NOT NULL,
        mode_label TEXT,
        checkpoint_index INTEGER NOT NULL,
        checkpoint_name TEXT NOT NULL,
        start_time_iso TEXT,
        end_time_iso TEXT,
        segment_ms INTEGER NOT NULL,
        deaths INTEGER NOT NULL DEFAULT 0,
        is_complete INTEGER NOT NULL DEFAULT 1,
        raw_checkpoint_name TEXT,
        UNIQUE(session_id, seq_in_session)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS checkpoint_defs (
        id INTEGER PRIMARY KEY,
        chapter INTEGER NOT NULL,
        mode INTEGER NOT NULL,
        checkpoint_index INTEGER NOT NULL,
        canonical_name TEXT NOT NULL,
        short_name TEXT,
        spreadsheet_name TEXT,
        UNIQUE(chapter, mode, checkpoint_index)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS checkpoint_aliases (
        id INTEGER PRIMARY KEY,
        alias_name TEXT NOT NULL UNIQUE,
        chapter INTEGER NOT NULL,
        mode INTEGER NOT NULL,
        checkpoint_index INTEGER NOT NULL,
        canonical_name TEXT NOT NULL,
        FOREIGN KEY (chapter, mode, checkpoint_index)
            REFERENCES checkpoint_defs(chapter, mode, checkpoint_index)
            ON DELETE CASCADE
    );
    """,
)


def ensure_parent_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def connect(db_path: Path) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    with conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)

        # CREATE TABLE IF NOT EXISTS does not add columns for legacy databases.
        # Keep schema evolution non-destructive by adding missing optional columns.
        session_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        if "session_type" not in session_columns:
            conn.execute("ALTER TABLE sessions ADD COLUMN session_type TEXT")

        segment_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(segments)").fetchall()
        }
        if "attempt_id" not in segment_columns:
            conn.execute(
                "ALTER TABLE segments ADD COLUMN attempt_id INTEGER REFERENCES attempts(id) ON DELETE CASCADE"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update the SQLite schema.")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database file (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = connect(args.db)
    initialize_schema(conn)
    conn.close()


if __name__ == "__main__":
    main()

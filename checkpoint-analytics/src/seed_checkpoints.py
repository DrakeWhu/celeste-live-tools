"""Seed canonical checkpoint definitions and aliases."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

from .schema import DEFAULT_DB_PATH, connect, initialize_schema


CHECKPOINT_SPEC: Dict[Tuple[int, int], List[str]] = {
    (0, 0): [
        "Prologue / Start",
    ],
    (1, 0): [
        "Forsaken City A / Start",
        "Forsaken City A / Crossing",
        "Forsaken City A / Chasm",
    ],
    (1, 1): [
        "Forsaken City B / Start",
        "Forsaken City B / Contraption",
        "Forsaken City B / Scrap Pit",
    ],
    (2, 0): [
        "Old Site A / Start",
        "Old Site A / Intervention",
        "Old Site A / Awake",
    ],
    (2, 1): [
        "Old Site B / Start",
        "Old Site B / Combination Lock",
        "Old Site B / Dream Altar",
    ],
    (3, 0): [
        "Celestial Resort A / Start",
        "Celestial Resort A / Huge Mess",
        "Celestial Resort A / Elevator Shaft",
        "Celestial Resort A / Presidential Suite",
    ],
    (3, 1): [
        "Celestial Resort B / Start",
        "Celestial Resort B / Staff Quarters",
        "Celestial Resort B / Library",
        "Celestial Resort B / Rooftop",
    ],
    (4, 0): [
        "Golden Ridge A / Start",
        "Golden Ridge A / Shrine",
        "Golden Ridge A / Old Trail",
        "Golden Ridge A / Cliff Face",
    ],
    (4, 1): [
        "Golden Ridge B / Start",
        "Golden Ridge B / Stepping Stones",
        "Golden Ridge B / Gusty Canyon",
        "Golden Ridge B / Eye Of The Storm",
    ],
    (5, 0): [
        "Mirror Temple A / Start",
        "Mirror Temple A / Depths",
        "Mirror Temple A / Unravelling",
        "Mirror Temple A / Search",
        "Mirror Temple A / Rescue",
    ],
    (5, 1): [
        "Mirror Temple B / Start",
        "Mirror Temple B / Central Chamber",
        "Mirror Temple B / Through The Mirror",
        "Mirror Temple B / Mix Master",
    ],
    (6, 0): [
        "Reflection A / Start",
        "Reflection A / Lake",
        "Reflection A / Hollows",
        "Reflection A / Reflection",
        "Reflection A / Rock Bottom",
        "Reflection A / Resolution",
    ],
    (6, 1): [
        "Reflection B / Start",
        "Reflection B / Reflection",
        "Reflection B / Rock Bottom",
        "Reflection B / Reprieve",
    ],
    (7, 0): [
        "The Summit A / Start",
        "The Summit A / 500M",
        "The Summit A / 1000M",
        "The Summit A / 1500M",
        "The Summit A / 2000M",
        "The Summit A / 2500M",
        "The Summit A / 3000M",
    ],
    (7, 1): [
        "The Summit B / Start",
        "The Summit B / 500M",
        "The Summit B / 1000M",
        "The Summit B / 1500M",
        "The Summit B / 2000M",
        "The Summit B / 2500M",
        "The Summit B / 3000M",
    ],
    (9, 0): [
        "Core A / Start",
        "Core A / Into The Core",
        "Core A / Hot And Cold",
        "Core A / Heart Of The Mountain",
    ],
    (9, 1): [
        "Core B / Start",
        "Core B / Into The Core",
        "Core B / Burning Or Freezing",
        "Core B / Heartbeat",
    ],
}

MODE_LETTERS = {0: "A", 1: "B"}
CHAPTER_LABELS = {
    0: "Prologue",
    1: "Forsaken City",
    2: "Old Site",
    3: "Celestial Resort",
    4: "Golden Ridge",
    5: "Mirror Temple",
    6: "Reflection",
    7: "The Summit",
    9: "Core",
}


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower())
    return slug.strip("_")


def short_name(chapter: int, mode: int, canonical_name: str) -> str:
    parts = canonical_name.split(" / ", 1)
    section = parts[1] if len(parts) > 1 else canonical_name
    if chapter == 0:
        prefix = CHAPTER_LABELS.get(chapter, "Chapter 0")
        return f"{prefix} {section}".strip()
    prefix = f"{chapter}{MODE_LETTERS.get(mode, '')}".strip()
    return f"{prefix} {section}".strip()


def build_checkpoint_defs() -> List[dict[str, object]]:
    entries: List[dict[str, object]] = []
    for (chapter, mode), names in CHECKPOINT_SPEC.items():
        for idx, canonical_name in enumerate(names):
            entries.append(
                {
                    "chapter": chapter,
                    "mode": mode,
                    "checkpoint_index": idx,
                    "canonical_name": canonical_name,
                    "short_name": short_name(chapter, mode, canonical_name),
                    "spreadsheet_name": slugify(canonical_name),
                }
            )
    return entries


CHECKPOINT_DEFS = build_checkpoint_defs()


CHECKPOINT_ALIASES = [
    {
        "alias_name": "Chapter 1 / Start",
        "chapter": 1,
        "mode": 0,
        "checkpoint_index": 0,
        "canonical_name": "Forsaken City A / Start",
    },
    {
        "alias_name": "Chapter 1 / Crossing",
        "chapter": 1,
        "mode": 0,
        "checkpoint_index": 1,
        "canonical_name": "Forsaken City A / Crossing",
    },
    {
        "alias_name": "Chapter 1 / Chasm",
        "chapter": 1,
        "mode": 0,
        "checkpoint_index": 2,
        "canonical_name": "Forsaken City A / Chasm",
    },
]


def upsert_checkpoint_defs(conn) -> int:
    sql = """
        INSERT INTO checkpoint_defs (
            chapter,
            mode,
            checkpoint_index,
            canonical_name,
            short_name,
            spreadsheet_name
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(chapter, mode, checkpoint_index) DO UPDATE SET
            canonical_name=excluded.canonical_name,
            short_name=excluded.short_name,
            spreadsheet_name=excluded.spreadsheet_name
    """
    with conn:
        conn.executemany(
            sql,
            [
                (
                    entry["chapter"],
                    entry["mode"],
                    entry["checkpoint_index"],
                    entry["canonical_name"],
                    entry.get("short_name"),
                    entry.get("spreadsheet_name"),
                )
                for entry in CHECKPOINT_DEFS
            ],
        )
    return len(CHECKPOINT_DEFS)


def upsert_aliases(conn) -> int:
    sql = """
        INSERT INTO checkpoint_aliases (
            alias_name,
            chapter,
            mode,
            checkpoint_index,
            canonical_name
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(alias_name) DO UPDATE SET
            chapter=excluded.chapter,
            mode=excluded.mode,
            checkpoint_index=excluded.checkpoint_index,
            canonical_name=excluded.canonical_name
    """
    with conn:
        conn.executemany(
            sql,
            [
                (
                    entry["alias_name"],
                    entry["chapter"],
                    entry["mode"],
                    entry["checkpoint_index"],
                    entry["canonical_name"],
                )
                for entry in CHECKPOINT_ALIASES
            ],
        )
    return len(CHECKPOINT_ALIASES)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed canonical checkpoints and aliases")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite database path (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = connect(args.db)
    try:
        initialize_schema(conn)
        defs_count = upsert_checkpoint_defs(conn)
        alias_count = upsert_aliases(conn)
    finally:
        conn.close()
    print(f"Seeded {defs_count} checkpoint defs and {alias_count} aliases")


if __name__ == "__main__":
    main()

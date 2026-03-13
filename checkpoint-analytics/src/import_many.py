"""Bulk CSV importer for Celeste checkpoint sessions."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from .importer import DuplicateSessionError, ImporterError, SESSION_TYPES, import_csv
from .schema import DEFAULT_DB_PATH


def gather_csv_paths(files: Iterable[Path], dirs: Iterable[Path]) -> list[Path]:
    collected: list[Path] = []

    def add_path(candidate: Path) -> None:
        resolved = candidate.expanduser().resolve()
        if resolved.suffix.lower() != ".csv":
            raise SystemExit(f"Not a CSV file: {resolved}")
        if not resolved.is_file():
            raise SystemExit(f"CSV not found: {resolved}")
        if resolved not in collected:
            collected.append(resolved)

    for file_path in files:
        if not file_path:
            continue
        add_path(file_path)

    for directory in dirs:
        dir_path = directory.expanduser().resolve()
        if not dir_path.is_dir():
            raise SystemExit(f"Directory not found: {dir_path}")
        for csv_file in sorted(dir_path.rglob("*.csv")):
            add_path(csv_file)

    collected.sort()
    return collected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bulk import checkpoint CSVs")
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Explicit CSV files to import",
    )
    parser.add_argument(
        "--dir",
        action="append",
        dest="dirs",
        default=[],
        type=Path,
        help="Directory containing CSV files (can be repeated)",
    )
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
            "Session type override applied to all imported sessions (default: auto). Allowed: "
            + ", ".join(SESSION_TYPES)
        ),
    )
    parser.add_argument("--category", help="Category label applied to all sessions")
    parser.add_argument("--notes", help="Notes applied to all sessions")

    args = parser.parse_args()
    if not args.paths and not args.dirs:
        parser.error("Provide at least one CSV path or --dir")
    return args


def main() -> None:
    args = parse_args()
    csv_paths = gather_csv_paths(args.paths, args.dirs)
    if not csv_paths:
        raise SystemExit("No CSV files found to import")

    imported = skipped = failed = 0
    failures: list[tuple[Path, str]] = []

    for csv_path in csv_paths:
        try:
            import_csv(
                csv_path,
                db_path=args.db,
                session_type=args.session_type,
                category=args.category,
                notes=args.notes,
            )
        except DuplicateSessionError as exc:
            skipped += 1
            print(f"SKIP: {csv_path} ({exc})")
        except ImporterError as exc:
            failed += 1
            failures.append((csv_path, str(exc)))
            print(f"FAIL: {csv_path} ({exc})")
        else:
            imported += 1
            print(f"IMPORTED: {csv_path}")

    summary = (
        f"Imported: {imported} | Skipped (duplicates): {skipped} | Failed: {failed}"
    )
    print(summary)

    if failures:
        print("Failed files:")
        for path, message in failures:
            print(f" - {path}: {message}")


if __name__ == "__main__":
    main()

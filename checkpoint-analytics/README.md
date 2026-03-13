# Celeste Checkpoint Analytics (v1)

Local-first tooling that ingests Celeste checkpoint logger CSVs into SQLite and exposes quick CLI stats for personal analysis.

## Project Overview

CSV exports from the community checkpoint logger are imported into a local SQLite database. From there, a CLI (`python -m src.stats …`) provides descriptive statistics, variability metrics, and practice-priority views without requiring any cloud services.

## Data Pipeline

1. Export CSV logs from the Celeste checkpoint logger.
2. Import the CSVs into SQLite using the helpers in `src/importer.py` or `src/import_many.py`.
3. Seed canonical checkpoint metadata once so alias/canonical projections work.
4. Query `src.stats` for personal analytics.

## Attempt Model

Sessions can contain multiple gameplay attempts. Each attempt is inferred automatically during CSV import.

Hierarchy:

```
session
  -> attempts
        -> segments
```

Attempts are detected using heuristics such as chapter resets and backward progression.

Attempt types currently supported:
- `full_run_complete`
- `full_run_incomplete`
- `chapter_practice`
- `checkpoint_grind`
- `undefined`

Full runs are detected when the attempt reaches Summit 3000M (chapter 7, mode 0, checkpoint_index 6).

## Project Layout

```
celeste-checkpoint-analytics/
  data/               # SQLite database + raw CSVs (not committed)
  src/
    schema.py         # create/validate schema
    importer.py       # import a single CSV
    import_many.py    # import batches of CSVs
    seed_checkpoints.py # canonical checkpoint + alias seeding
    stats.py          # raw + canonical stats CLI
```

Python 3.9+ and the stdlib are sufficient; no external packages are required.

## Initialize the Database

Create `data/celeste.db` (or any path you prefer) and bootstrap the schema:

```bash
python -m src.schema --db data/celeste.db
```

Tables created:
- `sessions`: one row per imported CSV/session, including timing metadata and optional labels.
- `segments`: checkpoint rows keyed to sessions with deterministic ordering and raw names preserved.
- `checkpoint_defs`: canonical checkpoint metadata keyed by `(chapter, mode, checkpoint_index)`.
- `checkpoint_aliases`: legacy/raw names mapped to canonical triplets for query-time normalization.

## Seed Canonical Checkpoints & Aliases

Run the seeding script after the schema exists (safe to rerun any time):

```bash
python -m src.seed_checkpoints --db data/celeste.db
```

This loads every logger-supported checkpoint for Prologue, Chapters 1–7, and Core (chapters 0,1,2,3,4,5,6,7,9 in A/B modes) and inserts conservative aliases such as `Chapter 1 / Start` → `Forsaken City A / Start`. Ambiguous historical names remain intentionally unmapped.

## Import Commands

### Single CSV

```bash
python -m src.importer path/to/checkpoints.csv --db data/celeste.db \
  --session-type chapter_practice --category "Any%" --notes "midnight grind"
```

- `csv_path` is required; the other flags populate optional `sessions` columns.
- Imports are idempotent per `source_file`: re-importing the same absolute CSV path raises a duplicate-session error.

If `--session-type` is omitted, the importer auto-classifies the session as one of:
- `full_run`
- `chapter_practice`
- `mixed_practice`

### Bulk CSVs

```bash
python -m src.import_many --dir ~/celeste/checkpoint_exports --db data/celeste.db
```

- Mix directories (`--dir`, repeatable) and explicit file paths.
- Files are resolved, sorted, and imported sequentially.
- Duplicates are skipped with a message; failures are reported but do not abort the batch.

## Statistics Commands

All analytics commands are local and only read from SQLite. The `src.stats` CLI accepts a global `--db` flag (defaulting to `data/celeste.db`).

### Core stats
- `pb` – per-checkpoint PB + attempt count using raw names.
- `checkpoint "Name"` – attempts, PB, average, worst, and death total for a specific raw checkpoint name.
- `chapter <chapter> <mode>` – ordered stats for one chapter/mode using raw names (e.g., `chapter 3 0` → Celestial Resort A).

### Canonical stats
- `canonical-pb` – PBs aggregated under canonical names using alias + metadata lookups.
- `canonical-chapter <chapter> <mode>` – chapter summary ordered by canonical checkpoint order, falling back to raw order where metadata is missing.

### Descriptive statistics layer
- `checkpoint-summary` – per-checkpoint distribution (attempt count, mean, sample stddev, min/max, percentiles, coefficient of variation).
- `checkpoint-consistency` – variability and potential time loss per checkpoint (IQR, relative IQR, range, PB gap, CV).
- `chapter-expected <chapter> <mode>` – aggregates checkpoint means/percentiles to estimate chapter completion time.
- `chapter-priority <chapter> <mode>` – ranks checkpoints within a chapter by PB gap, instability, and time share to suggest practice targets.
- `run-priority` – global ranking of checkpoints across the run for practice focus (uses the same metrics as chapter priority across every checkpoint).

Canonical variants (`canonical-checkpoint-summary`, `canonical-checkpoint-consistency`, `canonical-chapter-expected`, `canonical-chapter-priority`, `canonical-run-priority`) keep the same grouping identity but replace display names with canonical labels.

## Example Usage

```bash
python -m src.stats checkpoint-summary
python -m src.stats checkpoint-consistency
python -m src.stats chapter-priority 7 0
python -m src.stats run-priority
```

Global options (like `--db`) must precede the subcommand:

```bash
python -m src.stats --db data/celeste.db <command> [args]
```

All stats only include `segments.is_complete = 1`.

## Supported Coverage

`checkpoint_defs` includes every logger-provided checkpoint for Prologue, Chapters 1–7, and Core in both A- and B-sides (no Farewell/Epilogue/C-sides yet). Aliases currently cover the known safe `Chapter 1 / …` legacy labels.

## Known Limitations

- Ambiguous historical names (e.g., `Chapter 1 / Checkpoint 2`) are not auto-mapped; they appear under their raw strings until explicit aliases exist.
- Farewell, C-sides, modded levels, and other unsupported content are not part of the seeded metadata and will not canonicalize.
- Spreadsheet export/integration is not implemented; only SQLite + CLI reporting exist.
- The logger semantics are preserved as-is: cutscenes, save-and-quit segments, or other out-of-run events are not post-processed.

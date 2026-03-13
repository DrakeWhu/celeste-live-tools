#!/usr/bin/env python3
"""Room-based checkpoint logger.

Usage:
    python3 -m timer.checkpoint_logger
or python3 timer/checkpoint_logger.py

Writes CSV rows for each supported checkpoint segment into
`timer_data/checkpoint_logs/<timestamp>.csv`.

Requires the tracer to be streaming `/dev/shm/autosplitterinfo`. Only
vanilla chapters/sides listed in `ROOM_CHECKPOINTS` emit rows; others are
ignored on purpose. Checkpoint semantics follow the anchor rooms used by
CelesteAutosplitterCore and may differ from custom runner spreadsheets.
"""

import csv
import datetime as _dt
import importlib
import os
import sys
import time
from pathlib import Path

from .celeste_timer import AutoSplitterInfo

LOG_DIR = Path(__file__).resolve().parent.parent / "timer_data" / "checkpoint_logs"
POLL_INTERVAL = float(os.environ.get("CHECKPOINT_LOGGER_POLL", 0.01))
DEBUG_MODE = os.environ.get("CHECKPOINT_LOGGER_DEBUG") not in (None, "", "0")


def _env_truthy(name):
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() not in ("", "0", "false", "no", "off")


COACH_ENABLED = _env_truthy("CHECKPOINT_COACH_ENABLED")
COACH_DB = os.environ.get("CHECKPOINT_COACH_DB")
COACH_CONTEXT = os.environ.get("CHECKPOINT_COACH_CONTEXT", "any").strip() or "any"
COACH_ANALYTICS_ROOT = os.environ.get("CHECKPOINT_COACH_ANALYTICS_ROOT")
_COACH_LOADER_ATTEMPTED = False
_COACH_WARNING_EMITTED = False
_COACH_FEEDBACK_FOR_SEGMENT = None


def _warn_coach_once(message):
    global _COACH_WARNING_EMITTED
    if _COACH_WARNING_EMITTED:
        return
    print(f"Checkpoint coach unavailable: {message}")
    _COACH_WARNING_EMITTED = True


def _load_coach_feedback_for_segment():
    global _COACH_LOADER_ATTEMPTED, _COACH_FEEDBACK_FOR_SEGMENT

    if not COACH_ENABLED:
        return None
    if _COACH_LOADER_ATTEMPTED:
        return _COACH_FEEDBACK_FOR_SEGMENT

    _COACH_LOADER_ATTEMPTED = True

    if not COACH_DB:
        _warn_coach_once("set CHECKPOINT_COACH_DB to the analytics SQLite path")
        return None
    if not COACH_ANALYTICS_ROOT:
        _warn_coach_once("set CHECKPOINT_COACH_ANALYTICS_ROOT to the analytics repo root")
        return None

    analytics_root = Path(COACH_ANALYTICS_ROOT).expanduser().resolve()
    if not analytics_root.exists():
        _warn_coach_once(f"analytics root not found: {analytics_root}")
        return None

    analytics_root_str = str(analytics_root)
    if analytics_root_str not in sys.path:
        sys.path.insert(0, analytics_root_str)

    try:
        module = importlib.import_module("src.live_coach")
        coach_feedback = getattr(module, "coach_feedback_for_segment", None)
        if coach_feedback is None:
            _warn_coach_once("src.live_coach.coach_feedback_for_segment is missing")
            return None
        _COACH_FEEDBACK_FOR_SEGMENT = coach_feedback
        return _COACH_FEEDBACK_FOR_SEGMENT
    except Exception as exc:
        _warn_coach_once(str(exc))
        return None


def _print_coach_feedback(chapter, mode, checkpoint_index, checkpoint_name, segment_ms, cumulative_ms):
    coach_feedback_for_segment = _load_coach_feedback_for_segment()
    if coach_feedback_for_segment is None:
        return

    try:
        feedback = coach_feedback_for_segment(
            chapter=chapter,
            mode=mode,
            checkpoint_index=checkpoint_index,
            checkpoint_name=checkpoint_name,
            segment_ms=segment_ms,
            cumulative_ms=cumulative_ms,
            context=COACH_CONTEXT,
            db_path=Path(COACH_DB).expanduser(),
        )
    except Exception as exc:
        _warn_coach_once(str(exc))
        return

    if feedback:
        print(feedback)


def _anchor(index, label, room):
    return {"index": index, "label": label, "room": room}


# Anchor list mirrors the room triggers defined in CelesteAutosplitterCore.
ROOM_CHECKPOINTS = {
    (0, 0): [],
    (1, 0): [_anchor(1, "Crossing", "6"), _anchor(2, "Chasm", "9b")],
    (1, 1): [_anchor(1, "Contraption", "04"), _anchor(2, "Scrap Pit", "08")],
    (2, 0): [_anchor(1, "Intervention", "3"), _anchor(2, "Awake", "end_0")],
    (2, 1): [_anchor(1, "Combination Lock", "03"), _anchor(2, "Dream Altar", "08b")],
    (3, 0): [
        _anchor(1, "Huge Mess", "08-a"),
        _anchor(2, "Elevator Shaft", "09-d"),
        _anchor(3, "Presidential Suite", "00-d"),
    ],
    (3, 1): [
        _anchor(1, "Staff Quarters", "06"),
        _anchor(2, "Library", "11"),
        _anchor(3, "Rooftop", "16"),
    ],
    (4, 0): [
        _anchor(1, "Shrine", "b-00"),
        _anchor(2, "Old Trail", "c-00"),
        _anchor(3, "Cliff Face", "d-00"),
    ],
    (4, 1): [
        _anchor(1, "Stepping Stones", "b-00"),
        _anchor(2, "Gusty Canyon", "c-00"),
        _anchor(3, "Eye Of The Storm", "d-00"),
    ],
    (5, 0): [
        _anchor(1, "Depths", "b-00"),
        _anchor(2, "Unravelling", "c-00"),
        _anchor(3, "Search", "d-00"),
        _anchor(4, "Rescue", "e-00"),
    ],
    (5, 1): [
        _anchor(1, "Central Chamber", "b-00"),
        _anchor(2, "Through The Mirror", "c-00"),
        _anchor(3, "Mix Master", "d-00"),
    ],
    (6, 0): [
        _anchor(1, "Lake", "00"),
        _anchor(2, "Hollows", "04"),
        _anchor(3, "Reflection", "b-00"),
        _anchor(4, "Rock Bottom", "boss-00"),
        _anchor(5, "Resolution", "after-00"),
    ],
    (6, 1): [
        _anchor(1, "Reflection", "b-00"),
        _anchor(2, "Rock Bottom", "c-00"),
        _anchor(3, "Reprieve", "d-00"),
    ],
    (7, 0): [
        _anchor(1, "500M", "b-00"),
        _anchor(2, "1000M", "c-00"),
        _anchor(3, "1500M", "d-00"),
        _anchor(4, "2000M", "e-00b"),
        _anchor(5, "2500M", "f-00"),
        _anchor(6, "3000M", "g-00"),
    ],
    (7, 1): [
        _anchor(1, "500M", "b-00"),
        _anchor(2, "1000M", "c-01"),
        _anchor(3, "1500M", "d-00"),
        _anchor(4, "2000M", "e-00"),
        _anchor(5, "2500M", "f-00"),
        _anchor(6, "3000M", "g-00"),
    ],
    (9, 0): [
        _anchor(1, "Into The Core", "a-00"),
        _anchor(2, "Hot And Cold", "c-00"),
        _anchor(3, "Heart Of The Mountain", "d-00"),
    ],
    (9, 1): [
        _anchor(1, "Into The Core", "a-00"),
        _anchor(2, "Burning Or Freezing", "b-00"),
        _anchor(3, "Heartbeat", "c-01"),
    ],
}

CHAPTER_TITLES = {
    0: "Prologue",
    1: "Forsaken City",
    2: "Old Site",
    3: "Celestial Resort",
    4: "Golden Ridge",
    5: "Mirror Temple",
    6: "Reflection",
    7: "The Summit",
    8: "Epilogue",
    9: "Core",
    10: "Farewell",
}

MODE_SUFFIX = {0: "A", 1: "B", 2: "C"}


def _now_utc():
    return _dt.datetime.now(tz=_dt.timezone.utc)


def _iso(ts):
    return ts.isoformat()


def _checkpoint_name(chapter, checkpoint_index):
    return f"Chapter {chapter} / Checkpoint {checkpoint_index}"


def _mode_label(mode):
    return {0: "A", 1: "B", 2: "C"}.get(mode, str(mode))


def _chapter_label(chapter, mode):
    base = CHAPTER_TITLES.get(chapter, f"Chapter {chapter}")
    suffix = MODE_SUFFIX.get(mode)
    if suffix and chapter not in (0, 8, 10):
        return f"{base} {suffix}"
    return base


def _transition_end_reason(asi, tracker):
    if tracker.key == (5, 0) and tracker.checkpoint_index == 1 and asi.chapter_cassette:
        return "transition:mirror_temple_cassette"
    return None


def _has_player_control(asi):
    """Heuristic for when Madeline can move.

    We assume control when:
      * The player is inside a chapter (`chapter >= 0`).
      * The chapter has started (`chapter_started`).
      * The chapter is not complete (`not chapter_complete`).
      * The tracer reports `in_cutscene` is False.

    This mirrors how other scripts (e.g. death counter) infer active play.
    """

    return (
        asi.chapter >= 0
        and asi.chapter_started
        and not asi.chapter_complete
        and not asi.in_cutscene
    )


class SegmentTracker:
    def __init__(self, writer, outfile):
        self.writer = writer
        self.outfile = outfile
        self.reset_state()

    def reset_state(self):
        self.active = False
        self.chapter = None
        self.mode = None
        self.key = None
        self.chapter_label = None
        self.checkpoint_index = None
        self.checkpoint_label = None
        self.start_file_time = None
        self.start_death_count = None
        self.start_timestamp_iso = None
        self.next_anchor_idx = 0
        self.anchors = None
        self.pending_transition_end_reason = None

    def start_segment(self, asi, checkpoint_index, checkpoint_label):
        self.active = True
        self.chapter = asi.chapter
        self.mode = asi.mode
        self.key = (asi.chapter, asi.mode)
        self.checkpoint_index = checkpoint_index
        self.checkpoint_label = checkpoint_label
        self.start_file_time = asi.file_time
        self.start_death_count = asi.death_count
        self.start_timestamp_iso = _iso(_now_utc())
        if DEBUG_MODE:
            print(
                f"[segment] start chapter={self.chapter} mode={self.mode} "
                f"checkpoint={self.checkpoint_index} ({self.checkpoint_label}) level={asi.level_name}"
            )

    def _clear_active_segment(self):
        self.active = False
        self.checkpoint_index = None
        self.checkpoint_label = None
        self.start_file_time = None
        self.start_death_count = None
        self.start_timestamp_iso = None
        self.pending_transition_end_reason = None

    def end_segment(self, asi, reason):
        if not self.active:
            return
        end_time = _now_utc()
        end_timestamp_iso = _iso(end_time)
        end_file_time = asi.file_time
        segment_ms = max(0, end_file_time - self.start_file_time)
        cumulative_ms = max(0, end_file_time)
        deaths = max(0, asi.death_count - self.start_death_count)
        name = self.checkpoint_label or _checkpoint_name(self.chapter, self.checkpoint_index)
        row = [
            self.chapter,
            self.mode,
            _mode_label(self.mode),
            self.checkpoint_index,
            name,
            self.start_timestamp_iso,
            end_timestamp_iso,
            segment_ms,
            deaths,
            cumulative_ms,
        ]
        self.writer.writerow(row)
        self.outfile.flush()
        print(
            f"Logged {name}: {segment_ms} ms, deaths={deaths}, "
            f"chapter={self.chapter}, mode={self.mode}"
        )
        if COACH_ENABLED:
            _print_coach_feedback(
                chapter=self.chapter,
                mode=self.mode,
                checkpoint_index=self.checkpoint_index,
                checkpoint_name=name,
                segment_ms=segment_ms,
                cumulative_ms=cumulative_ms,
            )
        if DEBUG_MODE:
            print(f"[segment] end reason={reason} checkpoint={self.checkpoint_index}")
        self._clear_active_segment()

    def discard_segment(self, reason):
        if not self.active:
            return
        if DEBUG_MODE:
            print(f"[segment] discard reason={reason} checkpoint={self.checkpoint_index}")
        self._clear_active_segment()


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_name = _now_utc().strftime("%Y%m%d-%H%M%S") + ".csv"
    log_path = LOG_DIR / log_name
    asi = AutoSplitterInfo()
    last_debug_state = {
        "chapter": None,
        "mode": None,
        "chapter_checkpoints": None,
        "in_cutscene": None,
        "chapter_started": None,
        "chapter_complete": None,
        "level_name": None,
    }
    with log_path.open("w", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "chapter",
                "mode",
                "mode_label",
                "checkpoint_index",
                "checkpoint_name",
                "start_time_iso",
                "end_time_iso",
                "segment_ms",
                "deaths",
                "cumulative_ms",
            ]
        )
        fp.flush()
        tracker = SegmentTracker(writer, fp)
        print(f"Logging checkpoints to {log_path}")
        try:
            while True:
                if DEBUG_MODE:
                    current_state = {
                        "chapter": asi.chapter,
                        "mode": asi.mode,
                        "chapter_checkpoints": asi.chapter_checkpoints,
                        "in_cutscene": asi.in_cutscene,
                        "chapter_started": asi.chapter_started,
                        "chapter_complete": asi.chapter_complete,
                        "level_name": asi.level_name,
                    }
                    for key, last_val in last_debug_state.items():
                        current_val = current_state[key]
                        if current_val != last_val:
                            print(f"[state] {key}: {last_val} -> {current_val}")
                            last_debug_state[key] = current_val
                current_key = (asi.chapter, asi.mode)
                if tracker.active and tracker.key == current_key:
                    transition_end_reason = _transition_end_reason(asi, tracker)
                    if transition_end_reason is not None:
                        tracker.pending_transition_end_reason = transition_end_reason
                if tracker.key is not None and tracker.key != current_key:
                    if tracker.active:
                        if tracker.pending_transition_end_reason is not None:
                            tracker.end_segment(asi, tracker.pending_transition_end_reason)
                        else:
                            tracker.discard_segment("chapter_change")
                    tracker.reset_state()

                anchors = ROOM_CHECKPOINTS.get(current_key)
                if anchors is None:
                    tracker.reset_state()
                    time.sleep(POLL_INTERVAL)
                    continue

                tracker.anchors = anchors
                tracker.chapter_label = _chapter_label(asi.chapter, asi.mode)

                if not tracker.active and _has_player_control(asi):
                    tracker.start_segment(asi, 0, f"{tracker.chapter_label} / Start")

                if tracker.active and tracker.next_anchor_idx < len(tracker.anchors):
                    next_anchor = tracker.anchors[tracker.next_anchor_idx]
                    if asi.level_name == next_anchor["room"]:
                        tracker.end_segment(asi, f"room:{next_anchor['room']}")
                        tracker.start_segment(
                            asi,
                            next_anchor["index"],
                            f"{tracker.chapter_label} / {next_anchor['label']}"
                        )
                        tracker.next_anchor_idx += 1

                if (
                    tracker.active
                    and tracker.next_anchor_idx >= len(tracker.anchors)
                    and asi.chapter_complete
                ):
                    tracker.end_segment(asi, "chapter_complete")
                    tracker.reset_state()

                time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("Stopping checkpoint logger")


if __name__ == "__main__":
    main()

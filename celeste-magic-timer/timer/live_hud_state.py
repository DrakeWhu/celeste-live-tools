#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import importlib
import os
from pathlib import Path
import sys
import time

from typing import Callable, List, Optional, Protocol, Tuple

from .celeste_timer import AutoSplitterInfo
from .checkpoint_logger import CHAPTER_TITLES, MODE_SUFFIX, ROOM_CHECKPOINTS


class HudSemantic(str, Enum):
    GOLD = "gold"
    GOOD = "good"
    BAD = "bad"
    UNKNOWN = "unknown"


class MetricDisplay(str, Enum):
    CLOCK = "clock"
    DELTA = "delta"


class LiveSplitPhase(str, Enum):
    PAST = "past"
    CURRENT = "current"
    FUTURE = "future"


@dataclass(frozen=True)
class LiveMetric:
    label: str
    value_ms: Optional[int]
    display: MetricDisplay
    semantic: HudSemantic = HudSemantic.UNKNOWN


@dataclass(frozen=True)
class LiveSplitRow:
    name: str
    phase: LiveSplitPhase
    time_ms: Optional[int]
    delta_ms: Optional[int]
    semantic: HudSemantic


@dataclass(frozen=True)
class LiveRunState:
    run_label: str
    provider_label: str
    coach_context: Optional[str]
    analytics_status: str
    current_time_ms: int
    predicted_final: LiveMetric
    best_possible: LiveMetric
    pb_pace: LiveMetric
    median_pace: LiveMetric
    current_split_name: str
    split_rows: Tuple[LiveSplitRow, ...]


class LiveStateProvider(Protocol):
    def snapshot(self) -> LiveRunState:
        ...


@dataclass(frozen=True)
class _CoachSupport:
    get_cumulative_pace_stats: Callable[..., object]
    get_run_prediction: Callable[..., object]
    get_segment_history_stats: Callable[..., object]


@dataclass(frozen=True)
class _CompletedSegment:
    chapter: int
    mode: int
    checkpoint_index: int
    name: str
    segment_ms: Optional[int]
    cumulative_ms: int
    delta_ms: Optional[int]
    semantic: HudSemantic


@dataclass(frozen=True)
class _AnalyticsSnapshot:
    predicted_final_ms: Optional[int] = None
    best_possible_ms: Optional[int] = None
    pb_pace_ms: Optional[int] = None
    median_pace_ms: Optional[int] = None


def _format_checkpoint_label(chapter: int, mode: int, checkpoint_index: int) -> str:
    chapter_name = CHAPTER_TITLES.get(chapter, "Chapter %d" % chapter)
    mode_suffix = MODE_SUFFIX.get(mode)
    if mode_suffix and chapter not in (0, 8, 10):
        chapter_name = "%s %s" % (chapter_name, mode_suffix)

    if checkpoint_index == 0:
        return "%s / Start" % chapter_name

    anchors = ROOM_CHECKPOINTS.get((chapter, mode), ())
    for anchor in anchors:
        if anchor["index"] == checkpoint_index:
            return "%s / %s" % (chapter_name, anchor["label"])
    return "%s / Checkpoint %d" % (chapter_name, checkpoint_index)


def _has_player_control(asi: AutoSplitterInfo) -> bool:
    return (
        asi.chapter >= 0
        and asi.chapter_started
        and not asi.chapter_complete
        and not asi.in_cutscene
    )


def _delta_semantic(delta_ms: Optional[int]) -> HudSemantic:
    if delta_ms is None:
        return HudSemantic.UNKNOWN
    if delta_ms <= 0:
        return HudSemantic.GOOD
    return HudSemantic.BAD


def _load_coach_support(analytics_root: Optional[str]) -> Optional[_CoachSupport]:
    analytics_root_path = analytics_root
    if analytics_root_path:
        resolved_root = Path(analytics_root_path).expanduser().resolve()
        if not resolved_root.exists():
            return None
        resolved_root_text = str(resolved_root)
        if resolved_root_text not in sys.path:
            sys.path.insert(0, resolved_root_text)

    try:
        module = importlib.import_module("src.live_coach")
    except Exception:
        return None

    get_cumulative_pace_stats = getattr(module, "get_cumulative_pace_stats", None)
    get_run_prediction = getattr(module, "get_run_prediction", None)
    get_segment_history_stats = getattr(module, "get_segment_history_stats", None)
    if (
        get_cumulative_pace_stats is None
        or get_run_prediction is None
        or get_segment_history_stats is None
    ):
        return None

    return _CoachSupport(
        get_cumulative_pace_stats=get_cumulative_pace_stats,
        get_run_prediction=get_run_prediction,
        get_segment_history_stats=get_segment_history_stats,
    )


@dataclass(frozen=True)
class _DemoSegment:
    name: str
    actual_ms: int
    pb_ms: int
    gold_ms: Optional[int]
    median_ms: Optional[int]


class DemoLiveStateProvider:
    def __init__(self, start_offset_ms: int = 118000, visible_rows: int = 5):
        self._segments = (
            _DemoSegment("Start", 23400, 24200, 22800, 25000),
            _DemoSegment("Crossing", 48600, 46900, 45100, 49000),
            _DemoSegment("Shrine", 35300, 37100, 34700, 38000),
            _DemoSegment("Resort", 61200, 59000, 57800, 60400),
            _DemoSegment("Oshiro", 44700, 46000, 42900, 47100),
            _DemoSegment("Summit", 71500, 69400, 68100, 72000),
            _DemoSegment("Core", 52000, 53200, 50500, 54000),
        )
        self._visible_rows = max(3, visible_rows)
        self._started_at = time.monotonic()
        self._start_offset_ms = max(0, start_offset_ms)
        self._finish_hold_ms = 6000
        self._total_actual_ms = sum(segment.actual_ms for segment in self._segments)
        self._total_pb_ms = sum(segment.pb_ms for segment in self._segments)
        self._total_median_ms = sum(segment.median_ms for segment in self._segments if segment.median_ms is not None)

    def snapshot(self) -> LiveRunState:
        elapsed_since_boot_ms = int((time.monotonic() - self._started_at) * 1000)
        cycle_ms = self._total_actual_ms + self._finish_hold_ms
        cycle_position_ms = (elapsed_since_boot_ms + self._start_offset_ms) % cycle_ms
        run_elapsed_ms = min(cycle_position_ms, self._total_actual_ms)
        finished = cycle_position_ms >= self._total_actual_ms

        current_index, current_segment_elapsed_ms, completed_count = self._locate_segment(run_elapsed_ms)
        predicted_final_ms = self._predicted_final_ms(run_elapsed_ms, current_index, current_segment_elapsed_ms)
        best_possible_ms = self._best_possible_ms(run_elapsed_ms, current_index, current_segment_elapsed_ms)
        split_rows = self._build_rows(current_index, current_segment_elapsed_ms, completed_count)
        if finished:
            current_split_name = "Run Complete"
        else:
            current_split_name = self._segments[current_index].name

        return LiveRunState(
            run_label="Demo Any% HUD",
            provider_label="DEMO",
            coach_context=None,
            analytics_status="mock",
            current_time_ms=run_elapsed_ms,
            predicted_final=LiveMetric("Predicted Final", predicted_final_ms, MetricDisplay.CLOCK),
            best_possible=LiveMetric("Best Possible", best_possible_ms, MetricDisplay.CLOCK),
            pb_pace=LiveMetric(
                "Projected vs PB",
                None if predicted_final_ms is None else predicted_final_ms - self._total_pb_ms,
                MetricDisplay.DELTA,
                _delta_semantic(None if predicted_final_ms is None else predicted_final_ms - self._total_pb_ms),
            ),
            median_pace=LiveMetric(
                "Projected vs Median",
                None if predicted_final_ms is None else predicted_final_ms - self._total_median_ms,
                MetricDisplay.DELTA,
                _delta_semantic(None if predicted_final_ms is None else predicted_final_ms - self._total_median_ms),
            ),
            current_split_name=current_split_name,
            split_rows=split_rows,
        )

    def _locate_segment(self, run_elapsed_ms: int) -> Tuple[Optional[int], Optional[int], int]:
        remaining_ms = run_elapsed_ms
        for index, segment in enumerate(self._segments):
            if remaining_ms < segment.actual_ms:
                return index, remaining_ms, index
            remaining_ms -= segment.actual_ms
        return None, None, len(self._segments)

    def _predicted_final_ms(
        self,
        run_elapsed_ms: int,
        current_index: Optional[int],
        current_segment_elapsed_ms: Optional[int],
    ) -> Optional[int]:
        if current_index is None or current_segment_elapsed_ms is None:
            return run_elapsed_ms
        remaining_pb_ms = max(self._segments[current_index].pb_ms - current_segment_elapsed_ms, 0)
        remaining_pb_ms += sum(segment.pb_ms for segment in self._segments[current_index + 1 :])
        return run_elapsed_ms + remaining_pb_ms

    def _best_possible_ms(
        self,
        run_elapsed_ms: int,
        current_index: Optional[int],
        current_segment_elapsed_ms: Optional[int],
    ) -> Optional[int]:
        if current_index is None or current_segment_elapsed_ms is None:
            return run_elapsed_ms
        current_gold_ms = self._segments[current_index].gold_ms
        if current_gold_ms is None:
            return None
        remaining_gold_ms = max(current_gold_ms - current_segment_elapsed_ms, 0)
        for segment in self._segments[current_index + 1 :]:
            if segment.gold_ms is None:
                return None
            remaining_gold_ms += segment.gold_ms
        return run_elapsed_ms + remaining_gold_ms

    def _build_rows(
        self,
        current_index: Optional[int],
        current_segment_elapsed_ms: Optional[int],
        completed_count: int,
    ) -> Tuple[LiveSplitRow, ...]:
        focus_index = len(self._segments) - 1 if current_index is None else current_index
        start = max(0, focus_index - 2)
        end = min(len(self._segments), start + self._visible_rows)
        start = max(0, end - self._visible_rows)

        rows = []
        for index in range(start, end):
            segment = self._segments[index]
            if index < completed_count:
                delta_ms = segment.actual_ms - segment.pb_ms
                rows.append(
                    LiveSplitRow(
                        name=segment.name,
                        phase=LiveSplitPhase.PAST,
                        time_ms=segment.actual_ms,
                        delta_ms=delta_ms,
                        semantic=self._segment_semantic(segment.actual_ms, segment.pb_ms, segment.gold_ms, allow_gold=True),
                    )
                )
            elif index == current_index:
                delta_ms = None if current_segment_elapsed_ms is None else current_segment_elapsed_ms - segment.pb_ms
                rows.append(
                    LiveSplitRow(
                        name=segment.name,
                        phase=LiveSplitPhase.CURRENT,
                        time_ms=current_segment_elapsed_ms,
                        delta_ms=delta_ms,
                        semantic=self._segment_semantic(
                            current_segment_elapsed_ms,
                            segment.pb_ms,
                            segment.gold_ms,
                            allow_gold=False,
                        ),
                    )
                )
            else:
                rows.append(
                    LiveSplitRow(
                        name=segment.name,
                        phase=LiveSplitPhase.FUTURE,
                        time_ms=segment.pb_ms,
                        delta_ms=None,
                        semantic=HudSemantic.UNKNOWN,
                    )
                )
        return tuple(rows)

    @staticmethod
    def _delta_semantic(delta_ms: Optional[int]) -> HudSemantic:
        return _delta_semantic(delta_ms)

    @staticmethod
    def _segment_semantic(
        segment_time_ms: Optional[int],
        pb_ms: Optional[int],
        gold_ms: Optional[int],
        allow_gold: bool,
    ) -> HudSemantic:
        if segment_time_ms is None or pb_ms is None:
            return HudSemantic.UNKNOWN
        if allow_gold and gold_ms is not None and segment_time_ms < gold_ms:
            return HudSemantic.GOLD
        if segment_time_ms <= pb_ms:
            return HudSemantic.GOOD
        return HudSemantic.BAD


class RealLiveStateProvider:
    def __init__(
        self,
        asi_path: Optional[str] = None,
        analytics_root: Optional[str] = None,
        coach_db_path: Optional[str] = None,
        coach_context: Optional[str] = None,
        visible_rows: int = 5,
    ):
        tracer_path = asi_path or os.environ.get("ASI_PATH", "/dev/shm/autosplitterinfo")
        self._asi = AutoSplitterInfo(filename=tracer_path)
        self._coach_support = _load_coach_support(
            analytics_root or os.environ.get("CHECKPOINT_COACH_ANALYTICS_ROOT")
        )
        coach_db_value = coach_db_path or os.environ.get("CHECKPOINT_COACH_DB")
        self._coach_db_path = None if not coach_db_value else Path(coach_db_value).expanduser().resolve()
        self._coach_context = (coach_context or os.environ.get("CHECKPOINT_COACH_CONTEXT", "any")).strip() or "any"
        self._visible_rows = max(3, visible_rows)

        self._completed_segments: List[_CompletedSegment] = []
        self._analytics_snapshot = _AnalyticsSnapshot()
        self._last_seen_file_time: Optional[int] = None
        self._initialized = False
        self._previous_player_control = False

        self._active_key: Optional[Tuple[int, int]] = None
        self._active_checkpoint_index: Optional[int] = None
        self._active_label: Optional[str] = None
        self._active_start_ms: Optional[int] = None
        self._next_anchor_idx = 0
        self._pending_transition_completion = False

    def snapshot(self) -> LiveRunState:
        self._refresh_tracking()

        current_run_time_ms = max(0, int(self._asi.file_time))
        current_split_name = self._current_split_name()
        split_rows = self._build_rows(current_run_time_ms)

        return LiveRunState(
            run_label="Real Checkpoint HUD",
            provider_label="REAL",
            coach_context=self._coach_context,
            analytics_status="OK" if self._coach_support is not None else "unavailable",
            current_time_ms=current_run_time_ms,
            predicted_final=LiveMetric(
                "Predicted Final",
                self._analytics_snapshot.predicted_final_ms,
                MetricDisplay.CLOCK,
            ),
            best_possible=LiveMetric(
                "Best Possible",
                self._analytics_snapshot.best_possible_ms,
                MetricDisplay.CLOCK,
            ),
            pb_pace=LiveMetric(
                "PB Pace",
                self._analytics_snapshot.pb_pace_ms,
                MetricDisplay.DELTA,
                _delta_semantic(self._analytics_snapshot.pb_pace_ms),
            ),
            median_pace=LiveMetric(
                "Median Pace",
                self._analytics_snapshot.median_pace_ms,
                MetricDisplay.DELTA,
                _delta_semantic(self._analytics_snapshot.median_pace_ms),
            ),
            current_split_name=current_split_name,
            split_rows=split_rows,
        )

    def _refresh_tracking(self) -> None:
        current_run_time_ms = max(0, int(self._asi.file_time))
        player_control = _has_player_control(self._asi)
        allow_known_start = False

        if (
            self._last_seen_file_time is not None
            and current_run_time_ms < self._last_seen_file_time
        ):
            self._reset_run_tracking()
        self._last_seen_file_time = current_run_time_ms

        if self._active_key == (5, 0) and self._active_checkpoint_index == 1 and self._asi.chapter_cassette:
            self._pending_transition_completion = True

        current_key = (self._asi.chapter, self._asi.mode)
        if self._active_key is not None and current_key != self._active_key:
            if self._pending_transition_completion:
                self._complete_active_segment(current_run_time_ms)
            self._reset_chapter_tracking()
            allow_known_start = True

        anchors = ROOM_CHECKPOINTS.get(current_key)
        if anchors is None:
            self._previous_player_control = player_control
            self._initialized = True
            self._reset_chapter_tracking()
            return

        if self._active_key is None and player_control:
            if allow_known_start or (self._initialized and not self._previous_player_control):
                checkpoint_index = 0
                start_time_ms = current_run_time_ms
            else:
                checkpoint_index = self._current_checkpoint_index(current_key)
                start_time_ms = None
            self._next_anchor_idx = checkpoint_index
            self._start_active_segment(
                current_key[0],
                current_key[1],
                checkpoint_index,
                start_time_ms,
            )

        if self._active_key is None:
            self._previous_player_control = player_control
            self._initialized = True
            return

        if self._next_anchor_idx < len(anchors):
            next_anchor = anchors[self._next_anchor_idx]
            if self._asi.level_name == next_anchor["room"]:
                self._complete_active_segment(current_run_time_ms)
                self._start_active_segment(
                    current_key[0],
                    current_key[1],
                    next_anchor["index"],
                    current_run_time_ms,
                )
                self._next_anchor_idx += 1

        if (
            self._active_key is not None
            and self._next_anchor_idx >= len(anchors)
            and self._asi.chapter_complete
        ):
            self._complete_active_segment(current_run_time_ms)
            self._reset_chapter_tracking()

        self._previous_player_control = player_control
        self._initialized = True

    def _reset_run_tracking(self) -> None:
        self._completed_segments = []
        self._analytics_snapshot = _AnalyticsSnapshot()
        self._reset_chapter_tracking()

    def _reset_chapter_tracking(self) -> None:
        self._active_key = None
        self._active_checkpoint_index = None
        self._active_label = None
        self._active_start_ms = None
        self._next_anchor_idx = 0
        self._pending_transition_completion = False

    def _start_active_segment(
        self,
        chapter: int,
        mode: int,
        checkpoint_index: int,
        current_run_time_ms: Optional[int],
    ) -> None:
        self._active_key = (chapter, mode)
        self._active_checkpoint_index = checkpoint_index
        self._active_label = _format_checkpoint_label(chapter, mode, checkpoint_index)
        self._active_start_ms = current_run_time_ms
        self._pending_transition_completion = False

    def _complete_active_segment(self, current_run_time_ms: int) -> None:
        if (
            self._active_key is None
            or self._active_checkpoint_index is None
            or self._active_label is None
        ):
            return

        segment_ms = None
        segment_stats = None
        if self._active_start_ms is not None:
            segment_ms = max(0, current_run_time_ms - self._active_start_ms)
            segment_stats = self._get_segment_history_stats(
                chapter=self._active_key[0],
                mode=self._active_key[1],
                checkpoint_index=self._active_checkpoint_index,
                checkpoint_name=self._active_label,
                current_segment_ms=segment_ms,
            )

        delta_ms = None
        semantic = HudSemantic.UNKNOWN
        if segment_stats is not None:
            delta_ms = int(round(segment_stats.delta_vs_median_ms))
            if segment_stats.is_gold:
                semantic = HudSemantic.GOLD
            else:
                semantic = _delta_semantic(delta_ms)

        self._completed_segments.append(
            _CompletedSegment(
                chapter=self._active_key[0],
                mode=self._active_key[1],
                checkpoint_index=self._active_checkpoint_index,
                name=self._active_label,
                segment_ms=segment_ms,
                cumulative_ms=current_run_time_ms,
                delta_ms=delta_ms,
                semantic=semantic,
            )
        )
        if len(self._completed_segments) > 64:
            self._completed_segments = self._completed_segments[-64:]

        self._analytics_snapshot = self._build_analytics_snapshot(
            chapter=self._active_key[0],
            mode=self._active_key[1],
            checkpoint_index=self._active_checkpoint_index,
            cumulative_ms=current_run_time_ms,
        )

        self._active_key = None
        self._active_checkpoint_index = None
        self._active_label = None
        self._active_start_ms = None
        self._pending_transition_completion = False

    def _build_analytics_snapshot(
        self,
        chapter: int,
        mode: int,
        checkpoint_index: int,
        cumulative_ms: int,
    ) -> _AnalyticsSnapshot:
        run_prediction = self._get_run_prediction(
            chapter=chapter,
            mode=mode,
            checkpoint_index=checkpoint_index,
            current_cumulative_ms=cumulative_ms,
        )
        cumulative_pace = self._get_cumulative_pace_stats(
            chapter=chapter,
            mode=mode,
            checkpoint_index=checkpoint_index,
            current_cumulative_ms=cumulative_ms,
        )

        return _AnalyticsSnapshot(
            predicted_final_ms=None if run_prediction is None else run_prediction.predicted_final_ms,
            best_possible_ms=None if run_prediction is None else run_prediction.best_possible_ms,
            pb_pace_ms=None if cumulative_pace is None else int(round(cumulative_pace.delta_vs_pb_ms)),
            median_pace_ms=None if cumulative_pace is None else int(round(cumulative_pace.delta_vs_median_ms)),
        )

    def _coach_kwargs(self) -> dict:
        kwargs = {}
        if self._coach_db_path is not None:
            kwargs["db_path"] = self._coach_db_path
        return kwargs

    def _get_segment_history_stats(self, **kwargs):
        if self._coach_support is None:
            return None
        try:
            return self._coach_support.get_segment_history_stats(
                context=self._coach_context,
                **kwargs,
                **self._coach_kwargs(),
            )
        except Exception:
            return None

    def _get_run_prediction(self, **kwargs):
        if self._coach_support is None:
            return None
        try:
            return self._coach_support.get_run_prediction(
                **kwargs,
                **self._coach_kwargs(),
            )
        except Exception:
            return None

    def _get_cumulative_pace_stats(self, **kwargs):
        if self._coach_support is None:
            return None
        try:
            return self._coach_support.get_cumulative_pace_stats(
                **kwargs,
                **self._coach_kwargs(),
            )
        except Exception:
            return None

    def _current_split_name(self) -> str:
        if self._active_label is not None:
            return self._active_label

        current_key = (self._asi.chapter, self._asi.mode)
        if current_key in ROOM_CHECKPOINTS:
            return _format_checkpoint_label(
                current_key[0],
                current_key[1],
                self._current_checkpoint_index(current_key),
            )
        if self._asi.level_name:
            return self._asi.level_name
        return "Waiting for live data"

    def _build_rows(self, current_run_time_ms: int) -> Tuple[LiveSplitRow, ...]:
        rows = [
            LiveSplitRow(
                name=segment.name,
                phase=LiveSplitPhase.PAST,
                time_ms=segment.segment_ms,
                delta_ms=segment.delta_ms,
                semantic=segment.semantic,
            )
            for segment in self._completed_segments[-2:]
        ]

        if self._active_label is not None and self._active_start_ms is not None:
            rows.append(
                LiveSplitRow(
                    name=self._active_label,
                    phase=LiveSplitPhase.CURRENT,
                    time_ms=max(0, current_run_time_ms - self._active_start_ms),
                    delta_ms=None,
                    semantic=HudSemantic.UNKNOWN,
                )
            )
            upcoming_index = 0 if self._active_checkpoint_index is None else self._active_checkpoint_index + 1
            rows.extend(self._upcoming_rows(upcoming_index))
        else:
            current_checkpoint_index = self._current_checkpoint_index((self._asi.chapter, self._asi.mode))
            rows.append(
                LiveSplitRow(
                    name=self._current_split_name(),
                    phase=LiveSplitPhase.CURRENT,
                    time_ms=None,
                    delta_ms=None,
                    semantic=HudSemantic.UNKNOWN,
                )
            )
            rows.extend(self._upcoming_rows(current_checkpoint_index + 1))

        if len(rows) < self._visible_rows:
            rows.extend(
                LiveSplitRow(
                    name="Waiting for split data",
                    phase=LiveSplitPhase.FUTURE,
                    time_ms=None,
                    delta_ms=None,
                    semantic=HudSemantic.UNKNOWN,
                )
                for _ in range(self._visible_rows - len(rows))
            )

        return tuple(rows[: self._visible_rows])

    def _upcoming_rows(self, start_index: int) -> List[LiveSplitRow]:
        current_key = (self._asi.chapter, self._asi.mode)
        if current_key not in ROOM_CHECKPOINTS:
            return []

        anchors = ROOM_CHECKPOINTS[current_key]
        max_index = len(anchors)
        rows = []
        for checkpoint_index in range(start_index, max_index + 1):
            rows.append(
                LiveSplitRow(
                    name=_format_checkpoint_label(current_key[0], current_key[1], checkpoint_index),
                    phase=LiveSplitPhase.FUTURE,
                    time_ms=None,
                    delta_ms=None,
                    semantic=HudSemantic.UNKNOWN,
                )
            )
            if len(rows) >= self._visible_rows:
                break
        return rows

    def _current_checkpoint_index(self, current_key: Tuple[int, int]) -> int:
        anchors = ROOM_CHECKPOINTS.get(current_key, ())
        checkpoint_index = max(0, int(self._asi.chapter_checkpoints))
        return min(checkpoint_index, len(anchors))

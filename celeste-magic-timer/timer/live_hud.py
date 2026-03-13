#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Sequence

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from .live_hud_state import (
    DemoLiveStateProvider,
    HudSemantic,
    LiveMetric,
    LiveRunState,
    LiveSplitPhase,
    LiveSplitRow,
    LiveStateProvider,
    MetricDisplay,
    RealLiveStateProvider,
)


SEMANTIC_COLORS = {
    HudSemantic.GOLD: "#ffcc4d",
    HudSemantic.GOOD: "#30d158",
    HudSemantic.BAD: "#ff453a",
    HudSemantic.UNKNOWN: "#a6adb8",
}

PRIMARY_TEXT = "#f5f1e8"
SECONDARY_TEXT = "#aeb7c4"
PANEL_FILL = "rgba(18, 24, 32, 0.78)"
ROW_FILL = "rgba(20, 28, 38, 0.82)"
CURRENT_FILL = "rgba(41, 56, 74, 0.94)"
ROW_BORDER = "rgba(201, 215, 229, 0.14)"


def fmt_time(value_ms: int, ms_decimals: int = 3, sign: bool = False) -> str:
    negative = value_ms < 0
    if negative:
        value_ms = -value_ms

    milliseconds = value_ms % 1000
    seconds = value_ms // 1000 % 60
    minutes = value_ms // 1000 // 60 % 60
    hours = value_ms // 1000 // 60 // 60

    if ms_decimals > 0:
        if ms_decimals == 1:
            milliseconds //= 100
        elif ms_decimals == 2:
            milliseconds //= 10
        milliseconds_text = (".%%0%dd" % ms_decimals) % milliseconds
    else:
        milliseconds_text = ""

    seconds_text = "%02d" % seconds if hours or minutes else "%d" % seconds
    minutes_text = "%02d:" % minutes if hours else ("%d:" % minutes if minutes else "")
    hours_text = "%d:" % hours if hours else ""
    sign_text = "-" if negative else "+" if sign else ""
    return sign_text + hours_text + minutes_text + seconds_text + milliseconds_text


def format_clock(value_ms: Optional[int], decimals: int = 1) -> str:
    if value_ms is None:
        return "--"
    if value_ms >= 600000:
        decimals = 0
    return fmt_time(value_ms, ms_decimals=decimals)


def format_delta(value_ms: Optional[int]) -> str:
    if value_ms is None:
        return "--"
    decimals = 1 if abs(value_ms) < 600000 else 0
    return fmt_time(value_ms, ms_decimals=decimals, sign=True)


def metric_text(metric: LiveMetric) -> str:
    if metric.display == MetricDisplay.DELTA:
        return format_delta(metric.value_ms)
    return format_clock(metric.value_ms)


class MetricCard(QFrame):
    def __init__(self, metric: LiveMetric):
        super().__init__()
        self.setObjectName("metricCard")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(4)

        self._label = QLabel(metric.label.upper())
        self._label.setObjectName("metricLabel")

        self._value = QLabel(metric_text(metric))
        self._value.setObjectName("metricValue")
        self._value.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)

        layout.addWidget(self._label)
        layout.addWidget(self._value)

        self.update_metric(metric)

    def update_metric(self, metric: LiveMetric) -> None:
        self._label.setText(metric.label.upper())
        self._value.setText(metric_text(metric))
        accent = SEMANTIC_COLORS[metric.semantic]
        self._value.setStyleSheet("color: %s;" % accent)


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    text = hex_color.lstrip("#")
    return int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16)


def _rgba(hex_color: str, alpha: float) -> str:
    r, g, b = _hex_to_rgb(hex_color)
    return "rgba(%d, %d, %d, %.3f)" % (r, g, b, alpha)


class SplitRowWidget(QFrame):
    def __init__(self):
        super().__init__()
        self.setObjectName("splitRow")
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        self._accent = QFrame()
        self._accent.setFixedWidth(8)
        self._accent.setObjectName("splitAccent")

        text_column = QVBoxLayout()
        text_column.setContentsMargins(0, 0, 0, 0)
        text_column.setSpacing(2)

        self._name = QLabel()
        self._name.setObjectName("splitName")

        self._phase = QLabel()
        self._phase.setObjectName("splitPhase")

        text_column.addWidget(self._name)
        text_column.addWidget(self._phase)

        values_column = QVBoxLayout()
        values_column.setContentsMargins(0, 0, 0, 0)
        values_column.setSpacing(2)

        self._time = QLabel()
        self._time.setObjectName("splitTime")
        self._time.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        self._delta = QLabel()
        self._delta.setObjectName("splitDelta")
        self._delta.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        values_column.addWidget(self._time)
        values_column.addWidget(self._delta)

        layout.addWidget(self._accent)
        layout.addLayout(text_column, 1)
        layout.addLayout(values_column)

    def update_row(self, row: LiveSplitRow) -> None:
        accent_color = SEMANTIC_COLORS[row.semantic]
        self._accent.setStyleSheet(
            "background-color: %s; border-radius: 4px;" % accent_color
        )

        is_current = row.phase == LiveSplitPhase.CURRENT
        background = CURRENT_FILL if is_current else ROW_FILL
        border_color = _rgba(accent_color, 0.55) if (is_current or row.semantic == HudSemantic.GOLD) else ROW_BORDER
        self.setStyleSheet(
            "background-color: %s; border-radius: 12px; border: 1px solid %s;" % (background, border_color)
        )

        self._name.setText(row.name)
        self._name.setStyleSheet(
            "color: %s; font-weight: %s;"
            % (PRIMARY_TEXT, "700" if is_current else "500")
        )

        phase_text = {
            LiveSplitPhase.PAST: "recent split",
            LiveSplitPhase.CURRENT: "current split",
            LiveSplitPhase.FUTURE: "upcoming split",
        }[row.phase]
        self._phase.setText(phase_text)
        self._phase.setStyleSheet("color: %s;" % SECONDARY_TEXT)

        time_text = format_clock(row.time_ms)
        if row.phase == LiveSplitPhase.CURRENT:
            time_text = format_clock(row.time_ms, decimals=2)
        self._time.setText(time_text)
        self._time.setStyleSheet("color: %s;" % PRIMARY_TEXT)

        if row.phase == LiveSplitPhase.FUTURE:
            self._delta.setText("up next")
            self._delta.setStyleSheet("color: %s;" % SECONDARY_TEXT)
        else:
            self._delta.setText(format_delta(row.delta_ms))
            self._delta.setStyleSheet(
                "color: %s; background-color: %s; padding: 2px 8px; border-radius: 9px; font-weight: 800;"
                % ("#0b0f14" if row.semantic in (HudSemantic.GOLD, HudSemantic.GOOD) else "#fff4f2", _rgba(accent_color, 0.92))
            )


class LiveHudWindow(QMainWindow):
    def __init__(self, provider: LiveStateProvider, refresh_ms: int = 100):
        super().__init__()
        self._provider = provider
        self._metric_cards: List[MetricCard] = []
        self._split_rows: List[SplitRowWidget] = []

        self.setWindowTitle("Celeste Live HUD MVP")
        self.resize(460, 720)
        self.setMinimumSize(420, 640)

        central = QWidget()
        central.setObjectName("root")
        self.setCentralWidget(central)

        layout = QVBoxLayout(central)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        self._run_label = QLabel()
        self._run_label.setObjectName("runLabel")

        self._status_line = QLabel()
        self._status_line.setObjectName("statusLine")

        self._timer_panel = self._build_panel()
        timer_layout = QVBoxLayout(self._timer_panel)
        timer_layout.setContentsMargins(18, 16, 18, 16)
        timer_layout.setSpacing(8)

        self._current_time = QLabel()
        self._current_time.setObjectName("currentTime")

        self._current_split_caption = QLabel("CURRENT SPLIT")
        self._current_split_caption.setObjectName("sectionLabel")

        self._current_split_name = QLabel()
        self._current_split_name.setObjectName("currentSplitName")

        timer_layout.addWidget(self._current_time)
        timer_layout.addWidget(self._current_split_caption)
        timer_layout.addWidget(self._current_split_name)

        metrics_panel = self._build_panel()
        metrics_layout = QGridLayout(metrics_panel)
        metrics_layout.setContentsMargins(12, 12, 12, 12)
        metrics_layout.setHorizontalSpacing(10)
        metrics_layout.setVerticalSpacing(10)

        state = self._provider.snapshot()
        metrics = [state.predicted_final, state.best_possible, state.pb_pace, state.median_pace]
        for index, metric in enumerate(metrics):
            card = MetricCard(metric)
            self._metric_cards.append(card)
            metrics_layout.addWidget(card, index // 2, index % 2)

        split_panel = self._build_panel()
        split_layout = QVBoxLayout(split_panel)
        split_layout.setContentsMargins(12, 12, 12, 12)
        split_layout.setSpacing(8)

        split_header = QLabel("RECENT / CURRENT / UPCOMING")
        split_header.setObjectName("sectionLabel")
        split_layout.addWidget(split_header)

        for _ in state.split_rows:
            row_widget = SplitRowWidget()
            self._split_rows.append(row_widget)
            split_layout.addWidget(row_widget)

        split_layout.addStretch(1)

        layout.addWidget(self._run_label)
        layout.addWidget(self._status_line)
        layout.addWidget(self._timer_panel)
        layout.addWidget(metrics_panel)
        layout.addWidget(split_panel, 1)

        self._install_fonts()
        self._install_styles()
        self._apply_state(state)

        self._timer = QTimer(self)
        self._timer.setInterval(refresh_ms)
        self._timer.timeout.connect(self._refresh)
        self._timer.start()

    @staticmethod
    def _build_panel() -> QFrame:
        panel = QFrame()
        panel.setObjectName("panel")
        return panel

    def _install_fonts(self) -> None:
        self._run_label.setFont(QFont("DejaVu Sans Condensed", 9, 600))
        self._current_time.setFont(QFont("JetBrains Mono", 31, 700))
        self._current_split_name.setFont(QFont("DejaVu Sans Condensed", 18, 600))

    def _install_styles(self) -> None:
        self.setStyleSheet(
            """
            #root {
                background-color: qlineargradient(
                    x1: 0, y1: 0, x2: 1, y2: 1,
                    stop: 0 #10161d,
                    stop: 0.55 #17212c,
                    stop: 1 #0b1016
                );
            }
            #panel {
                background-color: rgba(18, 24, 32, 0.78);
                border: 1px solid rgba(201, 215, 229, 0.11);
                border-radius: 16px;
            }
            #runLabel {
                color: #c9d7e5;
            }
            #statusLine {
                color: rgba(201, 215, 229, 0.78);
                font-size: 12px;
                font-family: "JetBrains Mono", "DejaVu Sans Mono";
            }
            #currentTime {
                color: #f7f2e9;
            }
            #sectionLabel, #metricLabel {
                color: #93a1b0;
                font-size: 11px;
                font-weight: 600;
            }
            #currentSplitName {
                color: #f5f1e8;
            }
            #metricValue {
                color: #f5f1e8;
                font-size: 18px;
                font-family: "JetBrains Mono", "DejaVu Sans Mono";
                font-weight: 700;
            }
            #splitName {
                font-size: 15px;
            }
            #splitPhase {
                font-size: 11px;
            }
            #splitTime {
                color: #f5f1e8;
                font-size: 15px;
                font-family: "JetBrains Mono", "DejaVu Sans Mono";
                font-weight: 700;
            }
            #splitDelta {
                font-size: 12px;
                font-family: "JetBrains Mono", "DejaVu Sans Mono";
                font-weight: 800;
            }
            """
        )

    def _refresh(self) -> None:
        self._apply_state(self._provider.snapshot())

    def _apply_state(self, state: LiveRunState) -> None:
        self._run_label.setText(state.run_label.upper())
        self._status_line.setText(self._format_status(state))
        self._current_time.setText(format_clock(state.current_time_ms, decimals=2))
        self._current_split_name.setText(state.current_split_name)

        metrics = [state.predicted_final, state.best_possible, state.pb_pace, state.median_pace]
        for card, metric in zip(self._metric_cards, metrics):
            card.update_metric(metric)

        for widget, row in zip(self._split_rows, state.split_rows):
            widget.update_row(row)

    @staticmethod
    def _format_status(state: LiveRunState) -> str:
        parts = ["%s" % state.provider_label]
        if state.coach_context:
            parts.append("context=%s" % state.coach_context)
        parts.append("analytics=%s" % state.analytics_status)
        return " | ".join(parts)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch the PySide6 live HUD MVP.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--demo", action="store_true", help="Use the built-in demo state provider (default).")
    mode_group.add_argument("--real", action="store_true", help="Use the real tracer/checkpoint provider.")
    parser.add_argument("--refresh-ms", type=int, default=100, help="UI refresh interval in milliseconds.")
    parser.add_argument("--asi-path", help="Path to autosplitterinfo for real mode.")
    parser.add_argument("--analytics-root", help="Path to the checkpoint analytics repo root for real mode.")
    parser.add_argument("--coach-db", help="Path to the checkpoint analytics SQLite database for real mode.")
    parser.add_argument(
        "--coach-context",
        help="Segment history context for real mode (for example: any, full_run, chapter_practice).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    app = QApplication(sys.argv)
    app.setApplicationName("Celeste Live HUD MVP")

    if args.real:
        provider = RealLiveStateProvider(
            asi_path=args.asi_path,
            analytics_root=args.analytics_root,
            coach_db_path=args.coach_db,
            coach_context=args.coach_context,
        )
    else:
        provider = DemoLiveStateProvider()

    window = LiveHudWindow(provider, refresh_ms=max(16, args.refresh_ms))
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

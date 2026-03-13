# Celeste Live Tools

Lightweight LiveSplit-style HUD and checkpoint analytics for **Celeste on Linux**.

This project provides a real-time HUD and logging system designed for Celeste runners who want a native Linux workflow without relying on LiveSplit through Wine.

It combines:

* real-time checkpoint detection
* automatic split logging
* analytics-driven pacing estimates
* a compact stream-friendly HUD

---

## Features

* **Real-time checkpoint logger**
* **Live HUD overlay**
* **Split delta tracking**
* **Predicted final time**
* **Best possible time**
* **PB pace / median pace estimates**
* **Compact layout that works in non-maximized windows**

The tool is designed to behave similarly to LiveSplit while remaining simple and native to Linux.

---

## Screenshot



Example HUD during a run:

* real-time timer
* recent splits with deltas
* predicted final time
* pace metrics

---

## Repository structure

celeste-live-tools/

celeste-magic-timer/

checkpoint-analytics/

scripts/

* **celeste-magic-timer** contains the live HUD and tracer.
* **checkpoint-analytics** handles split analytics and pace estimation.
* **scripts** provides convenience commands to launch the system.

---

## Running the HUD

Typical usage:

scripts/celog --hud --full-run

This will:

1. start the checkpoint tracer
2. log checkpoint events
3. launch the live HUD
4. feed analytics into the display

---

## HUD panels

### Timer panel

Displays the live run timer and progress estimate.

### Metrics panel

Shows pacing analytics:

* predicted final
* best possible
* PB pace
* median pace

### Split list

Shows:

* recent splits
* current split
* upcoming splits

Each completed split shows its **delta relative to expected pace**.

---

## Requirements

* Linux
* Python 3.10+
* PySide6

Install dependencies:

pip install -r celeste-magic-timer/requirements.txt

---

## Motivation

LiveSplit works well on Windows but the Linux ecosystem lacks a native solution for Celeste runners.

This project was created to provide:

* a native Linux HUD
* checkpoint-level analytics
* a lightweight workflow that integrates easily with OBS and local tooling

---

## Status

This project is currently an **MVP**.

The HUD and analytics pipeline are functional and usable for real runs.

Contributions and feedback from Celeste runners are welcome.

---

## License

See LICENSE file.

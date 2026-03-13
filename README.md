# Celeste Live Tools

Linux-native speedrunning toolkit for Celeste.

This project provides:

- a real-time HUD similar to LiveSplit
- automatic checkpoint logging
- run analytics and predictions based on historical attempts

It builds on top of CelesteMagicTimer and extends it with a live HUD and an analytics engine.


## Architecture

celeste-live-tools/

├── celeste-magic-timer/  
│   tracer + checkpoint logger + live HUD  
│
├── checkpoint-analytics/  
│   run analytics engine (imports, stats, predictions)  
│
└── scripts/  
    celog   → start tracer + logger (+ HUD optional)  
    cesync  → import CSV logs into the analytics database


## Requirements

Linux  
Python 3.10+  
PySide6  

Install Python dependencies:

    pip install -r celeste-magic-timer/requirements.txt


## Quick start

Start the tracer, checkpoint logger and HUD:

    scripts/celog --hud --full-run

Chapter practice mode:

    scripts/celog --hud --practice


## Import runs into analytics

After generating CSV logs, import them into the analytics database:

    scripts/cesync

This will:

- import new CSV logs
- update analytics
- compute run statistics


## HUD features

- live run timer
- split comparison against historical runs
- semantic split coloring (gold / ahead / behind)
- predicted final time (for full runs)
- checkpoint-aware transitions (including cassette transitions such as 5A → 5B)


## Credits

This project builds on top of:

CelesteMagicTimer  
https://github.com/Watfaq/CelesteMagicTimer


## License

See LICENSE file for details.

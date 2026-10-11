"""Keep runtime state and private notification settings outside the source tree."""
from __future__ import annotations

import os
from pathlib import Path

_default_base = Path(os.environ.get("LOCALAPPDATA") or os.environ.get("XDG_DATA_HOME") or (Path.home() / ".local" / "share"))
DATA_DIR = Path(os.environ.get("INDEX_MONITOR_DATA_DIR") or (_default_base / "IndexDrawdownMonitor")).expanduser().resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

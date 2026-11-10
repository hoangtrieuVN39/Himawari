"""Standard replay review slots, loaded from config/replay_slots.json."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

DEFAULT_REPLAY_SLOTS = (
    "2026-11-09T07:00:00Z",
    "2026-11-09T08:00:00Z",
    "2026-11-09T11:00:00Z",
)
DEFAULT_CONFIG_PATH = "config/replay_slots.json"


def _validate(slot: str) -> str:
    try:
        datetime.strptime(slot, "%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid replay slot '{slot}' (expected YYYY-MM-DDTHH:MM:SSZ)") from exc
    return slot


def load_replay_slots(config_path: str = DEFAULT_CONFIG_PATH) -> list[str]:
    """Returns sorted, de-duplicated replay slots; falls back to the built-in defaults if the file is absent."""
    path = Path(config_path)
    if not path.exists():
        return list(DEFAULT_REPLAY_SLOTS)
    data = json.loads(path.read_text(encoding="utf-8"))
    return sorted({_validate(s) for s in data.get("replay_slots_utc", [])})

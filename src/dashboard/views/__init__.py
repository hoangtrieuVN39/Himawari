"""Per-area Streamlit renderers. app.py owns data loading and passes it in through ViewContext,
so tests that patch loaders on `src.dashboard.app` keep controlling what every view sees."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ViewContext:
    storage_root: str
    slots: dict[str, list[dict[str, Any]]]
    selected_slot: str
    rows: list[dict[str, Any]]
    is_live_mode: bool
    latest_slot: str
    sort_label: str
    deps: dict[str, Callable[..., Any]] = field(default_factory=dict)


# Session-state keys shared between views (Tab 1 reads the KPI trial state to hide answers).
KPI_VIEW_MODE_KEY = "kpi_view_mode"
KPI_VIEW_DEFAULT = "baseline_3_sources"
# View mode frozen when "Bắt đầu nhận ca" is pressed; the switch is locked until submit/reset.
TRIAL_VIEW_MODE_KEY = "trial_view_mode"


def active_trial_view_mode(session_state: Any) -> str:
    """View mode of the open trial (frozen at Start), else the currently selected one."""
    return session_state.get(TRIAL_VIEW_MODE_KEY) or session_state.get(KPI_VIEW_MODE_KEY) or KPI_VIEW_DEFAULT


def baseline_trial_in_progress(session_state: Any) -> bool:
    """True while a Baseline trial is open: answers must stay hidden on the Decision tab."""
    return bool(session_state.get("timer_has_started", False)) and active_trial_view_mode(session_state) == "baseline_3_sources"

"""KPI trial state machine (pure; no Streamlit).

Actions come from the browser trial component: {"type": start|stop|reset|submit, "nonce", "client_ts"}.
`client_ts` is the browser's Date.now() at the click, so client elapsed = difference of two timestamps
taken in the same browser (no server/network latency). The server keeps its own perf_counter measure
(`elapsed_seconds`, used by the KPI summary); `client_elapsed_seconds` is recorded alongside.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, MutableMapping

NOT_STARTED_MSG = "Chưa bắt đầu lượt thử. Hãy bấm “Bắt đầu nhận ca” trước khi nộp."
NO_AIRPORT_MSG = "Chọn sân bay cần rà soát đầu tiên trước khi nộp."
NO_REASON_MSG = "Chọn lý do căn cứ trước khi nộp."
TRIAL_VIEW_MODE_KEY = "trial_view_mode"
CLIENT_TOLERANCE_S = 2.0
_NONCE_KEY = "kpi_handled_nonces"
_NONCE_KEEP = 50


@dataclass
class SubmitData:
    server_elapsed: float
    client_elapsed: float | None
    view_mode: str
    airport: str
    reason: str
    obs_time: str


@dataclass
class TrialOutcome:
    handled: bool = True
    warning: str | None = None
    submit: SubmitData | None = None
    clear_answers: bool = False
    rerun_app: bool = False


def validate_client_elapsed(client: float | None, server: float) -> float | None:
    """Browser-measured seconds, or None when missing/implausible (negative, NaN, > server + 2 s)."""
    if client is None:
        return None
    try:
        value = float(client)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0 or value > server + CLIENT_TOLERANCE_S:
        return None
    return round(value, 2)


def _client_seconds(start_ms: Any, end_ms: Any) -> float | None:
    try:
        return (float(end_ms) - float(start_ms)) / 1000.0
    except (TypeError, ValueError):
        return None


def apply_trial_action(
    state: MutableMapping[str, Any],
    action: dict[str, Any],
    *,
    now: float,
    view_mode: str,
    answers: dict[str, Any],
) -> TrialOutcome:
    """Applies one action to the session state. `now` is time.perf_counter() on the server."""
    # A nonce is mandatory: it is what makes a re-delivered trigger (same value across reruns) harmless.
    nonce = action.get("nonce")
    if not isinstance(nonce, str) or not nonce:
        return TrialOutcome(handled=False)
    seen = list(state.get(_NONCE_KEY) or [])
    if nonce in seen:
        return TrialOutcome(handled=False)
    state[_NONCE_KEY] = (seen + [nonce])[-_NONCE_KEEP:]

    kind = action.get("type")
    client_ts = action.get("client_ts")

    if kind == "start":
        state.update(timer_state="running", timer_start=now, timer_elapsed=0.0, timer_has_started=True,
                     last_kpi_result=None, client_t0=client_ts, client_stop=None)
        state[TRIAL_VIEW_MODE_KEY] = view_mode
        return TrialOutcome(clear_answers=True, rerun_app=True)

    if kind == "stop":
        if state.get("timer_state") == "running":
            state["timer_state"] = "stopped"
            state["timer_elapsed"] = round(max(0.0, now - state["timer_start"]), 1)
            state["client_stop"] = client_ts
        return TrialOutcome(rerun_app=True)

    if kind == "reset":
        state.update(timer_state="idle", timer_start=0.0, timer_elapsed=0.0, timer_has_started=False,
                     last_kpi_result=None, client_t0=None, client_stop=None)
        state.pop(TRIAL_VIEW_MODE_KEY, None)
        return TrialOutcome(clear_answers=True, rerun_app=True)

    if kind != "submit":
        return TrialOutcome(handled=False)

    if not state.get("timer_has_started", False):
        return TrialOutcome(warning=NOT_STARTED_MSG)
    if not answers.get("kpi_airport"):
        return TrialOutcome(warning=NO_AIRPORT_MSG)
    if not answers.get("kpi_reason"):
        return TrialOutcome(warning=NO_REASON_MSG)

    if state.get("timer_state") == "running":
        server_elapsed = round(max(0.0, now - state["timer_start"]), 1)
        state["timer_state"] = "stopped"
        state["timer_elapsed"] = server_elapsed
        client_end = client_ts
    else:
        server_elapsed = float(state.get("timer_elapsed", 0.0))
        client_end = state.get("client_stop")

    submit = SubmitData(
        server_elapsed=server_elapsed,
        client_elapsed=validate_client_elapsed(_client_seconds(state.get("client_t0"), client_end), server_elapsed),
        view_mode=state.get(TRIAL_VIEW_MODE_KEY) or view_mode,
        airport=answers["kpi_airport"],
        reason=answers["kpi_reason"],
        obs_time=answers.get("kpi_obs_time") or "",
    )
    # Re-arm: the next trial needs a fresh "Bắt đầu nhận ca" (prevents duplicate records).
    state["timer_has_started"] = False
    state.pop(TRIAL_VIEW_MODE_KEY, None)
    return TrialOutcome(submit=submit, rerun_app=True)

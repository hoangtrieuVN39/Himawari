"""Browser-side KPI trial controls (st.components.v2).

Two instances share this component:
- mode="timer": live clock ticking in the browser + Bắt đầu / Dừng / Đặt lại buttons.
- mode="submit": the "Nộp & chấm điểm" button.
Each click sends a one-shot trigger {type, nonce, client_ts=Date.now()} to Python, so the server never
has to rerun every second to animate the clock and the client timestamps carry no network latency.
All markup is static or built with textContent; no external data is injected as HTML.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

_CSS = """
:host { display:block; font-family: system-ui, "Segoe UI", Roboto, Arial, sans-serif; color:#101828; }
.clock { border:1px solid #E4E7EC; border-radius:12px; background:#fff; padding:14px; text-align:center; }
.state { display:inline-flex; align-items:center; gap:6px; font-size:12px; font-weight:700; color:#475467; }
.dot { width:7px; height:7px; border-radius:50%; background:currentColor; }
.running .state { color:#067647; } .running .dot { background:#12B76A; animation:p 1.2s infinite; }
.stopped .state { color:#B54708; }
@keyframes p { 50% { opacity:.35; } }
.time { font: 800 44px/1.1 ui-monospace, "Cascadia Mono", Consolas, monospace; margin-top:4px; letter-spacing:.02em; }
.sub { font: 12px ui-monospace, Consolas, monospace; color:#475467; }
.row { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:8px; }
button { font: 600 14px system-ui, "Segoe UI", Roboto, Arial, sans-serif; min-height:38px; border-radius:8px;
  border:1px solid #D0D5DD; background:#fff; color:#101828; cursor:pointer; width:100%; }
button:hover { background:#F9FAFB; }
button.primary { background:#1F2937; border-color:#1F2937; color:#fff; margin-top:10px; }
button.primary:hover { background:#111827; }
button:focus-visible { outline:2px solid #175CD3; outline-offset:2px; }
button:disabled { opacity:.5; cursor:not-allowed; }
"""

_JS = """
export default function (component) {
  const { data, setTriggerValue, parentElement } = component;
  const d = data || {};
  let root = parentElement.querySelector(".kt-root");
  if (!root) { root = document.createElement("div"); root.className = "kt-root"; parentElement.appendChild(root); }
  if (root._timer) { clearInterval(root._timer); root._timer = null; }
  root.replaceChildren();

  const fire = (type) => setTriggerValue("action", {
    type, client_ts: Date.now(),
    nonce: (self.crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now()) + Math.random(),
  });
  const btn = (label, type, primary) => {
    const b = document.createElement("button");
    b.type = "button"; b.textContent = label; if (primary) b.className = "primary";
    b.addEventListener("click", () => { b.disabled = true; fire(type); });
    return b;
  };

  if (d.mode === "submit") { root.appendChild(btn("Nộp & chấm điểm", "submit", true)); return; }

  const labels = { idle: "Chưa bắt đầu", running: "Đang chạy", stopped: "Đã dừng" };
  const st = labels[d.state] ? d.state : "idle";
  const card = document.createElement("div"); card.className = "clock " + st;
  const state = document.createElement("div"); state.className = "state";
  const dot = document.createElement("span"); dot.className = "dot";
  state.append(dot, document.createTextNode(labels[st]));
  const time = document.createElement("div"); time.className = "time";
  const sub = document.createElement("div"); sub.className = "sub";
  card.append(state, time, sub);

  const t0 = Number(d.client_t0);
  const base = Number(d.server_elapsed) || 0;
  const mountedAt = Date.now();
  const seconds = () => {
    if (st !== "running") return base;
    if (Number.isFinite(t0) && t0 > 0) return Math.max(0, (Date.now() - t0) / 1000);
    return base + (Date.now() - mountedAt) / 1000;
  };
  const paint = () => {
    const s = seconds(), whole = Math.floor(s);
    time.textContent = String(Math.floor(whole / 60)).padStart(2, "0") + ":" + String(whole % 60).padStart(2, "0");
    sub.textContent = s.toFixed(1) + " giây";
  };
  paint();
  if (st === "running") root._timer = setInterval(paint, 200);

  const row = document.createElement("div"); row.className = "row";
  row.append(btn("Dừng & ghi nhận", "stop", false), btn("Đặt lại", "reset", false));
  root.append(card, btn("Bắt đầu nhận ca", "start", true), row);
  return () => { if (root._timer) clearInterval(root._timer); };
}
"""

_renderer = st.components.v2.component("dll_kpi_trial", css=_CSS, js=_JS)


def _noop() -> None:
    return None


def trial_controls(mode: str, key: str, **data: Any) -> dict[str, Any] | None:
    """Mounts one instance and returns the action triggered by the last click (or None)."""
    result = _renderer(key=key, data={"mode": mode, **data}, on_action_change=_noop)
    action = getattr(result, "action", None)
    if action is None and isinstance(result, dict):
        action = result.get("action")
    return action if isinstance(action, dict) else None

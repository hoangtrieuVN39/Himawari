"""Client-side Himawari-9 B13 time-lapse viewer.

All frames are embedded (base64) in one self-contained HTML document rendered with
streamlit.components.v1.html, so moving between frames happens entirely in the browser:
no Streamlit rerun, no server round trip. Pure function + signature cache; no Streamlit import.
"""

from __future__ import annotations

import base64
import html
import json
from pathlib import Path
import threading
from typing import Any

VIEWER_HEIGHT_PX = 600
_CACHE: dict[tuple, str] = {}
_CACHE_MAX = 32
_LOCK = threading.Lock()


def _frame_signature(frames: list[dict[str, Any]]) -> tuple:
    sig = []
    for f in frames:
        p = Path(f["png_path"])
        try:
            st = p.stat()
            sig.append((str(p), st.st_mtime_ns, st.st_size, f.get("utc_hhmm"), f.get("vn_hhmm")))
        except OSError:
            sig.append((str(p), None, None, f.get("utc_hhmm"), f.get("vn_hhmm")))
    return tuple(sig)


def build_viewer_html(frames: list[dict[str, Any]], icao: str) -> str:
    """Self-contained HTML for the frames (oldest → newest), defaulting to the newest frame.

    Frames whose file cannot be read are skipped. With a single frame the slider is omitted.
    Returns "" when no frame is readable (caller shows its own empty state).
    """
    key = (icao, _frame_signature(frames))
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    items = []
    for f in frames:
        try:
            data = Path(f["png_path"]).read_bytes()
        except OSError:
            continue
        items.append({
            "src": "data:image/png;base64," + base64.b64encode(data).decode("ascii"),
            "label": f"{f.get('utc_hhmm', '')} · {f.get('vn_hhmm', '')} ICT",
            "utc": f.get("utc_hhmm", ""),
        })
    if not items:
        return ""

    n = len(items)
    safe_icao = html.escape(icao)
    # Labels are only ever assigned via textContent; escaping every "<" keeps the payload inert inside
    # <script> (no </script>, <!-- or <script> sequence can appear; < is the same string to JS).
    payload = json.dumps(items).replace("<", "\\u003c")
    js_icao = json.dumps(icao).replace("<", "\\u003c")
    ticks = "".join(f'<span>{html.escape(it["utc"])}</span>' for it in items)
    slider = (
        f'<div class="ctl"><input id="r" type="range" min="0" max="{n - 1}" step="1" value="{n - 1}" '
        f'aria-label="Khung thời gian ảnh vệ tinh {safe_icao}">'
        f'<div class="ticks">{ticks}</div><div id="pos" class="pos"></div></div>'
        if n > 1 else ""
    )
    doc = f"""<!doctype html><html><head><meta charset="utf-8"><style>
:root{{font-family:system-ui,"Segoe UI",Roboto,Arial,sans-serif;color:#101828}}
body{{margin:0;background:transparent}}
.img{{width:100%;height:{VIEWER_HEIGHT_PX - (110 if n > 1 else 40)}px;display:flex;align-items:center;justify-content:center;
background:#F9FAFB;border-radius:8px;overflow:hidden}}
.img img{{max-width:100%;max-height:100%;object-fit:contain}}
.cap{{font:12px ui-monospace,Consolas,monospace;color:#475467;text-align:center;margin-top:6px}}
.ctl{{margin-top:8px}} input[type=range]{{width:100%;accent-color:#1F2937;cursor:pointer}}
input[type=range]:focus-visible{{outline:2px solid #175CD3;outline-offset:2px}}
.ticks{{display:flex;justify-content:space-between;font:11px ui-monospace,Consolas,monospace;color:#667085}}
.pos{{font-size:12px;color:#475467;margin-top:4px}}
</style></head><body>
<div class="img"><img id="im" alt="Himawari-9 B13 {safe_icao}"></div>
<div id="cap" class="cap"></div>{slider}
<script>
const F={payload};const im=document.getElementById("im"),cap=document.getElementById("cap"),
r=document.getElementById("r"),pos=document.getElementById("pos");
function show(i){{im.src=F[i].src;cap.textContent={js_icao}+" "+F[i].label;
if(pos)pos.textContent="Khung "+(i+1)+"/"+F.length+" · "+F[i].utc;}}
F.forEach(f=>{{const p=new Image();p.src=f.src;}});
if(r)r.addEventListener("input",e=>show(+e.target.value));
show(F.length-1);
</script></body></html>"""
    with _LOCK:  # Streamlit runs sessions on separate threads
        while len(_CACHE) >= _CACHE_MAX:
            _CACHE.pop(next(iter(_CACHE)), None)
        _CACHE[key] = doc
    return doc

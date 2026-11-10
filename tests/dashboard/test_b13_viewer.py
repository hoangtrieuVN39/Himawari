"""Client-side B13 viewer HTML (frames embedded; navigation never reruns Streamlit)."""

import base64
import re

from src.dashboard import b13_viewer as v


def _frames(tmp_path, names):
    out = []
    for i, name in enumerate(names):
        p = tmp_path / f"B13_VVDN_20261109_{name}Z.png"
        p.write_bytes(b"\x89PNG-" + name.encode())
        out.append({"png_path": str(p), "utc_hhmm": f"{name[:2]}:{name[2:]}Z", "vn_hhmm": f"{int(name[:2]) + 7:02d}:{name[2:]}"})
    return out


def test_frames_embedded_in_order_and_newest_selected(tmp_path):
    frames = _frames(tmp_path, ["0710", "0720", "0730"])
    doc = v.build_viewer_html(frames, "VVDN")
    srcs = re.findall(r'"src": "data:image/png;base64,([^"]+)"', doc)
    assert [base64.b64decode(s) for s in srcs] == [b"\x89PNG-0710", b"\x89PNG-0720", b"\x89PNG-0730"]
    assert 'type="range" min="0" max="2" step="1" value="2"' in doc
    assert "07:10Z" in doc and "07:30Z" in doc and "show(F.length-1)" in doc
    assert 'aria-label="Khung thời gian ảnh vệ tinh VVDN"' in doc


def test_single_frame_has_no_slider(tmp_path):
    doc = v.build_viewer_html(_frames(tmp_path, ["0700"]), "VVTS")
    assert 'type="range"' not in doc and "data:image/png;base64," in doc


def test_unreadable_frames_skipped_and_empty_when_none(tmp_path):
    frames = _frames(tmp_path, ["0710", "0720"])
    frames[0]["png_path"] = str(tmp_path / "missing.png")
    doc = v.build_viewer_html(frames, "VVNB")
    assert 'type="range"' not in doc  # only one readable frame left
    assert v.build_viewer_html([{"png_path": str(tmp_path / "x.png"), "utc_hhmm": "", "vn_hhmm": ""}], "VVNB") == ""


def test_payload_cannot_break_out_of_script(tmp_path):
    frames = _frames(tmp_path, ["0710"])
    frames[0]["utc_hhmm"] = "</script><img src=x onerror=alert(1)>"
    doc = v.build_viewer_html(frames, "<b>VVDN</b>")
    script = doc.split("<script>", 1)[1]
    assert "</script><img" not in script
    assert "<b>VVDN</b>" not in doc.split("<script>", 1)[0]


def test_cached_until_a_frame_file_changes(tmp_path, monkeypatch):
    frames = _frames(tmp_path, ["0710", "0720"])
    first = v.build_viewer_html(frames, "VVDN")
    monkeypatch.setattr(v.Path, "read_bytes", lambda self: (_ for _ in ()).throw(AssertionError("re-read")))
    assert v.build_viewer_html(frames, "VVDN") is first
    monkeypatch.undo()
    import os
    st = os.stat(frames[1]["png_path"])
    os.utime(frames[1]["png_path"], ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))
    assert v.build_viewer_html(frames, "VVDN") is not first


def test_cache_is_bounded_and_survives_concurrent_sessions(tmp_path):
    import threading

    frames = _frames(tmp_path, ["0710"])
    errors = []

    def worker(n):
        try:
            for i in range(60):
                v.build_viewer_html(frames, f"AP{n}-{i}")  # distinct keys force constant eviction
        except Exception as exc:  # noqa: BLE001 - the point is to catch any race error
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == []
    assert len(v._CACHE) <= v._CACHE_MAX


def test_script_payload_escapes_every_angle_bracket(tmp_path):
    frames = _frames(tmp_path, ["0710"])
    frames[0]["utc_hhmm"] = "<!-- <script> -->"
    script = v.build_viewer_html(frames, "VVDN").split("<script>", 1)[1].rsplit("</script>", 1)[0]
    assert "<" not in script.replace("<\\u003c", "")  # only our own JS comparisons (none) may remain
    assert "\\u003c!--" in script


def test_six_real_size_frames_stay_under_one_megabyte(tmp_path):
    frames = _frames(tmp_path, ["0710", "0720", "0730", "0740", "0750", "0800"])
    for f in frames:
        open(f["png_path"], "wb").write(b"\0" * 100 * 1024)  # repo PNGs average ~100 KB
    assert len(v.build_viewer_html(frames, "VVDN").encode()) < 1_000_000

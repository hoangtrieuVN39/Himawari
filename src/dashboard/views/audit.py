"""Tab "Đối soát dữ liệu": row conservation, quarantine log, binary file errors, 5V & HDFS stats."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard import components as c
from src.dashboard.data_loader import normalize_timestamp_pair
from src.dashboard.views import ViewContext

QUARANTINE_PREVIEW_ROWS = 4


def _short(value: Any, limit: int = 60) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _quarantine_row(rec: dict[str, Any]) -> list[Any]:
    codes = rec.get("error_codes") or rec.get("error_code") or rec.get("error_hint") or rec.get("status") or "—"
    if isinstance(codes, list):
        codes = ", ".join(str(x) for x in codes)
    src = rec.get("input_file_path") or rec.get("file") or rec.get("source") or "—"
    payload = rec.get("raw_payload", rec.get("payload", ""))
    synthetic = rec.get("is_synthetic_test")
    syn_txt = "Có" if synthetic else ("Không" if synthetic is not None else "—")
    return [codes, src, _short(payload), syn_txt]


def _quarantine(audit: dict[str, Any]) -> None:
    records = audit.get("quarantine_records") or []
    synthetic = False
    if not records:
        records = audit.get("synthetic_quarantine_preview") or []
        synthetic = True
    right = c.pill(f"{len(records)} bản ghi · hiển thị {min(len(records), QUARANTINE_PREVIEW_ROWS)}", "neutral") if records else ""
    st.html(c.section_header("2 · Nhật ký bản ghi cách ly", None, right))
    if not records:
        st.info("Không có bản ghi cách ly. Bật “Bơm dữ liệu lỗi kiểm thử” trong Quản trị kỹ thuật rồi chạy lại pipeline để kiểm thử.")
        return
    if synthetic:
        st.caption("Pipeline chưa ghi bản ghi lỗi thật — đang hiển thị bản xem trước các ca lỗi kiểm thử (synthetic).")
    st.html(c.data_table(
        ["Mã lỗi", "File nguồn", "Payload thô", "Giả lập"],
        [_quarantine_row(r) for r in records[:QUARANTINE_PREVIEW_ROWS]],
        mono_cols=(0, 1, 2),
    ))
    if len(records) > QUARANTINE_PREVIEW_ROWS:
        with st.expander(f"Xem tất cả {len(records)} bản ghi"):
            st.dataframe(
                pd.DataFrame([_quarantine_row(r) for r in records], columns=["Mã lỗi", "File nguồn", "Payload thô", "Giả lập"]),
                width="stretch", hide_index=True,
            )


def render_audit(ctx: ViewContext) -> None:
    audit = ctx.deps["load_audit_and_reconciliation"](ctx.storage_root) or {}
    recon = audit.get("reconciliation") or {}
    generated = recon.get("generated_at_utc")
    right = ""
    if generated:
        info = normalize_timestamp_pair(generated)
        right = f'<span class="muted">Báo cáo lúc {c._e(info["utc_hhmm"])} ({c._e(info["vn_hhmm"])} ICT) · {c._e(generated[:10])}</span>'
    st.html(c.section_header("Đối soát, cách ly & lưu trữ", "Khu vực kỹ thuật dành cho giảng viên và nhóm phát triển", right))

    # 1. Row conservation
    sources = recon.get("sources") or []
    status = ""
    if sources:
        all_ok = bool(recon.get("all_sources_reconciled")) and all(s.get("is_reconciled") for s in sources)
        status = c.pill(f"Cả {len(sources)} nguồn bảo toàn 100%", "ok") if all_ok else c.pill("Có nguồn lệch dòng", "danger")
    st.html(c.section_header("1 · Bảo toàn dòng", "đọc = hợp lệ + cách ly · hợp lệ = giữ lại + trùng", status))
    if sources:
        st.html(c.reconciliation_table(sources))
        for line in c.reconciliation_mismatches(sources):
            st.error(f"**LỆCH DÒNG** · {line}")
    else:
        st.info("Chưa có báo cáo đối soát. Chạy lại pipeline để tạo.")

    # 2. Quarantine
    _quarantine(audit)

    # 3. Binary errors
    errors = audit.get("binary_file_errors") or []
    st.html(c.section_header("3 · Lỗi tệp nhị phân vệ tinh", None, c.pill(f"{len(errors)} tệp", "neutral") if errors else ""))
    if errors:
        st.html(c.binary_error_cards(errors))
    else:
        st.info("Không có lỗi tệp nhị phân nào được ghi nhận.")

    # 4. 5V & HDFS
    ingest = audit.get("ingestion_summary") or {}
    total_bytes = ingest.get("total_bytes", 0) or 0
    st.html(c.section_header("4 · Thống kê 5V & HDFS"))
    st.html(c.tile_grid([
        c.stat_tile("Tổng log thu nạp", f"{int(ingest.get('total_logs', 0) or 0):,}"),
        c.stat_tile("Tổng dung lượng", c.format_bytes(total_bytes), f"{int(total_bytes):,} byte"),
        c.stat_tile("Tổng bản ghi", f"{int(ingest.get('total_records', 0) or 0):,}"),
    ], cols=3))
    hdfs_logs = (audit.get("hdfs_summary") or {}).get("logs") or []
    if hdfs_logs:
        st.dataframe(pd.DataFrame(hdfs_logs), width="stretch", hide_index=True)
    else:
        st.info("Chưa có nhật ký đồng bộ Hadoop WebHDFS (`data/logs/hdfs_sync_log.jsonl`).")

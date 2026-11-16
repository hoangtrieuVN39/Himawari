"""Merges Himawari B13 metrics envelopes produced by a backfill in another base dir into this Bronze.

Heavy backfills run with `--base-dir` on a roomy disk (raw segments + PNGs); only the small metrics
envelopes are merged here, so the pipeline and the ML features see them. Slots that already have a
metrics envelope in the destination are skipped, which keeps re-imports idempotent and never creates
duplicate scans (metrics batch ids are random per processing run).

CLI:
    python -m src.ingestion.import_metrics --from D:/DLL_backfill/data --to data
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict

from src.ingestion.raw_store import RawEnvelopeStore

SOURCE = "himawari_metrics"


def _slots_present(store: RawEnvelopeStore) -> set:
    return {
        (env.get("metadata") or {}).get("scan_slot_utc")
        for env in store.iter_envelopes(SOURCE, strict=False)
    } - {None}


def import_himawari_metrics(src_root: str, dst_root: str) -> Dict[str, Any]:
    """Copies metrics envelopes whose scan slot is new to `dst_root`; returns counts."""
    src = RawEnvelopeStore(storage_root=src_root)
    dst = RawEnvelopeStore(storage_root=dst_root)
    present = _slots_present(dst)
    imported = skipped = 0
    for env in src.iter_envelopes(SOURCE, strict=False):
        meta = env.get("metadata") or {}
        slot = meta.get("scan_slot_utc")
        if not slot or meta.get("is_synthetic_test") or slot in present:
            skipped += 1
            continue
        dst.write(SOURCE, slot[:10], env)
        present.add(slot)
        imported += 1
    return {"imported": imported, "skipped": skipped, "slots_in_destination": len(present)}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Merge backfilled Himawari metrics envelopes into Bronze")
    parser.add_argument("--from", dest="src", required=True, help="Base dir of the backfill run")
    parser.add_argument("--to", dest="dst", default="data", help="Destination Bronze base dir")
    args = parser.parse_args(argv)
    print(json.dumps(import_himawari_metrics(args.src, args.dst), indent=2))


if __name__ == "__main__":
    main()

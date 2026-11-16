"""Merging backfilled metrics: new scan slots only, idempotent on re-import."""

from pathlib import Path

from src.ingestion.import_metrics import import_himawari_metrics
from src.ingestion.raw_store import RawEnvelopeStore


def _metrics(store: RawEnvelopeStore, batch: str, slot: str, synthetic: bool = False) -> None:
    store.write("himawari_metrics", slot[:10], {
        "metadata": {"batch_id": batch, "source": "himawari_metrics", "scan_slot_utc": slot,
                     "is_backfill": True, "is_synthetic_test": synthetic},
        "airports": [{"airport_icao": "VVDN", "status": "AVAILABLE"}],
    })


def test_import_adds_only_new_slots_and_is_idempotent(tmp_path: Path):
    src = RawEnvelopeStore(storage_root=str(tmp_path / "backfill"))
    dst = RawEnvelopeStore(storage_root=str(tmp_path / "data"))
    _metrics(dst, "live_0800", "2026-11-12T08:00:00Z")
    _metrics(src, "bf_0800", "2026-11-12T08:00:00Z")  # already present in destination (other batch id)
    _metrics(src, "bf_0810", "2026-11-12T08:10:00Z")
    _metrics(src, "bf_synth", "2026-11-12T08:20:00Z", synthetic=True)

    first = import_himawari_metrics(str(tmp_path / "backfill"), str(tmp_path / "data"))
    second = import_himawari_metrics(str(tmp_path / "backfill"), str(tmp_path / "data"))

    assert first == {"imported": 1, "skipped": 2, "slots_in_destination": 2}
    assert second["imported"] == 0
    slots = sorted(e["metadata"]["scan_slot_utc"] for e in dst.iter_envelopes("himawari_metrics"))
    assert slots == ["2026-11-12T08:00:00Z", "2026-11-12T08:10:00Z"]

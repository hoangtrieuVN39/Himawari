"""Writes Gold/Silver lake versions with pyarrow (no Spark) for dashboard tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.spark.lake_paths import SUCCESS_MARKER, VERSION_PREFIX, default_lake_root, table_uri

_TIMESTAMP_COLUMNS = {
    "review_slot_utc", "observed_at_utc", "metar_fetched_at_utc", "forecast_fetched_at_utc",
    "target_forecast_hour_utc", "latest_b13_scan_time_utc", "b13_fetched_at_utc",
    "inputs_complete_at_utc", "published_at_utc", "first_published_at_utc",
}


def _ts(value: Any) -> Any:
    if isinstance(value, str) and value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    return value


def write_lake_version(
    storage_root: Path | str,
    table: str,
    rows: list[dict[str, Any]],
    run_id: str = "20261110T080000000000Z",
    committed: bool = True,
) -> Path:
    """Writes `rows` as one version of `table` under <storage_root>/lake; ISO 'Z' strings in known
    timestamp columns become UTC timestamps, as Spark writes them."""
    version = Path(table_uri(default_lake_root(str(storage_root)), table)) / f"{VERSION_PREFIX}{run_id}"
    version.mkdir(parents=True, exist_ok=True)
    converted = [{k: (_ts(v) if k in _TIMESTAMP_COLUMNS else v) for k, v in r.items()} for r in rows]
    pq.write_table(pa.Table.from_pylist(converted), version / "part-00000.parquet")
    if committed:
        (version / SUCCESS_MARKER).write_bytes(b"")
    return version


def decision_row(slot: str, icao: str, level: int, **overrides: Any) -> dict[str, Any]:
    """Minimal Gold decision row; co-priority columns are filled by `finalize_slot`."""
    row = {
        "review_slot_utc": slot,
        "airport_icao": icao,
        "observed_at_utc": slot,
        "raw_ob": f"METAR {icao} {slot[8:10]}{slot[11:13]}{slot[14:16]}Z 00000KT 9999 FEW020 30/24 Q1010",
        "visibility_m": 9999,
        "metar_is_present": True,
        "metar_age_minutes": 0.0,
        "forecast_is_present": True,
        "forecast_rain_next_hour_mm": 0.0,
        "b13_is_present": False,
        "station_weather_codes": [],
        "priority_level": level,
        "priority_label": f"Mức {level}",
        "reason_vi": f"Lý do mức {level}",
        "slot_kind": "live",
    }
    row.update(overrides)
    return row


def finalize_slot(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adds the co-priority columns the Spark priority engine produces."""
    top = min(r["priority_level"] for r in rows)
    accepted = sorted(r["airport_icao"] for r in rows if r["priority_level"] == top)
    for r in rows:
        r.update(
            top_priority_level=top,
            is_top_priority=r["priority_level"] == top,
            accepted_first_review_airports=accepted,
            co_priority_top_count=len(accepted),
            is_co_priority_top=r["priority_level"] == top and len(accepted) > 1,
        )
    return rows

"""End-to-end temporal contracts of the lake pipeline (late data, corrected bulletins, missing sources).

One synthetic Bronze root, two pipeline runs (Spark is slow on Windows): every assertion below is a
consumer-visible guarantee of the Gold decision table.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.lake_paths import local_table_version
from src.spark.run_pipeline import run_spark_pipeline
from src.spark.writers import latest_version_uri, read_table, write_table_version

SLOTS = ["2026-11-10T06:00:00Z", "2026-11-10T08:05:00Z", "2026-11-10T08:35:00Z"]
_IGNORED = {"run_id", "published_at_utc", "first_published_at_utc", "inputs_complete_at_utc"}


def _epoch(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def _metar_item(icao: str, obs: str, raw: str) -> dict:
    return {"icaoId": icao, "obsTime": _epoch(obs), "reportTime": obs.replace("Z", ".000Z"),
            "receiptTime": obs, "rawOb": raw, "visib": "6+"}


def _metar(store: RawEnvelopeStore, batch: str, fetched: str, items: list[dict], backfill: bool = False):
    store.write("metar", fetched[:10], {
        "metadata": {"batch_id": batch, "source": "aviationweather_metar", "fetched_at_utc": fetched,
                     "is_backfill": backfill},
        "payload": items,
    })


def _forecast(store: RawEnvelopeStore, batch: str, icao: str, fetched: str, rain_0900: float, backfill=False):
    hours = ["2026-11-10T08:00", "2026-11-10T09:00", "2026-11-10T10:00"]
    store.write("open_meteo", fetched[:10], {
        "metadata": {"batch_id": batch, "source": "open_meteo_forecast", "airport_icao": icao,
                     "fetched_at_utc": fetched, "is_backfill": backfill},
        "payload": {"hourly": {"time": hours, "precipitation": [0.0, rain_0900, 0.0], "rain": [0.0, rain_0900, 0.0],
                               "showers": [0.0, 0.0, 0.0], "weather_code": [1, 61, 1],
                               "visibility": [20000.0, 8000.0, 20000.0], "wind_speed_10m": [5.0, 6.0, 5.0],
                               "wind_gusts_10m": [9.0, 10.0, 9.0]}},
    })


@pytest.fixture
def bronze_root(tmp_path: Path) -> str:
    root = tmp_path / "data"
    store = RawEnvelopeStore(storage_root=str(root))
    _metar(store, "m0801", "2026-11-10T08:01:00Z", [
        _metar_item("VVDN", "2026-11-10T08:00:00Z", "METAR VVDN 100800Z 03005KT 4000 -RA BKN015 26/24 Q1012"),
        _metar_item("VVNB", "2026-11-10T07:30:00Z", "METAR VVNB 100730Z 09004KT 9999 FEW020 24/18 Q1016"),
        _metar_item("VVTS", "2026-11-10T08:00:00Z", "METAR VVTS 100800Z 18006KT 9999 SCT020 31/24 Q1009"),
    ])
    # Corrected bulletin for the same VVNB observation, received after the 08:05 slot.
    _metar(store, "m0806", "2026-11-10T08:06:00Z", [
        _metar_item("VVNB", "2026-11-10T07:30:00Z", "METAR COR VVNB 100730Z 09004KT 3000 TSRA FEW015CB 24/18 Q1016"),
    ])
    # Late live report: must not exist for the 08:05 slot.
    _metar(store, "m0831", "2026-11-10T08:31:00Z", [
        _metar_item("VVTS", "2026-11-10T08:30:00Z", "METAR VVTS 100830Z 20012G25KT 2000 +TSRA BKN010CB 26/24 Q1010"),
    ])
    _forecast(store, "f0800", "VVTS", "2026-11-10T08:00:00Z", rain_0900=2.0)
    # A forecast fetched after the slot, even flagged as backfill, must never replace the one available then.
    _forecast(store, "f0820", "VVTS", "2026-11-10T08:20:00Z", rain_0900=0.0, backfill=True)
    return str(root)


def _gold_rows(spark: SparkSession, root: str) -> dict[tuple[str, str], dict]:
    """Gold rows keyed by (slot HH:MM, airport); timestamps formatted in Spark (UTC session), because
    collect() would convert them to the OS timezone."""
    df = read_table(spark, f"{root}/lake", "decision_table")
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(field.name, F.date_format(field.name, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    out = {}
    for r in df.collect():
        d = r.asDict(recursive=True)
        out[(d["review_slot_utc"][11:16], d["airport_icao"])] = d
    return out


def test_gold_contracts_late_data_corrections_missing_sources_and_rerun(spark: SparkSession, bronze_root: str):
    first = run_spark_pipeline(storage_root=bronze_root, review_slots=SLOTS, mode="replay", spark=spark)
    assert first["decision_rows_count"] == 9
    for src in first["reconciliation"]["sources"]:
        assert src["rows_read"] == src["rows_valid_pre_dedup"] + src["rows_quarantined"], src
    rows = _gold_rows(spark, bronze_root)
    assert len(rows) == 9  # exactly 3 airports x 3 slots, even when every source is missing

    # 06:00: before any data -> METAR missing (level 2), forecast and B13 missing, rows still present
    for icao in ("VVDN", "VVNB", "VVTS"):
        r = rows[("06:00", icao)]
        assert (r["metar_is_present"], r["forecast_is_present"], r["b13_is_present"]) == (False, False, False)
        assert r["priority_level"] == 2

    # 08:05: the corrected VVNB bulletin and the late VVTS report are not visible yet
    assert rows[("08:05", "VVDN")]["priority_level"] == 1
    vvnb = rows[("08:05", "VVNB")]
    assert "TSRA" not in vvnb["raw_ob"] and vvnb["priority_level"] == 4
    vvts = rows[("08:05", "VVTS")]
    assert vvts["priority_level"] == 3 and vvts["forecast_rain_next_hour_mm"] == pytest.approx(2.0)
    assert vvts["forecast_fetched_at_utc"][11:16] == "08:00"
    assert rows[("08:05", "VVDN")]["forecast_is_present"] is False  # missing forecast stays missing

    # 08:35: corrected bulletin (revision 2) and the late report are both used. The VVNB observation
    # (07:30) is now 65 min old, so it is level 2 "verify data" despite the TSRA it reports.
    vvnb = rows[("08:35", "VVNB")]
    assert "TSRA" in vvnb["raw_ob"] and vvnb["revision_index"] == 2
    assert vvnb["priority_level"] == 2
    assert "+TSRA" in rows[("08:35", "VVTS")]["raw_ob"]
    assert rows[("08:35", "VVDN")]["priority_level"] == rows[("08:35", "VVTS")]["priority_level"] == 1
    assert rows[("08:35", "VVDN")]["accepted_first_review_airports"] == ["VVDN", "VVTS"]

    # Re-run on the same input: same keys and values; first publication time is kept
    second = run_spark_pipeline(storage_root=bronze_root, review_slots=SLOTS, mode="replay", spark=spark)
    assert second["run_id"] != first["run_id"]
    rerun = _gold_rows(spark, bronze_root)
    assert rerun.keys() == rows.keys()
    for key in rows:
        assert {k: v for k, v in rerun[key].items() if k not in _IGNORED} == \
               {k: v for k, v in rows[key].items() if k not in _IGNORED}
        assert rerun[key]["first_published_at_utc"] == rows[key]["first_published_at_utc"]
    assert local_table_version(f"{bronze_root}/lake", "decision_table").name == f"run_id={second['run_id']}"


def test_lake_versions_commit_atomically_and_keep_two(spark: SparkSession, tmp_path: Path):
    lake = str(tmp_path / "lake")
    df = spark.createDataFrame([(1,)], "x int")
    for run_id in ("20261110T080000000000Z", "20261110T081000000000Z", "20261110T082000000000Z"):
        write_table_version(df, lake, "quarantine", run_id)
    abandoned = tmp_path / "lake" / "silver" / "quarantine" / "run_id=20261110T083000000000Z"
    abandoned.mkdir()  # crashed writer: no _SUCCESS marker
    assert latest_version_uri(spark, lake, "quarantine").endswith("run_id=20261110T082000000000Z")
    write_table_version(df, lake, "quarantine", "20261110T084000000000Z")
    names = sorted(p.name for p in (tmp_path / "lake" / "silver" / "quarantine").iterdir())
    assert names == ["run_id=20261110T082000000000Z", "run_id=20261110T084000000000Z"]

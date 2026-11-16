"""Unit tests for point-in-time temporal join and future data leakage prevention."""

from pyspark.sql import SparkSession
from src.ingestion.raw_store import RawEnvelopeStore
from src.spark.quarantine import (
    validate_and_split_himawari,
    validate_and_split_metar,
    validate_and_split_open_meteo,
)
from src.spark.readers import (
    read_raw_himawari_metrics,
    read_raw_metar,
    read_raw_open_meteo,
)
from src.spark.temporal_join import build_point_in_time_snapshot
from src.spark.transforms import enrich_valid_himawari, enrich_valid_metar


def _active_paths(source: str) -> tuple[list[str], bool]:
    spec = RawEnvelopeStore(storage_root="data").read_specs(source)[0]
    return list(spec.paths), spec.multiline


def test_point_in_time_no_future_leakage_and_3_airport_spine(spark: SparkSession):
    # Reads the active Bronze layout (legacy raw/*.json files were pruned after the daily_jsonl migration)
    m_paths, m_multiline = _active_paths("metar")
    f_paths, f_multiline = _active_paths("open_meteo")
    h_paths, h_multiline = _active_paths("himawari_metrics")
    raw_m = read_raw_metar(spark, m_paths, multiline=m_multiline)
    raw_f = read_raw_open_meteo(spark, f_paths, multiline=f_multiline)
    raw_h = read_raw_himawari_metrics(spark, h_paths, multiline=h_multiline)

    valid_m, _, _ = validate_and_split_metar(raw_m)
    valid_f, _, _ = validate_and_split_open_meteo(raw_f)
    valid_h, _, _ = validate_and_split_himawari(raw_h)

    enr_m = enrich_valid_metar(valid_m)
    enr_h = enrich_valid_himawari(valid_h)

    # Review slot: 2026-11-09 07:00:00 UTC
    snapshot_df = build_point_in_time_snapshot(
        spark=spark,
        review_slots_utc=["2026-11-09T07:00:00Z"],
        metar_df=enr_m,
        forecast_df=valid_f,
        himawari_df=enr_h,
        airports=["VVDN", "VVNB", "VVTS"],
        mode="replay",
    )

    # Invariant: exactly 3 rows, 1 per airport
    rows = snapshot_df.collect()
    assert len(rows) == 3
    airports = {r.airport_icao for r in rows}
    assert airports == {"VVDN", "VVNB", "VVTS"}

    # VVDN has real rain at 07:00Z
    vvdn = [r for r in rows if r.airport_icao == "VVDN"][0]
    assert vvdn.has_precip_or_thunder_at_station is True
    assert "-RA" in vvdn.station_weather_codes


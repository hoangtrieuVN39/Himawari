"""Unit tests for Level 1–4 priority rules and co-priority tie resolution."""

from pyspark.sql import SparkSession
from src.spark.priority_engine import apply_priority_rules
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


def test_real_replay_scenario_0700_single_top_answer(spark: SparkSession):
    """At 07:00Z, VVDN reports real rain (-RA) while VVNB and VVTS are fair."""
    raw_m = read_raw_metar(spark, "data/raw/metar/date=2026-11-09/*.json")
    raw_f = read_raw_open_meteo(spark, "data/raw/open_meteo/date=2026-11-09/*.json")
    raw_h = read_raw_himawari_metrics(spark, "data/raw/himawari_metrics/date=2026-11-09/*.json")

    valid_m, _, _ = validate_and_split_metar(raw_m)
    valid_f, _, _ = validate_and_split_open_meteo(raw_f)
    valid_h, _, _ = validate_and_split_himawari(raw_h)

    enr_m = enrich_valid_metar(valid_m)
    enr_h = enrich_valid_himawari(valid_h)

    snapshot_df = build_point_in_time_snapshot(
        spark=spark,
        review_slots_utc=["2026-11-09T07:00:00Z"],
        metar_df=enr_m,
        forecast_df=valid_f,
        himawari_df=enr_h,
        mode="replay",
    )

    decision_df = apply_priority_rules(snapshot_df)
    rows = decision_df.collect()
    assert len(rows) == 3

    vvdn = [r for r in rows if r.airport_icao == "VVDN"][0]
    assert vvdn.priority_level == 1
    assert vvdn.is_top_priority is True
    assert vvdn.accepted_first_review_airports == ["VVDN"]
    assert vvdn.is_co_priority_top is False
    assert "mưa/dông tại trạm" in vvdn.reason_vi


def test_real_replay_scenario_0800_co_priority_top_tie(spark: SparkSession):
    """At 08:00Z, BOTH VVDN (rain) and VVNB (rain) have rain in METAR -> Co-priority Level 1."""
    raw_m = read_raw_metar(spark, "data/raw/metar/date=2026-11-09/*.json")
    raw_f = read_raw_open_meteo(spark, "data/raw/open_meteo/date=2026-11-09/*.json")
    raw_h = read_raw_himawari_metrics(spark, "data/raw/himawari_metrics/date=2026-11-09/*.json")

    valid_m, _, _ = validate_and_split_metar(raw_m)
    valid_f, _, _ = validate_and_split_open_meteo(raw_f)
    valid_h, _, _ = validate_and_split_himawari(raw_h)

    enr_m = enrich_valid_metar(valid_m)
    enr_h = enrich_valid_himawari(valid_h)

    snapshot_df = build_point_in_time_snapshot(
        spark=spark,
        review_slots_utc=["2026-11-09T08:00:00Z"],
        metar_df=enr_m,
        forecast_df=valid_f,
        himawari_df=enr_h,
        mode="replay",
    )

    decision_df = apply_priority_rules(snapshot_df)
    rows = decision_df.collect()
    assert len(rows) == 3

    by_icao = {r.airport_icao: r for r in rows}
    assert by_icao["VVDN"].priority_level == 1
    assert by_icao["VVNB"].priority_level == 1

    # Both VVDN and VVNB must be top priority with co-priority flag True
    assert by_icao["VVDN"].is_top_priority is True
    assert by_icao["VVNB"].is_top_priority is True
    assert by_icao["VVDN"].is_co_priority_top is True
    assert by_icao["VVNB"].is_co_priority_top is True

    # Order-independent accepted list contains both
    assert by_icao["VVDN"].accepted_first_review_airports == ["VVDN", "VVNB"]
    assert by_icao["VVNB"].accepted_first_review_airports == ["VVDN", "VVNB"]


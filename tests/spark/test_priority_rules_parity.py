"""The pure-Python priority rules (dashboard) must stay identical to the Spark engine."""

from datetime import datetime, timezone
import itertools

import pytest
from pyspark.sql import types as T

from src.spark.priority_engine import apply_priority_rules
from src.spark.priority_rules import PriorityInput, evaluate_priority

OBS = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
TARGET = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _case(**overrides) -> PriorityInput:
    base = dict(
        metar_is_present=True,
        metar_age_minutes=15.0,
        has_precip_or_thunder_at_station=False,
        is_low_visibility=False,
        station_weather_codes=[],
        visibility_m=10000,
        observed_hhmm="08:00Z",
        forecast_is_present=False,
        forecast_rain_next_hour_mm=None,
        target_forecast_hhmm="09:00Z",
    )
    base.update(overrides)
    return PriorityInput(**base)


def _grid() -> list[PriorityInput]:
    cases = []
    for present, age, precip, lowvis, fc_present, fc_rain in itertools.product(
        (True, False),
        (15.0, 60.0, 60.5, 120.0),
        (False, True),
        (False, True),
        (False, True),
        (None, 0.0, 0.1, 5.0),
    ):
        if not present:
            age = None
            precip = False
            lowvis = False
        if not fc_present:
            fc_rain = None
        elif fc_rain is None:
            continue
        cases.append(
            _case(
                metar_is_present=present,
                metar_age_minutes=age,
                has_precip_or_thunder_at_station=precip,
                is_low_visibility=lowvis,
                station_weather_codes=["-RA"] if precip else [],
                visibility_m=800 if lowvis else 10000,
                forecast_is_present=fc_present,
                forecast_rain_next_hour_mm=fc_rain,
            )
        )
    # de-duplicate identical inputs
    unique = {repr(c): c for c in cases}
    return list(unique.values())


def test_level_1_precip_and_low_visibility_reason():
    d = evaluate_priority(
        _case(has_precip_or_thunder_at_station=True, is_low_visibility=True,
              station_weather_codes=["TSRA"], visibility_m=800)
    )
    assert (d.level, d.label) == (1, "LEVEL_1_HAZARD_AT_STATION")
    assert d.reason_vi == "METAR mới (08:00Z) ghi nhận mưa/dông tại trạm (TSRA) và tầm nhìn thấp 800m (< 5km)"


def test_level_boundaries():
    assert evaluate_priority(_case(metar_age_minutes=60.0)).level == 4
    assert evaluate_priority(_case(metar_age_minutes=60.1)).level == 2
    assert evaluate_priority(_case(metar_is_present=False, metar_age_minutes=None)).level == 2
    assert evaluate_priority(_case(forecast_is_present=True, forecast_rain_next_hour_mm=0.1)).level == 3
    assert evaluate_priority(_case(forecast_is_present=True, forecast_rain_next_hour_mm=0.09)).level == 4
    # stale METAR with hazard is data verification, not hazard
    assert evaluate_priority(
        _case(metar_age_minutes=90.0, has_precip_or_thunder_at_station=True)
    ).level == 2


def test_spark_and_python_rules_agree_on_full_grid(spark):
    cases = _grid()
    schema = T.StructType([
        T.StructField("review_slot_utc", T.StringType()),
        T.StructField("airport_icao", T.StringType()),
        T.StructField("metar_is_present", T.BooleanType()),
        T.StructField("metar_age_minutes", T.DoubleType()),
        T.StructField("has_precip_or_thunder_at_station", T.BooleanType()),
        T.StructField("is_low_visibility", T.BooleanType()),
        T.StructField("station_weather_codes", T.ArrayType(T.StringType())),
        T.StructField("visibility_m", T.IntegerType()),
        T.StructField("observed_at_utc", T.TimestampType()),
        T.StructField("forecast_is_present", T.BooleanType()),
        T.StructField("forecast_rain_next_hour_mm", T.DoubleType()),
        T.StructField("target_forecast_hour_utc", T.TimestampType()),
    ])
    rows = [
        (
            f"slot-{i:04d}", "VVDN", c.metar_is_present, c.metar_age_minutes,
            c.has_precip_or_thunder_at_station, c.is_low_visibility,
            list(c.station_weather_codes), c.visibility_m, OBS,
            c.forecast_is_present, c.forecast_rain_next_hour_mm, TARGET,
        )
        for i, c in enumerate(cases)
    ]
    result = {r.review_slot_utc: r for r in apply_priority_rules(spark.createDataFrame(rows, schema)).collect()}

    assert len(result) == len(cases)
    for i, c in enumerate(cases):
        spark_row = result[f"slot-{i:04d}"]
        py = evaluate_priority(c)
        assert (spark_row.priority_level, spark_row.priority_label) == (py.level, py.label), c
        assert spark_row.reason_vi == py.reason_vi, c

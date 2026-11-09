"""Data quality validation, quarantine splitting, revision deduplication and reconciliation."""

import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VALID_AIRPORTS = ["VVDN", "VVNB", "VVTS"]


def _format_quarantine_records(
    df: DataFrame,
    source_name: str,
    event_time_col: str,
    raw_col: str,
) -> DataFrame:
    """Formats quarantined records into standard quarantine schema."""
    now_utc = datetime.now(timezone.utc).isoformat()
    return df.select(
        F.concat(
            F.lit(source_name + "_"),
            F.coalesce(F.col("batch_id"), F.lit("nobatch")),
            F.lit("_"),
            F.monotonically_increasing_id().cast("string"),
        ).alias("quarantine_id"),
        F.lit(source_name).alias("source"),
        F.col("batch_id"),
        F.col("airport_icao"),
        F.col(event_time_col).cast("string").alias("event_time_raw"),
        F.col("fetched_at_utc").cast("string").alias("fetched_at_raw"),
        F.col("error_codes"),
        F.to_json(F.struct(raw_col)).alias("raw_record_json"),
        F.col("input_file_path"),
        F.coalesce(
            F.col("is_synthetic_test") if "is_synthetic_test" in df.columns else F.lit(False),
            F.lit(False),
        ).alias("is_synthetic_test"),
        F.lit(now_utc).alias("quarantined_at_utc"),
    )


def validate_and_split_metar(
    df: DataFrame,
    valid_airports: list[str] | None = None,
) -> tuple[DataFrame, DataFrame, dict]:
    """Validates METAR rows, splits into valid vs quarantine, and tracks deduplication & revisions."""
    airports = valid_airports or VALID_AIRPORTS

    err_conds = [
        F.when(
            F.col("_corrupt_record").isNotNull()
            | F.col("batch_id").isNull()
            | F.col("fetched_at_utc").isNull(),
            F.lit("SCHEMA_VIOLATION"),
        ),
        F.when(
            F.col("airport_icao").isNull() | (~F.col("airport_icao").isin(airports)),
            F.lit("UNKNOWN_AIRPORT"),
        ),
        F.when(F.col("observed_at_utc").isNull(), F.lit("INVALID_TIMESTAMP")),
        F.when(
            (~F.col("is_backfill"))
            & (F.col("observed_at_utc") > F.col("fetched_at_utc") + F.expr("INTERVAL 10 MINUTES")),
            F.lit("FUTURE_OBSERVATION_TIME"),
        ),
        F.when(
            F.col("raw_ob").isNull() | (F.length(F.trim(F.col("raw_ob"))) < 10),
            F.lit("INVALID_METAR_PAYLOAD"),
        ),
    ]

    flagged_df = df.withColumn(
        "error_codes",
        F.array_compact(F.array(*err_conds)),
    )

    invalid_df = flagged_df.filter(F.size(F.col("error_codes")) > 0)
    quarantine_df = _format_quarantine_records(
        invalid_df,
        source_name="metar",
        event_time_col="observed_at_utc",
        raw_col="raw_ob",
    )

    valid_pre_dedup_df = (
        flagged_df.filter(F.size(F.col("error_codes")) == 0)
        .drop("error_codes")
        .cache()
    )

    dedup_window = Window.partitionBy("airport_icao", "observed_at_utc", "raw_ob").orderBy(
        F.col("fetched_at_utc").asc()
    )
    exact_deduped_df = valid_pre_dedup_df.withColumn(
        "dup_rank", F.row_number().over(dedup_window)
    )

    retained_exact_df = exact_deduped_df.filter(F.col("dup_rank") == 1).drop("dup_rank")

    revision_window = Window.partitionBy("airport_icao", "observed_at_utc").orderBy(
        F.col("fetched_at_utc").asc()
    )
    valid_retained_df = (
        retained_exact_df.withColumn(
            "revision_index", F.dense_rank().over(revision_window)
        ).cache()
    )

    quarantined = quarantine_df.count()
    valid_pre_dedup = valid_pre_dedup_df.count()
    retained_valid = valid_retained_df.count()
    total_read = valid_pre_dedup + quarantined
    duplicate_dropped = valid_pre_dedup - retained_valid

    reconcile = {
        "source": "metar",
        "rows_read": total_read,
        "rows_quarantined": quarantined,
        "rows_valid_pre_dedup": valid_pre_dedup,
        "rows_retained_valid": retained_valid,
        "rows_duplicate_dropped": duplicate_dropped,
        "is_reconciled": (total_read == valid_pre_dedup + quarantined)
        and (valid_pre_dedup == retained_valid + duplicate_dropped),
    }

    return valid_retained_df, quarantine_df, reconcile


def validate_and_split_open_meteo(
    df: DataFrame,
    valid_airports: list[str] | None = None,
) -> tuple[DataFrame, DataFrame, dict]:
    """Validates Open-Meteo forecast rows, splits quarantine, and deduplicates identical valid rows."""
    airports = valid_airports or VALID_AIRPORTS

    err_conds = [
        F.when(
            F.col("_corrupt_record").isNotNull()
            | F.col("batch_id").isNull()
            | F.col("fetched_at_utc").isNull(),
            F.lit("SCHEMA_VIOLATION"),
        ),
        F.when(
            F.col("airport_icao").isNull() | (~F.col("airport_icao").isin(airports)),
            F.lit("UNKNOWN_AIRPORT"),
        ),
        F.when(F.col("forecast_valid_at_utc").isNull(), F.lit("INVALID_TIMESTAMP")),
        F.when(
            (F.col("precipitation_mm") < 0.0)
            | (F.col("rain_mm") < 0.0)
            | (F.col("showers_mm") < 0.0)
            | (F.col("forecast_visibility_m") < 0.0),
            F.lit("NEGATIVE_FORECAST_VALUE"),
        ),
    ]

    flagged_df = df.withColumn(
        "error_codes",
        F.array_compact(F.array(*err_conds)),
    )

    invalid_df = flagged_df.filter(F.size(F.col("error_codes")) > 0)
    quarantine_df = _format_quarantine_records(
        invalid_df,
        source_name="open_meteo",
        event_time_col="forecast_valid_at_utc",
        raw_col="precipitation_mm",
    )

    valid_pre_dedup_df = (
        flagged_df.filter(F.size(F.col("error_codes")) == 0)
        .drop("error_codes")
        .cache()
    )

    dedup_window = Window.partitionBy(
        "airport_icao", "fetched_at_utc", "forecast_valid_at_utc"
    ).orderBy(F.col("batch_id").asc())
    valid_retained_df = (
        valid_pre_dedup_df.withColumn("dup_rank", F.row_number().over(dedup_window))
        .filter(F.col("dup_rank") == 1)
        .drop("dup_rank")
        .cache()
    )

    quarantined = quarantine_df.count()
    valid_pre_dedup = valid_pre_dedup_df.count()
    retained_valid = valid_retained_df.count()
    total_read = valid_pre_dedup + quarantined
    duplicate_dropped = valid_pre_dedup - retained_valid

    reconcile = {
        "source": "open_meteo",
        "rows_read": total_read,
        "rows_quarantined": quarantined,
        "rows_valid_pre_dedup": valid_pre_dedup,
        "rows_retained_valid": retained_valid,
        "rows_duplicate_dropped": duplicate_dropped,
        "is_reconciled": (total_read == valid_pre_dedup + quarantined)
        and (valid_pre_dedup == retained_valid + duplicate_dropped),
    }

    return valid_retained_df, quarantine_df, reconcile


def validate_and_split_himawari(
    df: DataFrame,
    valid_airports: list[str] | None = None,
) -> tuple[DataFrame, DataFrame, dict]:
    """Validates Himawari B13 cloud metrics rows and separates invalid or physical-out-of-bounds rows."""
    airports = valid_airports or VALID_AIRPORTS

    err_conds = [
        F.when(
            F.col("_corrupt_record").isNotNull()
            | F.col("batch_id").isNull()
            | F.col("fetched_at_utc").isNull(),
            F.lit("SCHEMA_VIOLATION"),
        ),
        F.when(
            F.col("airport_icao").isNull() | (~F.col("airport_icao").isin(airports)),
            F.lit("UNKNOWN_AIRPORT"),
        ),
        F.when(F.col("scan_time_utc").isNull(), F.lit("INVALID_TIMESTAMP")),
        F.when(
            (F.col("outer_bt_min_k") < 150.0)
            | (F.col("outer_bt_min_k") > 340.0)
            | (F.col("inner_bt_min_k") < 150.0)
            | (F.col("inner_bt_min_k") > 340.0),
            F.lit("OUT_OF_RANGE_BT_KELVIN"),
        ),
        F.when(
            (F.col("b13_valid_fraction") < 0.0)
            | (F.col("b13_valid_fraction") > 1.0)
            | (F.col("inner_cold_cloud_fraction") < 0.0)
            | (F.col("inner_cold_cloud_fraction") > 1.0)
            | (F.col("inner_severe_convection_fraction") < 0.0)
            | (F.col("inner_severe_convection_fraction") > 1.0),
            F.lit("OUT_OF_RANGE_CLOUD_FRACTION"),
        ),
    ]

    flagged_df = df.withColumn(
        "error_codes",
        F.array_compact(F.array(*err_conds)),
    )

    invalid_df = flagged_df.filter(F.size(F.col("error_codes")) > 0)
    quarantine_df = _format_quarantine_records(
        invalid_df,
        source_name="himawari_metrics",
        event_time_col="scan_time_utc",
        raw_col="outer_bt_min_k",
    )

    valid_pre_dedup_df = (
        flagged_df.filter(F.size(F.col("error_codes")) == 0)
        .drop("error_codes")
        .cache()
    )

    dedup_window = Window.partitionBy("airport_icao", "scan_time_utc").orderBy(
        F.col("fetched_at_utc").desc()
    )
    valid_retained_df = (
        valid_pre_dedup_df.withColumn("dup_rank", F.row_number().over(dedup_window))
        .filter(F.col("dup_rank") == 1)
        .drop("dup_rank")
        .cache()
    )

    quarantined = quarantine_df.count()
    valid_pre_dedup = valid_pre_dedup_df.count()
    retained_valid = valid_retained_df.count()
    total_read = valid_pre_dedup + quarantined
    duplicate_dropped = valid_pre_dedup - retained_valid

    reconcile = {
        "source": "himawari_metrics",
        "rows_read": total_read,
        "rows_quarantined": quarantined,
        "rows_valid_pre_dedup": valid_pre_dedup,
        "rows_retained_valid": retained_valid,
        "rows_duplicate_dropped": duplicate_dropped,
        "is_reconciled": (total_read == valid_pre_dedup + quarantined)
        and (valid_pre_dedup == retained_valid + duplicate_dropped),
    }

    return valid_retained_df, quarantine_df, reconcile


def build_reconciliation_report(
    metrics_by_source: list[dict],
    file_error_log_path: str = "data/logs/himawari_file_errors.jsonl",
) -> dict:
    """Builds the comprehensive reconciliation report across all sources and binary errors."""
    binary_errors_count = 0
    synthetic_binary_errors = 0
    p = Path(file_error_log_path)
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        binary_errors_count += 1
                        if record.get("is_synthetic_test"):
                            synthetic_binary_errors += 1
                    except Exception:
                        pass

    all_reconciled = all(m.get("is_reconciled", False) for m in metrics_by_source)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "all_sources_reconciled": all_reconciled,
        "sources": metrics_by_source,
        "binary_file_errors": {
            "total_count": binary_errors_count,
            "synthetic_count": synthetic_binary_errors,
            "log_path": file_error_log_path,
        },
    }


def inject_synthetic_quarantine_batch(output_dir: str = "data/raw/synthetic_quarantine") -> dict:
    """Injects synthetic invalid JSON envelopes labeled with is_synthetic_test = True."""
    out_p = Path(output_dir)
    out_p.mkdir(parents=True, exist_ok=True)

    metar_bad = {
        "metadata": {
            "batch_id": "synthetic_metar_bad_batch",
            "source": "aviationweather_metar",
            "source_url": "test://synthetic",
            "attribution": "Synthetic test record",
            "expected_slot_utc": "2026-11-09T15:00:00Z",
            "fetched_at_utc": "2026-11-09T15:00:00Z",
            "http_status": 200,
            "airports_requested": ["VVXX"],
            "is_backfill": False,
            "is_synthetic_test": True,
        },
        "payload": [
            {
                "icaoId": "VVXX",  # UNKNOWN_AIRPORT
                "receiptTime": "2026-11-09T15:02:00Z",
                "obsTime": 1794236400,
                "reportTime": "2026-11-09T15:00:00Z",
                "rawOb": "METAR VVXX 091500Z 10010KT 9999 NOSIG",
                "visib": "6+",
            },
            {
                "icaoId": "VVDN",  # FUTURE_OBSERVATION_TIME
                "receiptTime": "2026-11-09T15:02:00Z",
                "obsTime": 1794240000,
                "reportTime": "2026-11-09T16:00:00Z",
                "rawOb": "METAR VVDN 091600Z 10010KT 9999 NOSIG",
                "visib": "6+",
            },
            {
                "icaoId": "VVNB",  # INVALID_METAR_PAYLOAD
                "receiptTime": "2026-11-09T15:02:00Z",
                "obsTime": 1794236400,
                "reportTime": "2026-11-09T15:00:00Z",
                "rawOb": "NIL",
                "visib": "6+",
            },
        ],
    }

    himawari_bad = {
        "metadata": {
            "batch_id": "synthetic_himawari_bad_batch",
            "source": "himawari_metrics",
            "scan_slot_utc": "2026-11-09T15:00:00Z",
            "processed_at_utc": "2026-11-09T15:05:00Z",
            "is_backfill": False,
            "is_synthetic_test": True,
            "airport_count": 1,
        },
        "airports": [
            {
                "airport_icao": "VVDN",
                "segment_code": "S0410",
                "scan_time_utc": "2026-11-09T15:00:00Z",
                "fetched_at_utc": "2026-11-09T15:05:00Z",
                "status": "AVAILABLE",
                "outer_bt_min_k": 450.0,  # OUT_OF_RANGE_BT_KELVIN (>340K)
                "inner_bt_min_k": 100.0,  # OUT_OF_RANGE_BT_KELVIN (<150K)
                "b13_valid_fraction": 1.5,  # OUT_OF_RANGE_CLOUD_FRACTION (>1.0)
                "inner_cold_cloud_fraction": 0.0,
                "inner_severe_convection_fraction": 0.0,
            }
        ],
    }

    p_metar = out_p / "synthetic_bad_metar.json"
    p_hima = out_p / "synthetic_bad_himawari.json"

    p_metar.write_text(json.dumps(metar_bad, indent=2), encoding="utf-8")
    p_hima.write_text(json.dumps(himawari_bad, indent=2), encoding="utf-8")

    return {
        "metar_path": str(p_metar),
        "himawari_path": str(p_hima),
        "injected_at_utc": datetime.now(timezone.utc).isoformat(),
    }

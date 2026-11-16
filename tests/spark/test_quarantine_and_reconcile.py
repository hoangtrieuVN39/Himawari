"""Unit tests for quarantine splitting, deduplication, and reconciliation invariants."""

from pyspark.sql import SparkSession
from src.spark.quarantine import (
    build_reconciliation_report,
    validate_and_split_himawari,
    validate_and_split_metar,
    validate_and_split_open_meteo,
)
from tests.spark.real_data import (
    real_raw_himawari_metrics,
    real_raw_metar,
    real_raw_open_meteo,
)


def test_metar_quarantine_and_reconciliation_invariant(spark: SparkSession):
    raw_df = real_raw_metar(spark)
    valid_df, q_df, rec = validate_and_split_metar(raw_df)

    assert rec["is_reconciled"] is True
    assert rec["rows_read"] == rec["rows_valid_pre_dedup"] + rec["rows_quarantined"]
    assert rec["rows_valid_pre_dedup"] == rec["rows_retained_valid"] + rec["rows_duplicate_dropped"]
    assert valid_df.count() == rec["rows_retained_valid"]
    assert q_df.count() == rec["rows_quarantined"]


def test_open_meteo_quarantine_and_reconciliation_invariant(spark: SparkSession):
    raw_df = real_raw_open_meteo(spark)
    valid_df, q_df, rec = validate_and_split_open_meteo(raw_df)

    assert rec["is_reconciled"] is True
    assert rec["rows_read"] == rec["rows_valid_pre_dedup"] + rec["rows_quarantined"]
    assert rec["rows_valid_pre_dedup"] == rec["rows_retained_valid"] + rec["rows_duplicate_dropped"]


def test_himawari_quarantine_and_reconciliation_invariant(spark: SparkSession):
    raw_df = real_raw_himawari_metrics(spark)
    valid_df, q_df, rec = validate_and_split_himawari(raw_df)

    assert rec["is_reconciled"] is True
    assert rec["rows_read"] == rec["rows_valid_pre_dedup"] + rec["rows_quarantined"]
    assert rec["rows_valid_pre_dedup"] == rec["rows_retained_valid"] + rec["rows_duplicate_dropped"]


def test_comprehensive_reconciliation_report(spark: SparkSession):
    m_raw = real_raw_metar(spark)
    f_raw = real_raw_open_meteo(spark)
    h_raw = real_raw_himawari_metrics(spark)

    _, _, m_rec = validate_and_split_metar(m_raw)
    _, _, f_rec = validate_and_split_open_meteo(f_raw)
    _, _, h_rec = validate_and_split_himawari(h_raw)

    rep = build_reconciliation_report([m_rec, f_rec, h_rec], "data/logs/himawari_file_errors.jsonl")
    assert rep["all_sources_reconciled"] is True
    assert len(rep["sources"]) == 3
    assert "binary_file_errors" in rep


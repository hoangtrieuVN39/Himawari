"""End-to-end pipeline integration and idempotency tests."""

from pyspark.sql import SparkSession
from src.spark.run_pipeline import run_spark_pipeline


def test_pipeline_end_to_end_and_idempotency(spark: SparkSession):
    # Run 1: with synthetic quarantine injection
    res1 = run_spark_pipeline(
        storage_root="data",
        review_slots=["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z"],
        mode="replay",
        inject_quarantine_test=True,
        spark=spark,
    )
    assert res1["status"] == "SUCCESS"
    assert res1["decision_rows_count"] == 6  # 2 review slots * 3 airports
    assert res1["quarantine_rows_count"] >= 4  # Includes synthetic invalid records
    assert res1["all_sources_reconciled"] is True

    # Run 2: same input -> identical decision count and reconciliation
    res2 = run_spark_pipeline(
        storage_root="data",
        review_slots=["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z"],
        mode="replay",
        inject_quarantine_test=False,
        spark=spark,
    )
    assert res2["status"] == "SUCCESS"
    assert res2["decision_rows_count"] == 6
    assert res2["all_sources_reconciled"] is True


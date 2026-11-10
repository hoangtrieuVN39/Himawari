"""End-to-end pipeline integration and idempotency tests."""

import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from src.spark.run_pipeline import run_spark_pipeline


@pytest.fixture
def pipeline_root(tmp_path: Path) -> str:
    """Copy of data/ without heavy HSD binaries or generated outputs, so the pipeline never writes to the repo."""
    src = Path("data").resolve()
    dest = tmp_path / "data"

    def ignore(dir_path: str, names: list[str]) -> list[str]:
        rel = Path(dir_path).relative_to(src)
        ignored = [n for n in names if n.endswith(".tmp") or n == ".storage-locks"]
        if rel == Path("raw") and "himawari_hsd" in names:
            ignored.append("himawari_hsd")
        if rel == Path("processed"):
            ignored += [n for n in names if n in ("decision_table", "quarantine", "reconciliation")]
        return ignored

    shutil.copytree(src, dest, ignore=ignore)
    return str(dest)


def test_pipeline_end_to_end_and_idempotency(spark: SparkSession, pipeline_root: str):
    # Run 1: with synthetic quarantine injection
    res1 = run_spark_pipeline(
        storage_root=pipeline_root,
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
        storage_root=pipeline_root,
        review_slots=["2026-11-09T07:00:00Z", "2026-11-09T08:00:00Z"],
        mode="replay",
        inject_quarantine_test=False,
        spark=spark,
    )
    assert res2["status"] == "SUCCESS"
    assert res2["decision_rows_count"] == 6
    assert res2["all_sources_reconciled"] is True

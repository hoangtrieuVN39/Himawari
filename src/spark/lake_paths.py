"""Medallion lake layout shared by Spark writers and pure-Python readers (dashboard, diagnostics).

Every table is stored as immutable snapshot versions:

    <lake_root>/<table path>/run_id=<YYYYMMDDTHHMMSSffffffZ>/[partition=.../]part-*.parquet
                                                         /_SUCCESS

A version is visible only once Spark has written its `_SUCCESS` marker, so readers never see a
half-written snapshot; the newest committed version is the current table.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

LAKE_DIRNAME = "lake"
VERSION_PREFIX = "run_id="
SUCCESS_MARKER = "_SUCCESS"
KEEP_VERSIONS = 2

TABLES: dict[str, str] = {
    # Silver: validated, typed, deduplicated source records (Bronze stays in raw_compact/ JSONL)
    "metar_observations": "silver/metar_observations",
    "open_meteo_forecasts": "silver/open_meteo_forecasts",
    "himawari_b13_metrics": "silver/himawari_b13_metrics",
    "quarantine": "silver/quarantine",
    # Gold: decision-ready and audit tables
    "decision_table": "gold/decision_table",
    "reconciliation": "gold/reconciliation",
    "verification": "gold/verification",
    "feature_table": "gold/feature_table",
    "nowcast": "gold/nowcast",
}


def default_lake_root(storage_root: str) -> str:
    """Lake root next to Bronze: `<storage_root>/lake` (works for local paths and hdfs:// URIs)."""
    root = str(storage_root).rstrip("/\\")
    return f"{root}/{LAKE_DIRNAME}"


def table_uri(lake_root: str, table: str) -> str:
    """URI of a table directory (all versions live below it)."""
    if table not in TABLES:
        raise ValueError(f"Unknown lake table '{table}'. Known: {sorted(TABLES)}")
    return f"{str(lake_root).rstrip('/')}/{TABLES[table]}"


def new_run_id(now: datetime | None = None) -> str:
    """Lexicographically sortable UTC run id, e.g. 20261116T061141123456Z."""
    dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%S%fZ")


def latest_local_version(table_dir: Path) -> Path | None:
    """Newest committed version directory of a local table, or None."""
    if not table_dir.is_dir():
        return None
    committed = [
        p
        for p in table_dir.iterdir()
        if p.is_dir() and p.name.startswith(VERSION_PREFIX) and (p / SUCCESS_MARKER).exists()
    ]
    return max(committed, key=lambda p: p.name) if committed else None


def local_table_version(lake_root: str | Path, table: str) -> Path | None:
    """Newest committed version of `table` under a local lake root."""
    return latest_local_version(Path(table_uri(str(lake_root), table)))

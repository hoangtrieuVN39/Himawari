"""Spark writers/readers for the versioned Parquet lake (local paths and hdfs:// URIs alike).

Uses the Hadoop FileSystem API through the active SparkSession, so the same code commits,
lists and garbage-collects versions on the local disk and on HDFS.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.spark.lake_paths import KEEP_VERSIONS, SUCCESS_MARKER, VERSION_PREFIX, table_uri


def _fs_and_path(spark: SparkSession, uri: str):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs, path


def _version_dirs(spark: SparkSession, table_dir: str) -> list[tuple[str, bool]]:
    """(name, committed) of every run_id=... directory, oldest first."""
    fs, path = _fs_and_path(spark, table_dir)
    if not fs.exists(path):
        return []
    out = []
    for status in fs.listStatus(path):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith(VERSION_PREFIX):
            marker = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(status.getPath(), SUCCESS_MARKER)
            out.append((name, bool(fs.exists(marker))))
    return sorted(out)


def _committed_versions(spark: SparkSession, table_dir: str) -> list[str]:
    """Names (run_id=...) of committed versions, oldest first."""
    return [name for name, committed in _version_dirs(spark, table_dir) if committed]


def latest_version_uri(spark: SparkSession, lake_root: str, table: str) -> str | None:
    """URI of the newest committed version of `table`, or None when the table has none."""
    table_dir = table_uri(lake_root, table)
    versions = _committed_versions(spark, table_dir)
    return f"{table_dir}/{versions[-1]}" if versions else None


def read_table(spark: SparkSession, lake_root: str, table: str) -> DataFrame | None:
    """Current snapshot of `table` (newest committed version), or None."""
    uri = latest_version_uri(spark, lake_root, table)
    return spark.read.parquet(uri) if uri else None


def write_table_version(
    df: DataFrame,
    lake_root: str,
    table: str,
    run_id: str,
    partition_by: list[str] | None = None,
    keep_versions: int = KEEP_VERSIONS,
) -> str:
    """Writes `df` as a new immutable version and prunes versions beyond `keep_versions`.

    The version becomes visible to readers only when Spark writes its `_SUCCESS` marker at the
    end of the job; older versions are deleted only after that, so readers always find a
    complete snapshot. Returns the URI of the new version.
    """
    spark = df.sparkSession
    table_dir = table_uri(lake_root, table)
    version_uri = f"{table_dir}/{VERSION_PREFIX}{run_id}"
    out = df.repartition(*partition_by) if partition_by else df.coalesce(1)
    # Static mode: every version is a fresh directory, and dynamic partition overwrite skips the
    # _SUCCESS marker that readers use as the commit signal.
    writer = out.write.mode("overwrite").option("partitionOverwriteMode", "static")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(version_uri)

    fs, _ = _fs_and_path(spark, table_dir)
    dirs = _version_dirs(spark, table_dir)
    committed = [name for name, ok in dirs if ok]
    keep = set(committed[-keep_versions:])
    newest = committed[-1] if committed else ""
    for name, ok in dirs:
        # Prune old committed versions, and abandoned (crashed) writes older than the newest commit.
        if name not in keep and (ok or name < newest):
            fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"{table_dir}/{name}"), True)
    return version_uri

"""SparkSession factory with UTC session timezone and dual HDFS/local support."""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

_REPO_ROOT = Path(__file__).resolve().parents[2]

_SPARK_INSTANCE: SparkSession | None = None


def get_spark_session(
    app_name: str = "AirportWeatherReviewSpark",
    master: str = "local[*]",
    force_new: bool = False,
) -> SparkSession:
    """Returns or creates a SparkSession configured for UTC and fast local/Docker execution.

    Ensures:
      - spark.sql.session.timeZone = "UTC"
      - Arrow optimization enabled
      - UI disabled for fast test execution
      - Predictable local execution without requiring winutils.exe on Windows for memory DataFrames
    """
    global _SPARK_INSTANCE
    if _SPARK_INSTANCE is not None and not force_new:
        try:
            if not _SPARK_INSTANCE.sparkContext._jsc.sc().isStopped():
                return _SPARK_INSTANCE
        except Exception:
            pass

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if os.name == "nt":
        # Local Parquet writes on Windows need winutils.exe/hadoop.dll; the repo ships them.
        hadoop_home = _REPO_ROOT / "config" / "hadoop"
        if "HADOOP_HOME" not in os.environ and (hadoop_home / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(hadoop_home)
            os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")
        if "JAVA_HOME" not in os.environ and os.path.exists(r"C:\Program Files\Java\jdk-17"):
            os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
            os.environ["PATH"] = rf"{os.environ['JAVA_HOME']}\bin;" + os.environ.get("PATH", "")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )

    _SPARK_INSTANCE = builder.getOrCreate()
    return _SPARK_INSTANCE


def stop_spark_session() -> None:
    """Stops the global SparkSession if running."""
    global _SPARK_INSTANCE
    if _SPARK_INSTANCE is not None:
        try:
            _SPARK_INSTANCE.stop()
        except Exception:
            pass
        _SPARK_INSTANCE = None

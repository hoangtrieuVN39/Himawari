"""SparkSession factory with UTC session timezone and dual HDFS/local support."""

import os
from pyspark.sql import SparkSession

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

    import sys

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    python_path = sys.executable
    if os.path.exists(r"Z:\.venv\Scripts\python.exe"):
        python_path = r"Z:\.venv\Scripts\python.exe"
        os.environ["SPARK_HOME"] = r"Z:\.venv\Lib\site-packages\pyspark"
        if os.path.exists(r"Z:\config\hadoop"):
            os.environ["HADOOP_HOME"] = r"Z:\config\hadoop"
            os.environ["PATH"] = rf"Z:\config\hadoop\bin;" + os.environ.get("PATH", "")

    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
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

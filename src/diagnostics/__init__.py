"""Operational diagnostics (E3): freshness SLA, coverage, gap attribution, anomalies, drift, profile.

Entry point: ``python -m src.diagnostics.run_health`` (writes Parquet + summary.json under
``<storage_root>/lake/gold/health/``). The Streamlit tab only reads those artifacts.
"""

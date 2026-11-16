"""Builds the small, self-contained sample dataset for submission (E11).

Contents (relative to the archive root):
  data/raw_compact/<source>/date=<D>/batches-0001.jsonl  live envelopes of the chosen dates only
                                                          (back-filled envelopes are dropped)
  data/storage_state.json                                 daily_jsonl marker so the pipeline can rerun
  data/processed/himawari_png/airport=*/date=<D>/*.png    B13 frames of those dates
  data/lake/gold/<table>/run_id=.../, silver/quarantine   newest committed Gold versions + quarantine
                                                          (Silver source tables are rebuilt by the pipeline
                                                          from the sample Bronze; they hold back-filled rows)
  data/logs/*.jsonl                                       audit logs (ingestion, file errors, HDFS, pipeline)
  config/airports.json, config/replay_slots.json
  MANIFEST.json                                           counts, dates, exclusions, attribution

Never packs credentials: files whose name or content matches common secret patterns are refused.

Usage:
  python scripts/package_sample.py --storage-root data --out dist/sample_dataset.zip [--dates 2026-11-09,2026-11-16]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.spark.lake_paths import TABLES, default_lake_root, local_table_version  # noqa: E402
from src.spark.replay_slots import load_replay_slots  # noqa: E402

SOURCES = ("metar", "open_meteo", "himawari_metrics")
LOG_FILES = ("ingestion_log.jsonl", "himawari_file_errors.jsonl", "hdfs_sync_log.jsonl", "pipeline_runs.jsonl")
SECRET_PATTERN = re.compile(
    r"(aws_secret_access_key|aws_access_key_id|api[_-]?key|secret[_-]?key|password|BEGIN (RSA|OPENSSH) PRIVATE KEY)",
    re.IGNORECASE,
)
ATTRIBUTION = [
    "Himawari-9 B13: JMA/NOAA via AWS Open Data (noaa-himawari9); PNG/metrics are processed products",
    "METAR: NOAA/NWS Aviation Weather Center (https://aviationweather.gov/data/api/)",
    "Open-Meteo forecast (https://open-meteo.com/, CC BY 4.0)",
]


class SecretFoundError(RuntimeError):
    """Raised instead of packing a file that looks like it holds a credential."""


def _check_no_secret(name: str, payload: bytes) -> None:
    if SECRET_PATTERN.search(name) or SECRET_PATTERN.search(payload.decode("utf-8", errors="ignore")):
        raise SecretFoundError(f"Refusing to pack '{name}': matches a credential pattern")


def _add(zf: zipfile.ZipFile, arcname: str, payload: bytes) -> None:
    _check_no_secret(arcname, payload)
    zf.writestr(arcname, payload)


def _live_lines(shard: Path) -> tuple[list[str], int]:
    """Lines of a daily shard without back-filled envelopes; returns (kept lines, dropped count)."""
    kept, dropped = [], 0
    for line in shard.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            meta = json.loads(line).get("metadata") or {}
        except ValueError:
            kept.append(line)  # corrupt lines stay: they demonstrate quarantine
            continue
        if meta.get("is_backfill"):
            dropped += 1
        else:
            kept.append(line)
    return kept, dropped


def build_sample(storage_root: str, out_path: str, dates: list[str]) -> dict:
    root = Path(storage_root)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dates": dates,
        "replay_slots_utc": load_replay_slots(),
        "bronze_envelopes": {},
        "backfill_envelopes_dropped": 0,
        "lake_versions": {},
        "png_frames": 0,
        "exclusions": ["back-filled envelopes", "Silver source tables (rebuilt by the pipeline)",
                       "raw .DAT.bz2 segments", "credentials", "KPI participant logs"],
        "attribution": ATTRIBUTION,
    }
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for source in SOURCES:
            count = 0
            for date in dates:
                day_dir = root / "raw_compact" / source / f"date={date}"
                lines: list[str] = []
                for shard in sorted(day_dir.glob("batches-*.jsonl")):
                    kept, dropped = _live_lines(shard)
                    lines += kept
                    manifest["backfill_envelopes_dropped"] += dropped
                if lines:
                    _add(zf, f"data/raw_compact/{source}/date={date}/batches-0001.jsonl",
                         ("\n".join(lines) + "\n").encode("utf-8"))
                    count += len(lines)
            manifest["bronze_envelopes"][source] = count
        _add(zf, "data/storage_state.json", json.dumps(
            {"schema_version": 1, "raw_layout": "daily_jsonl", "active_sources": list(SOURCES),
             "shard_max_bytes": 64 * 1024 * 1024, "updated_at_utc": manifest["generated_at_utc"]}, indent=2
        ).encode("utf-8"))

        for date in dates:
            for png in sorted((root / "processed" / "himawari_png").glob(f"airport=*/date={date}/*.png")):
                zf.write(png, Path("data", png.relative_to(root)).as_posix())
                manifest["png_frames"] += 1

        lake = default_lake_root(storage_root)
        for table in [t for t, rel in TABLES.items() if rel.startswith("gold/") or t == "quarantine"]:
            version = local_table_version(lake, table)
            if version is None:
                continue
            manifest["lake_versions"][table] = version.name
            for f in sorted(p for p in version.rglob("*") if p.is_file() and not p.name.endswith(".crc")):
                _add(zf, Path("data", "lake", version.relative_to(Path(lake)), f.relative_to(version)).as_posix(),
                     f.read_bytes())
        health = Path(lake) / "gold" / "health"
        for f in sorted(health.glob("*")) if health.is_dir() else []:
            _add(zf, f"data/lake/gold/health/{f.name}", f.read_bytes())

        for name in LOG_FILES:
            p = root / "logs" / name
            if p.exists():
                _add(zf, f"data/logs/{name}", p.read_bytes())
        for cfg in ("airports.json", "replay_slots.json"):
            p = REPO_ROOT / "config" / cfg
            _add(zf, f"config/{cfg}", p.read_bytes())
        _add(zf, "MANIFEST.json", json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
    manifest["archive"] = str(out)
    manifest["archive_bytes"] = out.stat().st_size
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Package the submission sample dataset")
    parser.add_argument("--storage-root", default="data")
    parser.add_argument("--out", default="dist/sample_dataset.zip")
    parser.add_argument("--dates", default=None, help="Comma list of UTC dates (default: replay dates + today)")
    args = parser.parse_args(argv)
    dates = [d.strip() for d in args.dates.split(",")] if args.dates else sorted(
        {s[:10] for s in load_replay_slots()} | {datetime.now(timezone.utc).strftime("%Y-%m-%d")}
    )
    print(json.dumps(build_sample(args.storage_root, args.out, dates), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

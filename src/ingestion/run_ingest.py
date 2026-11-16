import argparse
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from src.ingestion.config_loader import load_airports_config
from src.ingestion.metar_client import ingest_metar
from src.ingestion.metar_backfill import MAX_BACKFILL_DAYS, backfill_metar
from src.ingestion.open_meteo_client import ingest_open_meteo
from src.ingestion.himawari_client import (
    download_himawari_slot,
    find_latest_available_slot,
    cleanup_old_raw_segments,
)
from src.ingestion.satpy_processor import (
    process_himawari_slot,
    inject_synthetic_corrupt_test,
)
from src.ingestion.hdfs_client import WebHDFSClient


def _backfill_days(value: str) -> int:
    try:
        days = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"--backfill-days must be an integer, got {value!r}")
    if not 1 <= days <= MAX_BACKFILL_DAYS:
        raise argparse.ArgumentTypeError(f"--backfill-days must be in 1..{MAX_BACKFILL_DAYS} (AWC keeps 30 days), got {days}")
    return days


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Airports Weather & Satellite Data Ingestor (METAR, Open-Meteo & Himawari-9)"
    )
    parser.add_argument(
        "--config",
        default="config/airports.json",
        help="Path to airports configuration JSON"
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="Base directory for output data and logs"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run single ingestion cycle and exit"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuous scheduled loop (METAR/Himawari every 10m, forecast every 60m)"
    )
    parser.add_argument(
        "--backfill-hours",
        type=int,
        default=2,
        help="Hours of historical METAR to request (default: 2; e.g. 24 for backfill)"
    )
    parser.add_argument(
        "--backfill-days",
        type=_backfill_days,
        default=None,
        metavar="N",
        help="Backfill N days (1..30) of historical METAR/SPECI in 24 h windows per airport "
             "(is_backfill=true, rate limited, reruns are no-ops); then exit unless --once/--loop"
    )
    parser.add_argument(
        "--include-himawari",
        action="store_true",
        help="Fetch and process latest Himawari-9 B13 slot during --once or --loop"
    )
    parser.add_argument(
        "--himawari-window",
        nargs=2,
        metavar=("START_ISO", "END_ISO"),
        default=None,
        help="Backfill all 10-minute Himawari-9 slots in [START_ISO, END_ISO]"
    )
    parser.add_argument(
        "--sync-hdfs",
        action="store_true",
        help="Sync local data/ directory to Hadoop HDFS and record benchmark"
    )
    parser.add_argument(
        "--inject-corrupt-test",
        action="store_true",
        help="Generate labeled synthetic corrupt .DAT.bz2 file and log to himawari_file_errors.jsonl"
    )
    parser.add_argument(
        "--retain-raw-hours",
        type=int,
        default=24,
        help="Retention window for raw .DAT.bz2 segments (default: 24)"
    )
    return parser.parse_args(args)


def run_himawari_slot_pipeline(
    scan_dt_utc: datetime,
    airports_cfg: Dict[str, Any],
    base_dir: str = "data",
    is_backfill: bool = False,
    retain_raw_hours: int = 24,
    pinned_slots: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Executes end-to-end Himawari-9 slot ingestion:
    1. Determines required segments from airports_cfg
    2. Downloads segments (with automatic fallback to latest available date on S3 if clock skewed/archive missing)
    3. Processes slot with Satpy (cropping, metrics extraction, PNG rendering)
    4. Cleans up expired raw segments preserving pinned and unprocessed files
    """
    if scan_dt_utc.tzinfo is None:
        scan_dt_utc = scan_dt_utc.replace(tzinfo=timezone.utc)
    else:
        scan_dt_utc = scan_dt_utc.astimezone(timezone.utc)

    slot_iso = scan_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    segments = sorted({cfg.get("himawari_segment", "S0410") for cfg in airports_cfg.values()})

    # 1. Download requested slot
    dl_res = download_himawari_slot(
        scan_dt_utc,
        segments=segments,
        base_dir=base_dir,
        is_backfill=is_backfill,
    )

    # 2. Clock-skew / archive fallback
    if dl_res.get("status") == "FAILED":
        try:
            latest_avail = find_latest_available_slot(scan_dt_utc)
            # Try preserving requested hour and minute first
            candidate = latest_avail.replace(
                hour=scan_dt_utc.hour,
                minute=scan_dt_utc.minute,
                second=0,
                microsecond=0,
            )
            fallback_dl = download_himawari_slot(
                scan_dt_utc,
                segments=segments,
                base_dir=base_dir,
                is_backfill=is_backfill,
                source_dt_utc=candidate,
            )
            if fallback_dl.get("status") != "FAILED":
                dl_res = fallback_dl
            else:
                dl_res = download_himawari_slot(
                    scan_dt_utc,
                    segments=segments,
                    base_dir=base_dir,
                    is_backfill=is_backfill,
                    source_dt_utc=latest_avail,
                )
        except Exception as exc:
            print(f"[{slot_iso}] Himawari archive fallback warning: {exc}")

    # 3. Process slot with Satpy
    segment_files = dl_res.get("files", {})
    metrics_res = process_himawari_slot(
        # A fallback download is a different scan: keep its real time so the point-in-time join
        # treats it as older imagery instead of evidence for the requested slot.
        scan_slot_utc=dl_res.get("source_slot_utc") or slot_iso,
        segment_files=segment_files,
        airports_config=airports_cfg,
        base_dir=base_dir,
        is_backfill=is_backfill,
    )

    # 4. Clean up old raw segments
    deleted_count = cleanup_old_raw_segments(
        base_dir=base_dir,
        retain_hours=retain_raw_hours,
        pinned_slots=pinned_slots,
    )

    return {
        "scan_slot_utc": slot_iso,
        "download": dl_res,
        "metrics": metrics_res,
        "deleted_segments": deleted_count,
        "status": dl_res.get("status", "UNKNOWN"),
    }


def run_himawari_window_backfill(
    start_iso: str,
    end_iso: str,
    config_path: str = "config/airports.json",
    base_dir: str = "data",
    retain_raw_hours: int = 24,
    airports_cfg: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Backfills all 10-minute Himawari-9 slots in [start_iso, end_iso] inclusive.
    Collects all slot timestamps in pinned_slots so cleanup does not delete replay data.
    """
    clean_start = start_iso.replace("Z", "+00:00")
    clean_end = end_iso.replace("Z", "+00:00")
    try:
        start_dt = datetime.fromisoformat(clean_start)
    except ValueError:
        start_dt = datetime.strptime(start_iso[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    try:
        end_dt = datetime.fromisoformat(clean_end)
    except ValueError:
        end_dt = datetime.strptime(end_iso[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    else:
        start_dt = start_dt.astimezone(timezone.utc)

    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    else:
        end_dt = end_dt.astimezone(timezone.utc)

    if airports_cfg is None:
        airports_cfg = load_airports_config(config_path)

    # Generate 10-minute slots
    slot_dts: List[datetime] = []
    curr = start_dt
    while curr <= end_dt:
        slot_dts.append(curr)
        curr += timedelta(minutes=10)

    pinned_slots = [dt.strftime("%Y-%m-%dT%H:%M:%SZ") for dt in slot_dts]
    print(f"Starting Himawari-9 window backfill: {len(slot_dts)} slots between {start_iso} and {end_iso}")

    results: List[Dict[str, Any]] = []
    for slot_dt in slot_dts:
        slot_iso = slot_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"--> Processing backfill slot {slot_iso} ...")
        res = run_himawari_slot_pipeline(
            scan_dt_utc=slot_dt,
            airports_cfg=airports_cfg,
            base_dir=base_dir,
            is_backfill=True,
            retain_raw_hours=retain_raw_hours,
            pinned_slots=pinned_slots,
        )
        results.append(res)

    print(f"Window backfill completed: {len(results)} slots processed.")
    return results


def run_cycle(
    config_path: str = "config/airports.json",
    base_dir: str = "data",
    backfill_hours: int = 2,
    is_backfill: bool = False,
    fetch_forecast: bool = True,
    include_himawari: bool = False,
    retain_raw_hours: int = 24,
    sync_hdfs: bool = False,
) -> Dict[str, Any]:
    """
    Executes a single coordinated ingestion pass:
    1. Loads airports config
    2. Ingests METAR for all airports
    3. (Optionally) Ingests Open-Meteo forecast for all airports
    4. (Optionally) Ingests latest Himawari-9 B13 slot and computes Satpy metrics/PNGs
    5. (Optionally) Synchronizes data directory to Hadoop HDFS
    """
    airports_cfg = load_airports_config(config_path)
    airport_codes = list(airports_cfg.keys())

    now_utc = datetime.now(timezone.utc)
    now_utc_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now_utc_str}] Starting ingestion cycle for {len(airport_codes)} airports: {airport_codes}")

    metar_res = ingest_metar(
        airports=airport_codes,
        hours=backfill_hours,
        is_backfill=is_backfill,
        base_dir=base_dir,
    )
    print(f"[{now_utc_str}] METAR: {metar_res['status']} ({metar_res.get('record_count', 0)} records)")

    forecast_results = []
    if fetch_forecast:
        forecast_results = ingest_open_meteo(
            airports_config=airports_cfg,
            base_dir=base_dir,
        )
        for fres in forecast_results:
            airport_name = fres.get("airport", "UNKNOWN")
            print(f"[{now_utc_str}] Open-Meteo [{airport_name}]: {fres['status']} ({fres.get('record_count', 0)} hrs)")

    himawari_res = None
    if include_himawari:
        try:
            latest_slot = find_latest_available_slot(now_utc)
        except Exception:
            # Fallback to now rounded down to 10 min
            target_min = (now_utc.minute // 10) * 10
            latest_slot = now_utc.replace(minute=target_min, second=0, microsecond=0)

        print(f"[{now_utc_str}] Himawari-9: processing slot {latest_slot.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        himawari_res = run_himawari_slot_pipeline(
            scan_dt_utc=latest_slot,
            airports_cfg=airports_cfg,
            base_dir=base_dir,
            is_backfill=is_backfill,
            retain_raw_hours=retain_raw_hours,
        )
        print(f"[{now_utc_str}] Himawari-9: {himawari_res.get('status', 'COMPLETED')}")

    hdfs_res = None
    hdfs_client = WebHDFSClient()
    if sync_hdfs or hdfs_client.is_available():
        hdfs_res = hdfs_client.sync_directory(
            local_dir=base_dir,
            log_path=f"{base_dir}/logs/hdfs_sync_log.jsonl",
        )
        print(f"[{now_utc_str}] HDFS sync: {hdfs_res.get('status')} ({hdfs_res.get('files_uploaded', 0)} files, {hdfs_res.get('throughput_mb_s', 0.0)} MB/s)")

    return {
        "timestamp_utc": now_utc_str,
        "metar": metar_res,
        "open_meteo": forecast_results,
        "himawari": himawari_res,
        "hdfs_sync": hdfs_res,
    }


def main():
    args = parse_args()

    # 1. Synthetic corruption injection test
    if args.inject_corrupt_test:
        print(f"--- Injecting synthetic corrupt Himawari test into {args.base_dir} ---")
        corrupt_res = inject_synthetic_corrupt_test(base_dir=args.base_dir)
        err_type = corrupt_res.get("error_type", "UnknownError")
        err_msg = corrupt_res.get("error_message", "")
        print(f"Synthetic corruption logged: {err_type} - {err_msg}")
        if (
            not args.once and not args.loop and not args.himawari_window
            and not args.sync_hdfs and not args.backfill_days
        ):
            return

    # 1b. METAR window backfill (AWC keeps only 30 days; one request per airport per 24 h window)
    if args.backfill_days:
        airport_codes = list(load_airports_config(args.config).keys())
        print(f"--- Running METAR backfill: {args.backfill_days} days for {airport_codes} ---")
        backfill_summary = backfill_metar(
            airports=airport_codes,
            days=args.backfill_days,
            base_dir=args.base_dir,
        )
        print(
            "METAR backfill done: "
            f"{backfill_summary['requests']} requests, {backfill_summary['records_total']} records "
            f"({backfill_summary['inserted']} batches inserted, {backfill_summary['noop']} already present, "
            f"{backfill_summary['no_data']} empty, {backfill_summary['conflicts']} conflicts skipped, "
            f"{backfill_summary['failed']} failed)"
        )
        if not args.once and not args.loop and not args.himawari_window and not args.sync_hdfs:
            return

    # 2. Window backfill mode
    if args.himawari_window:
        start_iso, end_iso = args.himawari_window
        print(f"--- Running Himawari window backfill [{start_iso} -> {end_iso}] ---")
        run_himawari_window_backfill(
            start_iso=start_iso,
            end_iso=end_iso,
            config_path=args.config,
            base_dir=args.base_dir,
            retain_raw_hours=args.retain_raw_hours,
        )
        if args.sync_hdfs:
            print("--- Running HDFS sync after window backfill ---")
            WebHDFSClient().sync_directory(local_dir=args.base_dir, log_path=f"{args.base_dir}/logs/hdfs_sync_log.jsonl")
        if not args.once and not args.loop:
            return

    # 3. Direct HDFS sync standalone
    if args.sync_hdfs and not args.once and not args.loop:
        print(f"--- Synchronizing local {args.base_dir} to HDFS ---")
        res = WebHDFSClient().sync_directory(local_dir=args.base_dir, log_path=f"{args.base_dir}/logs/hdfs_sync_log.jsonl")
        print(f"HDFS sync status: {res.get('status')} ({res.get('files_uploaded', 0)} files, {res.get('throughput_mb_s', 0.0)} MB/s)")
        return

    is_backfill = args.backfill_hours > 2

    # 4. Single cycle mode
    if args.once or not args.loop:
        print(f"--- Running single cycle (backfill_hours={args.backfill_hours}, include_himawari={args.include_himawari}) ---")
        run_cycle(
            config_path=args.config,
            base_dir=args.base_dir,
            backfill_hours=args.backfill_hours,
            is_backfill=is_backfill,
            fetch_forecast=True,
            include_himawari=args.include_himawari,
            retain_raw_hours=args.retain_raw_hours,
            sync_hdfs=args.sync_hdfs,
        )
        print("Cycle finished.")
        return

    # 5. Continuous daemon loop mode
    print(f"--- Starting daemon loop (METAR: 10m, Open-Meteo: 60m, Himawari: {args.include_himawari}) ---")
    last_metar_slot = None
    last_forecast_hour = None

    try:
        while True:
            now = datetime.now(timezone.utc)
            current_slot = (now.hour, (now.minute // 10) * 10)  # 10-minute slot
            current_hour = (now.day, now.hour)

            run_metar = (current_slot != last_metar_slot)
            run_forecast = (current_hour != last_forecast_hour)

            if run_metar or run_forecast:
                run_cycle(
                    config_path=args.config,
                    base_dir=args.base_dir,
                    backfill_hours=2,
                    is_backfill=False,
                    fetch_forecast=run_forecast,
                    include_himawari=args.include_himawari,
                    retain_raw_hours=args.retain_raw_hours,
                    sync_hdfs=args.sync_hdfs,
                )
                last_metar_slot = current_slot
                if run_forecast:
                    last_forecast_hour = current_hour

            # Sleep 15 seconds before checking clock slot again
            time.sleep(15)
    except KeyboardInterrupt:
        print("\nIngestor loop stopped by user.")


if __name__ == "__main__":
    main()

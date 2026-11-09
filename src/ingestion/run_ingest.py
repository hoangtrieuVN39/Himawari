import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from src.ingestion.config_loader import load_airports_config
from src.ingestion.metar_client import ingest_metar
from src.ingestion.open_meteo_client import ingest_open_meteo

def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Airports Weather Data Ingestor (METAR & Open-Meteo)"
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
        help="Run continuous scheduled loop (METAR every 10m, forecast every 60m)"
    )
    parser.add_argument(
        "--backfill-hours",
        type=int,
        default=2,
        help="Hours of historical METAR to request (default: 2; e.g. 24 for backfill)"
    )
    return parser.parse_args(args)

def run_cycle(
    config_path: str = "config/airports.json",
    base_dir: str = "data",
    backfill_hours: int = 2,
    is_backfill: bool = False,
    fetch_forecast: bool = True
) -> Dict[str, Any]:
    """
    Executes a single coordinated ingestion pass:
    1. Loads airports config
    2. Ingests METAR for all airports
    3. (Optionally) Ingests Open-Meteo forecast for all airports
    """
    airports_cfg = load_airports_config(config_path)
    airport_codes = list(airports_cfg.keys())

    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now_utc_str}] Starting ingestion cycle for {len(airport_codes)} airports: {airport_codes}")

    metar_res = ingest_metar(
        airports=airport_codes,
        hours=backfill_hours,
        is_backfill=is_backfill,
        base_dir=base_dir
    )
    print(f"[{now_utc_str}] METAR: {metar_res['status']} ({metar_res.get('record_count', 0)} records)")

    forecast_results = []
    if fetch_forecast:
        forecast_results = ingest_open_meteo(
            airports_config=airports_cfg,
            base_dir=base_dir
        )
        for fres in forecast_results:
            airport_name = fres.get("airport", "UNKNOWN")
            print(f"[{now_utc_str}] Open-Meteo [{airport_name}]: {fres['status']} ({fres.get('record_count', 0)} hrs)")

    return {
        "timestamp_utc": now_utc_str,
        "metar": metar_res,
        "open_meteo": forecast_results
    }

def main():
    args = parse_args()

    is_backfill = args.backfill_hours > 2

    if args.once or not args.loop:
        print(f"--- Running single cycle (backfill_hours={args.backfill_hours}) ---")
        run_cycle(
            config_path=args.config,
            base_dir=args.base_dir,
            backfill_hours=args.backfill_hours,
            is_backfill=is_backfill,
            fetch_forecast=True
        )
        print("Cycle finished.")
        return

    # Continuous loop mode
    print("--- Starting daemon loop (METAR: 10m, Open-Meteo: 60m) ---")
    last_metar_slot = None
    last_forecast_hour = None

    try:
        while True:
            now = datetime.now(timezone.utc)
            current_slot = (now.hour, (now.minute // 10) * 10) # 10-minute slot
            current_hour = (now.day, now.hour)

            run_metar = (current_slot != last_metar_slot)
            run_forecast = (current_hour != last_forecast_hour)

            if run_metar or run_forecast:
                run_cycle(
                    config_path=args.config,
                    base_dir=args.base_dir,
                    backfill_hours=2,
                    is_backfill=False,
                    fetch_forecast=run_forecast
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

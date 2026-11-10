import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from src.ingestion.http_client import fetch_with_retry
from src.ingestion.storage import append_ingestion_log
from src.ingestion.raw_store import RawEnvelopeStore

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ATTRIBUTION = "Weather data by Open-Meteo.com (https://open-meteo.com/)"

def ingest_open_meteo(
    airports_config: Dict[str, Dict[str, Any]],
    expected_slot_utc: Optional[str] = None,
    base_dir: str = "data"
) -> List[Dict[str, Any]]:
    """
    Fetches hourly weather & precipitation forecast for each configured airport
    from the Open-Meteo API. Saves individual raw envelopes and appends audit logs.
    """
    now = datetime.now(timezone.utc)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str = now.strftime("%Y-%m-%d")
    timestamp_compact = now.strftime("%Y%m%dT%H%M%SZ")

    if not expected_slot_utc:
        expected_slot_utc = now_iso

    log_path = str(Path(base_dir) / "logs" / "ingestion_log.jsonl")
    results: List[Dict[str, Any]] = []

    for icao, airport in airports_config.items():
        icao_clean = icao.upper()
        lat = airport["latitude"]
        lon = airport["longitude"]
        random_suffix = uuid.uuid4().hex[:6]
        batch_id = f"{timestamp_compact}_open_meteo_{icao_clean}_{random_suffix}"

        params = {
            "latitude": str(lat),
            "longitude": str(lon),
            "hourly": "precipitation,rain,showers,weather_code,visibility,wind_speed_10m,wind_gusts_10m",
            "timezone": "UTC",
            "forecast_days": "2"
        }

        try:
            response = fetch_with_retry(OPEN_METEO_BASE_URL, params=params)
            payload = response.json()
            hourly_data = payload.get("hourly", {})
            forecast_hours_count = len(hourly_data.get("time", []))

            envelope = {
                "metadata": {
                    "batch_id": batch_id,
                    "source": "open_meteo_forecast",
                    "airport_icao": icao_clean,
                    "airport_name": airport.get("name", ""),
                    "latitude": lat,
                    "longitude": lon,
                    "source_url": response.url,
                    "attribution": OPEN_METEO_ATTRIBUTION,
                    "expected_slot_utc": expected_slot_utc,
                    "fetched_at_utc": now_iso,
                    "http_status": response.status_code,
                    "forecast_hours_count": forecast_hours_count
                },
                "payload": payload
            }

            store = RawEnvelopeStore(storage_root=base_dir)
            receipt = store.write(
                source="open_meteo",
                date_str=date_str,
                envelope=envelope,
            )

            saved_path = receipt.path
            bytes_written = receipt.bytes_added
            storage_layout = store.active_layout()

            log_entry = {
                "batch_id": batch_id,
                "source": "open_meteo",
                "airport": icao_clean,
                "expected_slot_utc": expected_slot_utc,
                "fetched_at_utc": now_iso,
                "status": "SUCCESS",
                "http_status": response.status_code,
                "record_count": forecast_hours_count,
                "bytes_written": bytes_written,
                "file_path": saved_path,
                "storage_layout": storage_layout,
                "is_backfill": False,
                "error_type": None,
                "error_message": None
            }
            append_ingestion_log(log_entry, log_path=log_path)
            results.append(log_entry)

        except Exception as err:
            error_type = type(err).__name__
            error_msg = str(err)

            log_entry = {
                "batch_id": batch_id,
                "source": "open_meteo",
                "airport": icao_clean,
                "expected_slot_utc": expected_slot_utc,
                "fetched_at_utc": now_iso,
                "status": "FAILED",
                "http_status": getattr(getattr(err, "response", None), "status_code", None),
                "record_count": 0,
                "bytes_written": 0,
                "file_path": None,
                "is_backfill": False,
                "error_type": error_type,
                "error_message": error_msg
            }
            append_ingestion_log(log_entry, log_path=log_path)
            results.append(log_entry)

    return results


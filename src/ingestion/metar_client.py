import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

from src.ingestion.http_client import fetch_with_retry
from src.ingestion.storage import append_ingestion_log
from src.ingestion.raw_store import RawEnvelopeStore

METAR_API_BASE_URL = "https://aviationweather.gov/api/data/metar"
METAR_ATTRIBUTION = "NOAA/NWS Aviation Weather Center (https://aviationweather.gov/data/api/)"

# AWC API limits (https://aviationweather.gov/data/api/): max 400 entries per request, 30 days of history.
METAR_MAX_ENTRIES_PER_REQUEST = 400
METAR_RETENTION_DAYS = 30


def parse_metar_payload(response: Any) -> List[Dict[str, Any]]:
    """Decodes an AWC METAR response into a list of observations.

    HTTP 204 (or an empty body) means "no data for this query": returns [] instead of raising.
    """
    if getattr(response, "status_code", None) == 204 or not getattr(response, "content", b"1"):
        return []
    payload = response.json()
    if not isinstance(payload, list):
        payload = [payload] if payload else []
    return payload

def ingest_metar(
    airports: List[str],
    hours: int = 2,
    expected_slot_utc: Optional[str] = None,
    is_backfill: bool = False,
    base_dir: str = "data"
) -> Dict[str, Any]:
    """
    Fetches METAR reports for requested airports from Aviation Weather Center.
    Wraps response into a raw envelope, writes atomically, and appends to ingestion log.
    """
    now = datetime.now(timezone.utc)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str = now.strftime("%Y-%m-%d")
    timestamp_compact = now.strftime("%Y%m%dT%H%M%SZ")

    if not expected_slot_utc:
        expected_slot_utc = now_iso

    random_suffix = uuid.uuid4().hex[:6]
    batch_id = f"{timestamp_compact}_metar_{random_suffix}"
    ids_param = ",".join([code.upper() for code in airports])
    params = {
        "ids": ids_param,
        "format": "json",
        "hours": str(hours)
    }

    log_path = str(Path(base_dir) / "logs" / "ingestion_log.jsonl")

    try:
        response = fetch_with_retry(METAR_API_BASE_URL, params=params)
        payload = parse_metar_payload(response)

        record_count = len(payload)

        envelope = {
            "metadata": {
                "batch_id": batch_id,
                "source": "aviationweather_metar",
                "source_url": response.url,
                "attribution": METAR_ATTRIBUTION,
                "expected_slot_utc": expected_slot_utc,
                "fetched_at_utc": now_iso,
                "http_status": response.status_code,
                "airports_requested": [code.upper() for code in airports],
                "is_backfill": is_backfill,
                "record_count": record_count
            },
            "payload": payload
        }

        store = RawEnvelopeStore(storage_root=base_dir)
        receipt = store.write(
            source="metar",
            date_str=date_str,
            envelope=envelope,
        )

        saved_path = receipt.path
        bytes_written = receipt.bytes_added
        storage_layout = store.active_layout()

        log_entry = {
            "batch_id": batch_id,
            "source": "metar",
            "airport": "ALL",
            "expected_slot_utc": expected_slot_utc,
            "fetched_at_utc": now_iso,
            "status": "SUCCESS",
            "http_status": response.status_code,
            "record_count": record_count,
            "bytes_written": bytes_written,
            "file_path": saved_path,
            "storage_layout": storage_layout,
            "is_backfill": is_backfill,
            "error_type": None,
            "error_message": None
        }
        append_ingestion_log(log_entry, log_path=log_path)

        return log_entry

    except Exception as err:
        error_type = type(err).__name__
        error_msg = str(err)

        log_entry = {
            "batch_id": batch_id,
            "source": "metar",
            "airport": "ALL",
            "expected_slot_utc": expected_slot_utc,
            "fetched_at_utc": now_iso,
            "status": "FAILED",
            "http_status": getattr(getattr(err, "response", None), "status_code", None),
            "record_count": 0,
            "bytes_written": 0,
            "file_path": None,
            "is_backfill": is_backfill,
            "error_type": error_type,
            "error_message": error_msg
        }
        append_ingestion_log(log_entry, log_path=log_path)

        return log_entry


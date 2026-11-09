import json
from pathlib import Path
from typing import Dict, Any

REQUIRED_AIRPORT_FIELDS = {"icao", "name", "latitude", "longitude"}

def load_airports_config(config_path: str = "config/airports.json") -> Dict[str, Dict[str, Any]]:
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Airports configuration file not found: {config_path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    airports_list = data.get("airports", [])
    if not airports_list:
        raise ValueError(f"No airports defined in {config_path}")

    config_by_icao: Dict[str, Dict[str, Any]] = {}
    for item in airports_list:
        missing = REQUIRED_AIRPORT_FIELDS - set(item.keys())
        if missing:
            raise ValueError(f"Missing required field(s) {missing} in airport record: {item}")
        item.setdefault("himawari_segment", "S0410" if float(item["latitude"]) >= 12.0 else "S0510")
        item.setdefault("outer_half_deg", 1.0)
        item.setdefault("inner_half_deg", 0.25)
        icao = item["icao"].upper()
        config_by_icao[icao] = item

    return config_by_icao


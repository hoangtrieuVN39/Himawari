import json
import pytest
from src.ingestion.config_loader import load_airports_config

def test_load_airports_config_success(tmp_path):
    sample_data = {
        "airports": [
            {
                "icao": "VVDN",
                "iata": "DAD",
                "name": "Da Nang International Airport",
                "latitude": 16.0439,
                "longitude": 108.1994
            },
            {
                "icao": "VVNB",
                "iata": "HAN",
                "name": "Noi Bai International Airport",
                "latitude": 21.2212,
                "longitude": 105.8072
            },
            {
                "icao": "VVTS",
                "iata": "SGN",
                "name": "Tan Son Nhat International Airport",
                "latitude": 10.8188,
                "longitude": 106.6520
            }
        ]
    }
    cfg_file = tmp_path / "airports.json"
    cfg_file.write_text(json.dumps(sample_data), encoding="utf-8")

    result = load_airports_config(str(cfg_file))
    assert len(result) == 3
    assert "VVDN" in result
    assert result["VVDN"]["latitude"] == 16.0439
    assert result["VVDN"]["longitude"] == 108.1994

def test_load_airports_config_missing_field(tmp_path):
    invalid_data = {
        "airports": [
            {"icao": "VVDN", "latitude": 16.0439} # Missing longitude
        ]
    }
    cfg_file = tmp_path / "invalid_airports.json"
    cfg_file.write_text(json.dumps(invalid_data), encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required field"):
        load_airports_config(str(cfg_file))

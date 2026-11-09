"""Unit tests for METAR visibility normalization, phenomenon parsing, and B13 cloud classification."""

from src.spark.transforms import parse_metar_raw_ob


def test_parse_vvdn_rain_9999():
    raw_ob = "METAR VVDN 090700Z 08007KT 9999 -RA SCT020 BKN070 27/24 Q1016 NOSIG"
    res = parse_metar_raw_ob(raw_ob)
    assert res["visibility_m"] == 10000
    assert res["visibility_capped_9999"] is True
    assert res["is_low_visibility"] is False
    assert res["has_precip_or_thunder_at_station"] is True
    assert "-RA" in res["station_weather_codes"]
    assert res["has_vicinity_weather"] is False


def test_parse_vvnb_low_visibility_rain():
    raw_ob = "METAR VVNB 090800Z 05008KT 2000 RA BR BKN012 24/23 Q1015"
    res = parse_metar_raw_ob(raw_ob)
    assert res["visibility_m"] == 2000
    assert res["visibility_capped_9999"] is False
    assert res["is_low_visibility"] is True
    assert res["has_precip_or_thunder_at_station"] is True
    assert "RA" in res["station_weather_codes"]


def test_parse_vvts_vicinity_thunderstorm_and_tempo_trend():
    raw_ob = "METAR VVTS 090830Z 05006KT 9999 VCTS FEW025CB SCT060 33/24 Q1009 TEMPO 3000 TSRA"
    res = parse_metar_raw_ob(raw_ob)
    assert res["visibility_m"] == 10000
    assert res["has_precip_or_thunder_at_station"] is False
    assert res["has_vicinity_weather"] is True
    assert "VCTS" in res["vicinity_weather_codes"]
    assert "TSRA" in res["trend_weather_codes"]


def test_parse_speci_cor_heavy_thunderstorm():
    raw_ob = "SPECI VVDN 091505Z COR 11015G25KT 4000 +TSRA BKN015CB 25/24 Q1012"
    res = parse_metar_raw_ob(raw_ob)
    assert res["is_amended_or_speci"] is True
    assert res["visibility_m"] == 4000
    assert res["is_low_visibility"] is True
    assert res["has_precip_or_thunder_at_station"] is True
    assert "+TSRA" in res["station_weather_codes"]


def test_parse_statute_miles_fallback():
    raw_ob = "METAR KORD 091200Z 00000KT CLR 20/15 A2992"
    res = parse_metar_raw_ob(raw_ob, visib_raw="2.5")
    assert res["visibility_m"] == 4023
    assert res["visibility_capped_9999"] is False
    assert res["is_low_visibility"] is True

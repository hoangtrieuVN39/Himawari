"""PSI drift detection."""

from datetime import datetime, timedelta, timezone

import numpy as np

from src.diagnostics.drift import compute_drift, metar_visibility_m, psi

UTC = timezone.utc


def test_psi_is_zero_for_identical_distributions():
    rng = np.random.default_rng(0)
    sample = rng.normal(20, 5, 500).tolist()
    value, bins = psi(sample, list(sample))
    assert value is not None and abs(value) < 1e-9
    assert bins == 10


def test_psi_is_small_for_same_population_and_large_for_shift():
    rng = np.random.default_rng(1)
    ref = rng.normal(0, 1, 2000).tolist()
    same = rng.normal(0, 1, 2000).tolist()
    shifted = rng.normal(2.0, 1, 2000).tolist()
    assert psi(ref, same)[0] < 0.1
    assert psi(ref, shifted)[0] > 0.2


def test_psi_handles_heavy_ties_and_constant_reference():
    ref = [0.0] * 90 + [0.1] * 10
    assert psi(ref, [0.0] * 50 + [0.5] * 50)[0] > 0.2
    value, bins = psi([3.0] * 40, [3.0] * 40)
    assert value == 0.0 and bins == 2
    assert psi([], [1.0])[0] is None


def test_compute_drift_flags_shifted_feature_and_reports_insufficient():
    now = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)
    rng = np.random.default_rng(2)
    feats = {name: [] for name in ("inner_bt_min_c", "inner_cold_cloud_fraction", "visibility_m", "forecast_rain_mm")}
    for i in range(300):  # reference: 72 h before the last 24 h
        feats["inner_bt_min_c"].append((now - timedelta(hours=24 + 72 * i / 300 + 0.1), float(rng.normal(10, 3))))
    for i in range(100):
        feats["inner_bt_min_c"].append((now - timedelta(hours=24 * i / 100), float(rng.normal(-25, 3))))
    rows = {r["feature"]: r for r in compute_drift(feats, now)}
    bt = rows["inner_bt_min_c"]
    assert bt["status"] == "ok" and bt["drift_flag"] is True and bt["psi"] > 0.2
    assert bt["n_reference"] == 300 and bt["n_current"] == 100
    assert rows["visibility_m"]["status"] == "insufficient_data" and rows["visibility_m"]["drift_flag"] is False


def test_metar_visibility_parser():
    assert metar_visibility_m("METAR VVDN 160600Z 13010KT 9999 FEW020 30/25 Q1011 NOSIG") == 9999.0
    assert metar_visibility_m("METAR VVTS 110200Z 05004KT 010V090 CAVOK 30/23 Q1013") == 10000.0
    assert metar_visibility_m("METAR VVNB 160600Z VRB02KT 3000 BR SCT020 25/24 Q1012") == 3000.0
    assert metar_visibility_m(None) is None

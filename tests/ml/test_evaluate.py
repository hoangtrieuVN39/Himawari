"""E7 metric functions on tiny, hand-computable inputs (no Spark)."""

from __future__ import annotations

import math

import pytest

from src.ml import evaluate as ev


def test_contingency_and_scores():
    y = [1, 1, 1, 0, 0, 0, 0, 1]
    p = [1, 1, 0, 1, 0, 0, 0, 0]
    s = ev.binary_scores(y, p)
    assert (s["hits"], s["misses"], s["false_alarms"], s["correct_negatives"]) == (2, 2, 1, 3)
    assert s["pod"] == pytest.approx(0.5)
    assert s["far"] == pytest.approx(1 / 3)
    assert s["csi"] == pytest.approx(2 / 5)
    assert s["frequency_bias"] == pytest.approx(3 / 4)
    assert s["n_events"] == 4 and s["n"] == 8


def test_undefined_scores_are_none_not_zero():
    s = ev.binary_scores([0, 0, 0], [0, 0, 0])
    assert s["pod"] is None and s["far"] is None and s["csi"] is None and s["frequency_bias"] is None
    assert ev.binary_scores([1, 1], [0, 0])["far"] is None
    assert ev.binary_scores([1, 1], [0, 0])["csi"] == 0.0


def test_brier_score():
    assert ev.brier_score([1, 0], [1.0, 0.0]) == 0.0
    assert ev.brier_score([1, 0], [0.8, 0.4]) == pytest.approx((0.04 + 0.16) / 2)
    assert ev.brier_score([], []) is None


def test_wilson_interval_known_values():
    lo, hi = ev.wilson_interval(5, 10)
    assert lo == pytest.approx(0.2366, abs=1e-3) and hi == pytest.approx(0.7634, abs=1e-3)
    lo0, hi0 = ev.wilson_interval(0, 20)
    assert lo0 == 0.0 and hi0 == pytest.approx(0.1611, abs=1e-3)
    assert ev.wilson_interval(0, 0) == (None, None)
    lo1, hi1 = ev.wilson_interval(10, 10)
    assert hi1 == 1.0 and lo1 < 1.0


def test_pr_auc_perfect_inverse_and_ties():
    assert ev.pr_auc([1, 1, 0, 0], [0.9, 0.8, 0.2, 0.1]) == pytest.approx(1.0)
    # positives ranked last: precision at the two positives is 1/3 and 2/4
    assert ev.pr_auc([0, 0, 1, 1], [0.9, 0.8, 0.2, 0.1]) == pytest.approx(0.5 * (1 / 3) + 0.5 * (2 / 4))
    # all scores tied: one threshold, precision = prevalence
    assert ev.pr_auc([1, 0, 0, 1], [0.5] * 4) == pytest.approx(0.5)
    assert ev.pr_auc([0, 0], [0.1, 0.2]) is None


def test_reliability_bins():
    bins = ev.reliability_bins([1, 0, 1, 1], [0.05, 0.05, 0.95, 1.0], n_bins=10)
    assert len(bins) == 10
    assert bins[0]["n"] == 2 and bins[0]["observed_rate"] == 0.5 and bins[0]["mean_predicted"] == pytest.approx(0.05)
    assert bins[9]["n"] == 2 and bins[9]["observed_rate"] == 1.0        # p == 1.0 falls in the last bin
    assert bins[4]["n"] == 0 and bins[4]["observed_rate"] is None


def test_best_csi_threshold_and_fallbacks():
    y = [1, 1, 0, 0, 1, 0]
    p = [0.9, 0.8, 0.7, 0.2, 0.6, 0.1]
    best = ev.best_csi_threshold(y, p)
    assert best["threshold"] == pytest.approx(0.6) and best["csi"] == pytest.approx(3 / 4) and not best["fallback"]
    assert ev.best_csi_threshold([0, 0], [0.1, 0.9])["fallback"] is True
    assert ev.best_csi_threshold([1, 0], [0.1, 0.9])["fallback"] is False  # threshold 0.1 predicts both: CSI 0.5


def test_platt_scaling_recovers_a_monotone_miscalibration():
    # scores are over-confident: true rate at p=0.9 is only ~0.6, at p=0.1 it is ~0.4
    prob = [0.9] * 10 + [0.1] * 10
    y = [1] * 6 + [0] * 4 + [1] * 4 + [0] * 6
    params = ev.fit_platt(prob, y)
    calibrated = ev.apply_platt([0.9, 0.1], params)
    assert calibrated[0] == pytest.approx(0.6, abs=0.02) and calibrated[1] == pytest.approx(0.4, abs=0.02)
    assert ev.fit_platt([0.5, 0.5], [1, 1]) == {"a": 1.0, "b": 0.0, "identity": True}
    assert all(0.0 < c < 1.0 and not math.isnan(c) for c in ev.apply_platt([0.0, 1.0], params))

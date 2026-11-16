"""Pure-Python verification metrics shared by E5 (rule verification) and E7 (nowcast evaluation).

No Spark, no numpy: every function takes plain lists so it can be unit-tested on tiny inputs and
applied to the few thousand rows collected from the driver.

Conventions
-----------
* 2x2 table: hits (a), false alarms (b), misses (c), correct negatives (d).
* POD = a/(a+c), FAR = b/(a+b), CSI = a/(a+b+c), frequency bias = (a+b)/(a+c).
  A metric whose denominator is 0 is `None` (undefined), never 0.
* Brier score, reliability bins and PR-AUC apply to *probabilistic* outputs only; deterministic
  rules/forecasts are scored with the 2x2 metrics.
"""

from __future__ import annotations

import math
from typing import Iterable, Sequence

PROB_EPS = 1e-6


def contingency(y_true: Sequence[int], y_pred: Sequence[int]) -> dict[str, int]:
    """2x2 counts of binary predictions against binary observations."""
    if len(y_true) != len(y_pred):
        raise ValueError("y_true and y_pred must have the same length")
    hits = misses = false_alarms = correct_negatives = 0
    for t, p in zip(y_true, y_pred):
        if t and p:
            hits += 1
        elif t:
            misses += 1
        elif p:
            false_alarms += 1
        else:
            correct_negatives += 1
    return {
        "hits": hits,
        "misses": misses,
        "false_alarms": false_alarms,
        "correct_negatives": correct_negatives,
    }


def _ratio(num: float, den: float) -> float | None:
    return num / den if den else None


def scores_from_counts(hits: int, misses: int, false_alarms: int, correct_negatives: int = 0) -> dict:
    """POD, FAR, CSI, frequency bias (None when undefined) plus the sample sizes."""
    return {
        "pod": _ratio(hits, hits + misses),
        "far": _ratio(false_alarms, hits + false_alarms),
        "csi": _ratio(hits, hits + misses + false_alarms),
        "frequency_bias": _ratio(hits + false_alarms, hits + misses),
        "n_events": hits + misses,
        "n": hits + misses + false_alarms + correct_negatives,
    }


def binary_scores(y_true: Sequence[int], y_pred: Sequence[int]) -> dict:
    """Counts and POD/FAR/CSI/frequency bias of binary predictions."""
    counts = contingency(y_true, y_pred)
    return {**counts, **scores_from_counts(**counts)}


def brier_score(y_true: Sequence[int], prob: Sequence[float]) -> float | None:
    """Mean squared error of probabilities (None for empty input)."""
    if not y_true:
        return None
    return sum((p - t) ** 2 for t, p in zip(y_true, prob)) / len(y_true)


def wilson_interval(successes: int, n: int, z: float = 1.959964) -> tuple[float | None, float | None]:
    """Wilson score interval of a binomial proportion (95 % by default); (None, None) when n == 0."""
    if n <= 0:
        return None, None
    p = successes / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    lo = 0.0 if successes <= 0 else max(0.0, centre - half)
    hi = 1.0 if successes >= n else min(1.0, centre + half)
    return lo, hi


def pr_auc(y_true: Sequence[int], score: Sequence[float]) -> float | None:
    """Average precision (area under the step-wise precision-recall curve).

    Tied scores are treated as one threshold. None when there is no positive example.
    """
    positives = sum(1 for t in y_true if t)
    if positives == 0:
        return None
    order = sorted(range(len(score)), key=lambda i: -score[i])
    tp = fp = 0
    prev_recall = 0.0
    ap = 0.0
    i = 0
    while i < len(order):
        j = i
        while j < len(order) and score[order[j]] == score[order[i]]:
            if y_true[order[j]]:
                tp += 1
            else:
                fp += 1
            j += 1
        recall = tp / positives
        precision = tp / (tp + fp)
        ap += (recall - prev_recall) * precision
        prev_recall = recall
        i = j
    return ap


def reliability_bins(y_true: Sequence[int], prob: Sequence[float], n_bins: int = 10) -> list[dict]:
    """Equal-width reliability table: n, mean predicted probability and observed rate per bin."""
    bins = [{"bin_low": k / n_bins, "bin_high": (k + 1) / n_bins, "n": 0, "_p": 0.0, "_y": 0} for k in range(n_bins)]
    for t, p in zip(y_true, prob):
        k = min(n_bins - 1, max(0, int(p * n_bins)))
        bins[k]["n"] += 1
        bins[k]["_p"] += p
        bins[k]["_y"] += 1 if t else 0
    out = []
    for b in bins:
        n = b["n"]
        out.append(
            {
                "bin_low": b["bin_low"],
                "bin_high": b["bin_high"],
                "n": n,
                "mean_predicted": b["_p"] / n if n else None,
                "observed_rate": b["_y"] / n if n else None,
            }
        )
    return out


def best_csi_threshold(y_true: Sequence[int], prob: Sequence[float]) -> dict:
    """Operating point maximising CSI of `prob >= threshold`.

    Candidates are the distinct predicted probabilities. Ties go to the higher threshold (fewer
    alerts). With no positive example, or when no threshold scores a hit, falls back to 0.5 and
    reports `csi = None` / `fallback = True`.
    """
    if not any(y_true):
        return {"threshold": 0.5, "csi": None, "fallback": True}
    best_thr, best_csi = None, -1.0
    for thr in sorted(set(prob)):
        csi = binary_scores(y_true, [1 if p >= thr else 0 for p in prob])["csi"] or 0.0
        if csi >= best_csi:  # ascending sweep: '>=' keeps the higher threshold on ties
            best_thr, best_csi = thr, csi
    if best_csi <= 0.0:
        return {"threshold": 0.5, "csi": None, "fallback": True}
    return {"threshold": best_thr, "csi": best_csi, "fallback": False}


def _logit(p: float) -> float:
    p = min(1.0 - PROB_EPS, max(PROB_EPS, p))
    return math.log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    e = math.exp(x)
    return e / (1.0 + e)


def fit_platt(prob: Sequence[float], y_true: Sequence[int], l2: float = 1e-3, iters: int = 100) -> dict:
    """Platt scaling: p' = sigmoid(a * logit(p) + b), fitted by Newton-Raphson on the log-loss.

    Needs both classes; otherwise returns the identity map (a=1, b=0) and `identity = True`.
    A small L2 pull towards the identity keeps the fit stable on a few hundred rows.
    """
    pos = sum(1 for t in y_true if t)
    if pos == 0 or pos == len(y_true):
        return {"a": 1.0, "b": 0.0, "identity": True}
    xs = [_logit(p) for p in prob]
    a, b = 1.0, 0.0
    for _ in range(iters):
        g_a = g_b = 0.0
        h_aa = h_ab = h_bb = 0.0
        for x, t in zip(xs, y_true):
            q = _sigmoid(a * x + b)
            r = q - (1 if t else 0)
            w = q * (1 - q)
            g_a += r * x
            g_b += r
            h_aa += w * x * x
            h_ab += w * x
            h_bb += w
        g_a += l2 * (a - 1.0)
        h_aa += l2
        h_bb += 1e-9
        det = h_aa * h_bb - h_ab * h_ab
        if abs(det) < 1e-12:
            break
        da = (h_bb * g_a - h_ab * g_b) / det
        db = (-h_ab * g_a + h_aa * g_b) / det
        a -= da
        b -= db
        if abs(da) + abs(db) < 1e-9:
            break
    return {"a": a, "b": b, "identity": False}


def apply_platt(prob: Iterable[float], params: dict) -> list[float]:
    return [_sigmoid(params["a"] * _logit(p) + params["b"]) for p in prob]

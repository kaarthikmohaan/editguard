"""Recall at a review budget and its paired bootstrap, on cases small enough to check by hand."""

import json
from pathlib import Path

import pytest

np = pytest.importorskip("numpy", reason="needs the evaluation group")

from editguard.evaluation.metrics import Ranking, paired_bootstrap  # noqa: E402
from editguard.evaluation.scoring import rules_scores  # noqa: E402
from editguard.features.features import compute_features  # noqa: E402
from editguard.features.score import rule_score  # noqa: E402
from editguard.producer.parse import to_edit_event  # noqa: E402

ONE_HOUR = np.ones(1)


def ranking(scores, damaging, hour=None) -> Ranking:
    return Ranking(np.array(scores), np.array(damaging), np.zeros(len(scores), dtype=int)
                   if hour is None else np.array(hour))  # fmt: skip


def test_recall_without_ties() -> None:
    # 100 edits scored 100..1; damaging at 100, 50 and 10. 2% of 100 = the top 2 (100, 99).
    scores = list(range(100, 0, -1))
    damaging = [s in (100, 50, 10) for s in scores]
    assert ranking(scores, damaging).recall(ONE_HOUR) == pytest.approx(1 / 3)


def test_edits_tied_at_the_cut_get_fractional_credit() -> None:
    # 10 edits, budget 20% = 2 reviews. One edit scores 9, three tie at 5, six score 1.
    # The 9 takes one review; one review is left for three tied edits: 1/3 each.
    # Damaging: one of the 5s and one of the 1s, so recall = (1/3) / 2.
    scores = [9, 5, 5, 5, 1, 1, 1, 1, 1, 1]
    damaging = [False, True, False, False, True, False, False, False, False, False]
    assert ranking(scores, damaging).recall(ONE_HOUR, budget=0.2) == pytest.approx(1 / 6)


def test_hour_weights_equal_duplicated_rows() -> None:
    scores, damaging, hour = [3, 2, 1, 4], [True, False, True, False], [0, 0, 0, 1]
    weighted = ranking(scores, damaging, hour).recall(np.array([2, 0]), budget=0.5)
    duplicated = ranking([3, 2, 1] * 2, [True, False, True] * 2).recall(ONE_HOUR, budget=0.5)
    assert weighted == pytest.approx(duplicated)


def test_edge_cases() -> None:
    assert ranking([1, 2], [True, False]).recall(ONE_HOUR, budget=1.0) == 1.0
    assert np.isnan(ranking([1, 2], [False, False]).recall(ONE_HOUR))


def test_paired_bootstrap() -> None:
    rng = np.random.default_rng(1)
    n, hours = 5000, 50
    damaging = rng.random(n) < 0.05
    hour = rng.integers(0, hours, n)
    perfect = ranking(damaging.astype(float), damaging, hour)  # flags exactly the bad edits
    noise = ranking(rng.random(n), damaging, hour)

    same = paired_bootstrap(noise, noise, hours, int(damaging.sum()), n, resamples=200)
    assert (same.difference, same.low, same.high) == (0.0, 0.0, 0.0)

    better = paired_bootstrap(perfect, noise, hours, int(damaging.sum()), n, resamples=200)
    assert better.low > 0  # clearly better on every resample
    assert better == paired_bootstrap(perfect, noise, hours, int(damaging.sum()), n, resamples=200)


def test_offline_scores_are_the_live_rule_scores() -> None:
    event = json.loads((Path(__file__).parent / "fixtures/page_change_edit.json").read_text())
    record = to_edit_event(event)
    assert rules_scores([record]) == [rule_score(compute_features(record)).value]

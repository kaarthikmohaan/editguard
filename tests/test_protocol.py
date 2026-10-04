"""The evaluation protocol (ADR 0013) is what the ADR says, and its dates add up."""

from datetime import UTC, datetime, timedelta

from editguard.evaluation import protocol


def test_test_window_is_the_pre_registered_week() -> None:
    start, end = protocol.TEST_WINDOW
    assert (start, end) == (datetime(2026, 10, 5, tzinfo=UTC), datetime(2026, 10, 12, tzinfo=UTC))
    assert end - start == timedelta(days=7)


def test_results_wait_until_every_label_in_the_window_is_final() -> None:
    assert protocol.TEST_WINDOW_FINAL == protocol.TEST_WINDOW[1] + timedelta(hours=48)


def test_budget_and_minimum_follow_the_design() -> None:
    assert protocol.BUDGET == 0.02  # design section 1
    assert protocol.MIN_DAMAGING == 100  # ADR 0006

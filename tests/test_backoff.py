import random

from editguard.producer.backoff import backoff_delay


def test_delay_grows_but_never_exceeds_cap() -> None:
    random.seed(0)
    for attempt in range(20):
        delay = backoff_delay(attempt, base_s=1.0, cap_s=60.0)
        assert 0 <= delay <= min(60.0, 2**attempt)


def test_first_retry_is_quick() -> None:
    random.seed(0)
    assert all(backoff_delay(0) <= 1.0 for _ in range(100))

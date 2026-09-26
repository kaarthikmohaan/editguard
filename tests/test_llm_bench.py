import pytest

from editguard.tools.llm_bench import required_calls_per_second


def test_required_rate_is_one_and_a_half_times_two_percent() -> None:
    # 3,600 edits/h -> 72 flags/h at 2% -> 108/h with 1.5x headroom -> 0.03 per second
    assert required_calls_per_second(3600) == pytest.approx(0.03)

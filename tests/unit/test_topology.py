import pytest

from payments.messaging.topology import compute_retry_delays


def test_single_retry_stage() -> None:
    """2 попытки → одна retry-стадия с базовой задержкой."""
    assert compute_retry_delays(max_attempts=2, base_ms=5000, multiplier=2.0, cap_ms=300_000) == [5000]


def test_three_attempts_exponential() -> None:
    """3 попытки → две стадии, экспоненциальный рост."""
    assert compute_retry_delays(max_attempts=3, base_ms=5000, multiplier=2.0, cap_ms=300_000) == [5000, 10000]


def test_four_attempts_exponential() -> None:
    """4 попытки → три стадии: 5с, 10с, 20с."""
    assert compute_retry_delays(max_attempts=4, base_ms=5000, multiplier=2.0, cap_ms=300_000) == [5000, 10000, 20000]


def test_five_attempts_exponential() -> None:
    assert compute_retry_delays(max_attempts=5, base_ms=5000, multiplier=2.0, cap_ms=300_000) == [
        5000,
        10000,
        20000,
        40000,
    ]


def test_cap_limits_growth() -> None:
    """Потолок cap_ms ограничивает задержки."""
    assert compute_retry_delays(max_attempts=6, base_ms=5000, multiplier=2.0, cap_ms=30000) == [
        5000,
        10000,
        20000,
        30000,
        30000,
    ]


def test_multiplier_one_is_linear() -> None:
    """Multiplier=1.0 даёт линейный backoff (все задержки равны base)."""
    assert compute_retry_delays(max_attempts=4, base_ms=5000, multiplier=1.0, cap_ms=300_000) == [5000, 5000, 5000]


def test_max_attempts_below_two_returns_empty() -> None:
    """max_attempts=1 не имеет retry-стадий."""
    assert compute_retry_delays(max_attempts=1, base_ms=5000, multiplier=2.0, cap_ms=300_000) == []


@pytest.mark.parametrize(
    "max_attempts, expected_count",
    [(2, 1), (3, 2), (4, 3), (5, 4), (10, 9)],
)
def test_stage_count_matches_max_attempts(max_attempts: int, expected_count: int) -> None:
    """Количество стадий = max_attempts - 1, независимо от множителя."""
    delays = compute_retry_delays(max_attempts=max_attempts, base_ms=1000, multiplier=3.0, cap_ms=10_000_000)
    assert len(delays) == expected_count

"""_rate_limiter: maps an activities-per-second rate to a DBOS queue limiter."""

import pytest

from temporal_dbos.worker import _rate_limiter


def test_none_when_unset() -> None:
    assert _rate_limiter(None) is None


def test_integer_rate_per_second() -> None:
    assert _rate_limiter(100) == {"limit": 100, "period": 1.0}
    assert _rate_limiter(10.0) == {"limit": 10, "period": 1.0}


def test_sub_one_rate_is_exact() -> None:
    # 0.5/s → one start per 2 seconds.
    assert _rate_limiter(0.5) == {"limit": 1, "period": 2.0}


def test_nonpositive_rejected() -> None:
    with pytest.raises(ValueError):
        _rate_limiter(0)
    with pytest.raises(ValueError):
        _rate_limiter(-5)

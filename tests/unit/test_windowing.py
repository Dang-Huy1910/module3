import pytest

from src.analytics.windowing import window_start_utc


def test_windowing_edge_cases():
    # 10:00:59Z falls into 10:00:00Z
    assert window_start_utc("2026-11-02T10:00:59Z") == "2026-11-02T10:00:00Z"

    # 10:01:00Z starts next window
    assert window_start_utc("2026-11-02T10:01:00Z") == "2026-11-02T10:01:00Z"

    # Timezone offset: 17:00:05+07:00 is 10:00:05 UTC -> floors to 10:00:00Z
    assert window_start_utc("2026-11-02T17:00:05+07:00") == "2026-11-02T10:00:00Z"


def test_windowing_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="naive"):
        window_start_utc("2026-11-02T10:00:00")  # No Z or offset

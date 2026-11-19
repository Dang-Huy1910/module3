from datetime import datetime, timezone
from typing import Union


def window_start_utc(event_time: Union[str, datetime]) -> str:
    """Converts a timezone-aware ISO-8601 timestamp to UTC and floors it to a 1-minute window.

    Output format: YYYY-MM-DDTHH:MM:00Z

    Raises:
        ValueError: If event_time is naive (lacks timezone) or malformed.
    """
    # TODO: Học viên hoàn thiện logic cắt cửa sổ thời gian 1 phút chuẩn UTC:
    # 1. Parse chuỗi ISO-8601 sang datetime
    # 2. Bắt buộc kiểm tra múi giờ (từ chối timezone-naive)
    # 3. Chuyển sang UTC và làm tròn giây/microgiây về 0
    # 4. Trả về định dạng %Y-%m-%dT%H:%M:00Z
    raise NotImplementedError("Học viên cần triển khai hàm window_start_utc")

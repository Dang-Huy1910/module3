from datetime import datetime, timezone
from typing import Optional


def build_s3_key(
    topic: str,
    partition: int,
    offset: int,
    event_time: Optional[str] = None,
) -> str:
    """Builds deterministic S3 object key according to data contract:

    raw/topic=<topic>/dt=<UTC-event-date>/partition=<partition>/offset=<zero-padded-offset>.json
    If event_time is missing or invalid, stores under unknown/ prefix.
    """
    # TODO: Học viên hoàn thiện logic tạo key S3 xác định theo quy ước:
    # 1. offset zero-padded 20 chữ số (f"{offset:020d}")
    # 2. Parse event_time sang UTC date (YYYY-MM-DD)
    # 3. Trả về đúng đường dẫn phân cấp raw/topic=.../dt=.../partition=.../offset=...
    raise NotImplementedError("Học viên cần triển khai hàm build_s3_key")

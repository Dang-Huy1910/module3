from src.archiver.s3_keys import build_s3_key


def test_build_s3_key_valid():
    key = build_s3_key(
        topic="lab6-order-events",
        partition=0,
        offset=5,
        event_time="2026-11-02T10:00:05Z",
    )
    assert key == "raw/topic=lab6-order-events/dt=2026-11-02/partition=0/offset=00000000000000000005.json"


def test_build_s3_key_invalid_time():
    key = build_s3_key(
        topic="lab6-order-events",
        partition=1,
        offset=12,
        event_time="not-a-valid-time",
    )
    assert key == "raw/invalid/topic=lab6-order-events/partition=1/offset=00000000000000000012.json"

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_time_utc TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS minute_metrics (
    window_start_utc TEXT PRIMARY KEY,
    order_count INTEGER NOT NULL DEFAULT 0,
    created_order_value_vnd INTEGER NOT NULL DEFAULT 0,
    product_view_count INTEGER NOT NULL DEFAULT 0,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_minute_activity (
    window_start_utc TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    first_seen_utc TEXT NOT NULL,
    PRIMARY KEY (window_start_utc, customer_id)
);

CREATE TABLE IF NOT EXISTS rejected_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL,
    kafka_key TEXT,
    reason TEXT NOT NULL,
    raw_payload_base64 TEXT,
    rejected_at_utc TEXT NOT NULL,
    UNIQUE (topic, kafka_partition, kafka_offset)
);

CREATE VIEW IF NOT EXISTS dashboard_minute AS
SELECT
    m.window_start_utc,
    m.order_count,
    m.created_order_value_vnd,
    m.product_view_count,
    COALESCE(c.active_customers, 0) AS active_customers,
    m.updated_at_utc
FROM minute_metrics m
LEFT JOIN (
    SELECT window_start_utc, COUNT(DISTINCT customer_id) AS active_customers
    FROM customer_minute_activity
    GROUP BY window_start_utc
) c ON m.window_start_utc = c.window_start_utc
ORDER BY m.window_start_utc DESC;
"""


class AnalyticsStore:
    """Manages transactional SQLite storage for streaming analytics."""

    def __init__(self, db_path: str = "data/realtime_analytics.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        with self.get_connection() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.commit()

    def record_rejected(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: Optional[str],
        reason: str,
        raw_payload_b64: Optional[str],
    ) -> bool:
        now_utc = datetime.now(timezone.utc).isoformat()
        sql = """
        INSERT OR IGNORE INTO rejected_events (
            topic, kafka_partition, kafka_offset, kafka_key, reason, raw_payload_base64, rejected_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        with self.get_connection() as conn:
            conn.execute(sql, (topic, partition, offset, key, reason, raw_payload_b64, now_utc))
            conn.commit()
        return True

    def process_event_transactional(
        self,
        event: Dict[str, Any],
        window_utc: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        """Processes a single event transactionally with idempotency.

        Returns:
            True if event was newly applied.
            False if event was a duplicate (metrics unchanged).
        """
        # TODO: Học viên hoàn thiện logic giao dịch SQLite:
        # 1. Bắt đầu transaction (BEGIN IMMEDIATE)
        # 2. Deduplication: Kiểm tra xem event_id đã tồn tại trong processed_events chưa
        # 3. Ghi vào processed_events
        # 4. Ghi nhận customer_id vào customer_minute_activity
        # 5. UPSERT chỉ số vào minute_metrics (ON CONFLICT(window_start_utc) DO UPDATE)
        # 6. Commit transaction
        raise NotImplementedError("Học viên cần triển khai hàm process_event_transactional")

    def get_dashboard_metrics(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM dashboard_minute ORDER BY window_start_utc ASC;")
            return [dict(row) for row in cursor.fetchall()]

import argparse
import base64
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.analytics.analytics_store import AnalyticsStore
from src.analytics.windowing import window_start_utc
from src.common.metrics import (
    EVENTS_TOTAL,
    LAST_SUCCESS_UNIXTIME,
    PROCESSING_SECONDS,
    start_metrics_server,
)
from src.common.serialization import deserialize_event
from src.common.validation import (
    ValidationError,
    validate_order_created,
    validate_product_viewed,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("analytics_consumer")


class AnalyticsConsumer:
    """Consumes multiple Kafka topics (order-events & customer-activity-events),

    applies windowed aggregations into SQLite transactionally, and exposes Prometheus metrics.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        order_topic: str,
        activity_topic: str,
        db_path: str,
        metrics_port: int = 9108,
        crash_after_sqlite_event_id: Optional[str] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.order_topic = order_topic
        self.activity_topic = activity_topic
        self.store = AnalyticsStore(db_path=db_path)
        self.crash_after_sqlite_event_id = crash_after_sqlite_event_id

        # Start internal Prometheus scraper server
        start_metrics_server(port=metrics_port)
        logger.info("Prometheus metrics server started on port %d", metrics_port)

        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
        self.consumer = Consumer(self.conf)
        self.running = True

    def process_message(self, msg: Any) -> bool:
        start_t = time.perf_counter()
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        raw_key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
        raw_value = msg.value()

        event_payload = None
        window_utc = None
        quarantine_reason = None

        try:
            event_payload = deserialize_event(raw_value)
            event_type = event_payload.get("event_type")

            if topic == self.order_topic and event_type == "OrderCreated":
                validate_order_created(event_payload)
            elif topic == self.activity_topic and event_type == "ProductViewed":
                validate_product_viewed(event_payload)
            else:
                raise ValidationError(f"Unknown topic/event_type: {topic} / {event_type}")

            window_utc = window_start_utc(event_payload["event_time"])

        except (ValueError, ValidationError) as err:
            quarantine_reason = str(err)
            EVENTS_TOTAL.labels(service="analytics", result="rejected").inc()
            logger.warning("Validation failed at [%s:%d@%d]. Reason: %s", topic, partition, offset, quarantine_reason)

        except Exception as err:
            EVENTS_TOTAL.labels(service="analytics", result="error").inc()
            logger.critical("Unexpected decoding error at [%s:%d@%d]: %s. Offset UNCOMMITTED.", topic, partition, offset, err)
            return False

        # SQLite Transaction
        if quarantine_reason:
            b64_raw = base64.b64encode(raw_value).decode("ascii") if raw_value else None
            self.store.record_rejected(
                topic=topic,
                partition=partition,
                offset=offset,
                key=raw_key,
                reason=quarantine_reason,
                raw_payload_b64=b64_raw,
            )
        else:
            try:
                applied = self.store.process_event_transactional(
                    event=event_payload,
                    window_utc=window_utc,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                )
                if applied:
                    EVENTS_TOTAL.labels(service="analytics", result="accepted").inc()
                    LAST_SUCCESS_UNIXTIME.labels(service="analytics").set(time.time())
                else:
                    EVENTS_TOTAL.labels(service="analytics", result="duplicate").inc()
            except Exception as err:
                EVENTS_TOTAL.labels(service="analytics", result="error").inc()
                logger.error("DB transaction failure: %s. Offset will NOT be committed.", err)
                return False

            # Test crash hook
            event_id = event_payload.get("event_id")
            if self.crash_after_sqlite_event_id and self.crash_after_sqlite_event_id == event_id:
                logger.critical("*** TEST HOOK: Hard crashing before Kafka commit for %s ***", event_id)
                os._exit(43)

        # Synchronously commit offset
        try:
            self.consumer.commit(message=msg, asynchronous=False)
            elapsed = time.perf_counter() - start_t
            PROCESSING_SECONDS.labels(service="analytics").observe(elapsed)
            return True
        except KafkaException as commit_err:
            EVENTS_TOTAL.labels(service="analytics", result="error").inc()
            logger.error("Failed to commit offset: %s", commit_err)
            return False

    def start(self, max_records: Optional[int] = None, idle_timeout: float = 15.0) -> None:
        topics = [self.order_topic, self.activity_topic]
        self.consumer.subscribe(topics)
        logger.info("Analytics consumer subscribed to %s (group '%s'). Listening...", topics, self.group_id)

        records_processed = 0
        last_msg_time = time.time()

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    if max_records and (time.time() - last_msg_time > idle_timeout):
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                last_msg_time = time.time()
                if not self.process_message(msg):
                    raise RuntimeError(
                        f"Failed to process {msg.topic()}[{msg.partition()}]@{msg.offset()}; "
                        "stopping before a later commit can skip this offset"
                    )
                records_processed += 1

                if max_records and records_processed >= max_records:
                    break

        except KeyboardInterrupt:
            logger.info("Stopping analytics consumer...")
        finally:
            self.consumer.close()
            logger.info("Analytics consumer closed.")


def main():
    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    default_order_topic = os.getenv("KAFKA_ORDER_TOPIC", "order-events")
    default_activity_topic = os.getenv("KAFKA_ACTIVITY_TOPIC", "customer-activity-events")
    default_group = os.getenv("KAFKA_GROUP_ID", "realtime-analytics-v1")
    default_db = os.getenv("SQLITE_PATH", "data/realtime_analytics.db")
    default_port = int(os.getenv("METRICS_PORT", "9108"))

    parser = argparse.ArgumentParser(description="Multi-topic realtime analytics consumer with Prometheus metrics.")
    parser.add_argument("--bootstrap", type=str, default=default_bootstrap)
    parser.add_argument("--order-topic", type=str, default=default_order_topic)
    parser.add_argument("--activity-topic", type=str, default=default_activity_topic)
    parser.add_argument("--group", type=str, default=default_group)
    parser.add_argument("--db", type=str, default=default_db)
    parser.add_argument("--metrics-port", type=int, default=default_port)
    parser.add_argument("--crash-after-sqlite-event-id", type=str, default=None)
    parser.add_argument("--max-records", type=int, default=None)

    args = parser.parse_args()

    consumer = AnalyticsConsumer(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        order_topic=args.order_topic,
        activity_topic=args.activity_topic,
        db_path=args.db,
        metrics_port=args.metrics_port,
        crash_after_sqlite_event_id=args.crash_after_sqlite_event_id,
    )
    consumer.start(max_records=args.max_records)


if __name__ == "__main__":
    main()

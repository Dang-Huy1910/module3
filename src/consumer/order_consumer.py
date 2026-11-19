import argparse
import base64
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.common.serialization import deserialize_event
from src.common.validation import ValidationError, validate_order_created

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("order_consumer")


class OrderConsumer:
    """Kafka Consumer implementing strict manual commit and quarantine envelope routing."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str = "order-events",
        output_dir: str = "data",
        crash_after_write_event_id: Optional[str] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.output_dir = Path(output_dir)
        self.crash_after_write_event_id = crash_after_write_event_id

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.processed_path = self.output_dir / "processed.jsonl"
        self.quarantine_path = self.output_dir / "quarantine.jsonl"

        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.consumer = Consumer(self.conf)
        self.running = True

    def _append_to_file(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Appends a single JSON envelope to JSONL and calls fsync for disk durability."""
        line = json.dumps(data, ensure_ascii=False) + "\n"
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def process_message(self, msg: Any) -> bool:
        """Processes a single Kafka message.

        Returns True if offset should be committed, False if infrastructure error.
        """
        # TODO: Học viên hoàn thiện logic xử lý tin nhắn:
        # 1. Giải mã payload và validate (validate_order_created)
        # 2. Nếu hợp lệ: ghi vào processed.jsonl -> fsync
        # 3. Nếu sai schema: ghi vào quarantine.jsonl (bọc base64) -> commit offset
        # 4. Gọi commit offset đồng bộ: self.consumer.commit(message=msg, asynchronous=False)
        raise NotImplementedError("Học viên cần triển khai hàm process_message")

    def start(self, max_records: Optional[int] = None, idle_timeout: float = 15.0) -> None:
        self.consumer.subscribe([self.topic])
        logger.info("Consumer group '%s' subscribed to topic '%s'. Listening...", self.group_id, self.topic)
        # TODO: Học viên hoàn thiện vòng lặp poll() và gọi process_message().
        # Nếu process_message() trả False, dừng consumer trước khi commit offset tiếp theo.
        raise NotImplementedError("Học viên cần triển khai vòng lặp start()")


def main():
    parser = argparse.ArgumentParser(description="Consume OrderCreated events and append to JSONL.")
    parser.add_argument("--group", type=str, required=True)
    parser.add_argument("--output-dir", type=str, default="data")
    parser.add_argument("--topic", type=str, default="order-events")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--crash-after-write-event-id", type=str, default=None)
    parser.add_argument("--max-records", type=int, default=None)

    args = parser.parse_args()
    consumer = OrderConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group,
        topic=args.topic,
        output_dir=args.output_dir,
        crash_after_write_event_id=args.crash_after_write_event_id,
    )
    consumer.start(max_records=args.max_records)


if __name__ == "__main__":
    main()

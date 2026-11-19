import argparse
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any, Dict

from confluent_kafka import Producer

from src.common.serialization import serialize_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fixture_publisher")


def main():
    parser = argparse.ArgumentParser(description="Publish JSONL test fixtures to Kafka topics.")
    parser.add_argument("--fixture", type=str, required=True, help="Path to JSONL fixture file")
    parser.add_argument("--bootstrap", type=str, default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--order-topic", type=str, default="lab3-order-events", help="Remapped order topic name")
    parser.add_argument("--activity-topic", type=str, default="lab3-activity-events", help="Remapped activity topic name")
    args = parser.parse_args()

    fixture_path = Path(args.fixture)
    if not fixture_path.exists():
        logger.error("Fixture file not found: %s", fixture_path)
        sys.exit(1)

    producer_conf = {
        "bootstrap.servers": args.bootstrap,
        "client.id": "fixture-publisher-client",
        "acks": "all",
    }
    producer = Producer(producer_conf)

    delivered = 0
    failed = 0

    def delivery_report(err: Any, msg: Any) -> None:
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            logger.error("Delivery failed for key %s: %s", msg.key(), err)
        else:
            delivered += 1
            logger.info("Delivered to %s [%d] @ %d (Key=%s)", msg.topic(), msg.partition(), msg.offset(), msg.key())

    lines = fixture_path.read_text(encoding="utf-8").strip().splitlines()
    logger.info("Publishing %d records from %s ...", len(lines), fixture_path.name)

    for line_num, line in enumerate(lines, 1):
        if not line.strip():
            continue

        data = json.loads(line)
        target_topic = data.get("topic")
        key = data.get("key")
        value_obj = data.get("value")

        # Remap topic names if specified
        if target_topic == "order-events":
            actual_topic = args.order_topic
        elif target_topic == "customer-activity-events":
            actual_topic = args.activity_topic
        else:
            actual_topic = target_topic

        # Note: Must NOT validate invalid fixture before publish (to test quarantine)
        serialized_val = serialize_event(value_obj) if isinstance(value_obj, dict) else str(value_obj).encode("utf-8")
        serialized_key = key.encode("utf-8") if isinstance(key, str) else None

        producer.produce(
            topic=actual_topic,
            key=serialized_key,
            value=serialized_val,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    logger.info("Flushing publisher queue...")
    unflushed = producer.flush(10.0)
    failed += unflushed

    logger.info("--- PUBLISH SUMMARY --- Delivered: %d | Failed: %d", delivered, failed)
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

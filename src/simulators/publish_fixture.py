import argparse
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Set

from confluent_kafka import Producer
from src.common.serialization import serialize_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("publish_fixture")


def main():
    parser = argparse.ArgumentParser(description="Publish JSONL test fixture for Module 3 Buổi 5.")
    parser.add_argument("--file", type=str, default="fixtures/module6_buoi2_events.jsonl", help="Fixture path")
    parser.add_argument("--bootstrap", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka broker")
    parser.add_argument("--republish", action="store_true", help="Deliberately allow duplicate business events")

    args = parser.parse_args()

    fixture_path = Path(args.file)
    if not fixture_path.exists():
        logger.error("Fixture file not found: %s", fixture_path)
        sys.exit(1)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "fixture-publisher",
        "acks": "all",
    })

    lines = fixture_path.read_text(encoding="utf-8").strip().splitlines()
    seen_event_ids: Set[str] = set()
    delivered = 0
    failed = 0

    def cb(err: Any, msg: Any):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            logger.error("Delivery failed: %s", err)
        else:
            delivered += 1
            logger.info("Published to %s [%d] @ %d (Key=%s)", msg.topic(), msg.partition(), msg.offset(), msg.key())

    logger.info("Publishing %d records from %s (republish=%s)...", len(lines), fixture_path.name, args.republish)

    for line in lines:
        if not line.strip():
            continue
        env = json.loads(line)
        topic = env["topic"]
        key = env["key"]
        val = env["value"]

        event_id = val.get("event_id")
        if not args.republish and event_id in seen_event_ids:
            logger.warning("Skipping in-batch duplicate event_id %s (use --republish to bypass)", event_id)
            continue
        seen_event_ids.add(event_id)

        val_bytes = serialize_event(val)
        producer.produce(
            topic=topic,
            key=key.encode("utf-8") if isinstance(key, str) else None,
            value=val_bytes,
            on_delivery=cb,
        )
        producer.poll(0)

    unflushed = producer.flush(10.0)
    failed += unflushed
    logger.info("--- PUBLISH SUMMARY --- Delivered: %d | Failed: %d", delivered, failed)
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

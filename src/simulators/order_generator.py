import argparse
from datetime import datetime, timezone
import logging
import os
import random
import sys
import time
from typing import Any, Dict, List
import uuid

from src.common.serialization import serialize_event
from src.common.validation import validate_order_created
from src.producer.order_producer import OrderProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("order_generator")

PRODUCT_CATALOG = [
    {"product_id": "P-101", "name": "Tai nghe không dây", "unit_price": 150000},
    {"product_id": "P-102", "name": "Bàn phím cơ TKL", "unit_price": 450000},
    {"product_id": "P-103", "name": "Chuột Gaming RGB", "unit_price": 250000},
    {"product_id": "P-104", "name": "Cáp sạc Type-C 65W", "unit_price": 85000},
    {"product_id": "P-105", "name": "Lót chuột cỡ lớn", "unit_price": 60000},
]


def generate_single_order(run_id: str, sequence_num: int, rng: random.Random) -> Dict[str, Any]:
    """Generates a single valid OrderCreated event with deterministic run-id scoping."""
    order_id = f"ORD-{run_id}-{sequence_num:04d}"
    customer_id = f"CUS-{(sequence_num % 10) + 1:03d}"  # 10 customers repeat to test partition affinity
    event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order_id}-{sequence_num}"))

    # Random 1-3 items
    num_items = rng.randint(1, 3)
    chosen_products = rng.sample(PRODUCT_CATALOG, num_items)

    items: List[Dict[str, Any]] = []
    total_amount = 0

    for prod in chosen_products:
        quantity = rng.randint(1, 4)
        price = prod["unit_price"]
        items.append({
            "product_id": prod["product_id"],
            "quantity": quantity,
            "unit_price": price,
        })
        total_amount += quantity * price

    event = {
        "event_id": event_id,
        "event_type": "OrderCreated",
        "event_version": 1,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "order-simulator",
        "order_id": order_id,
        "customer_id": customer_id,
        "items": items,
        "total_amount": total_amount,
        "currency": "VND",
    }
    return event


def main():
    parser = argparse.ArgumentParser(description="Simulate and publish OrderCreated events to Kafka.")
    parser.add_argument("--count", type=int, required=True, help="Total number of events to generate")
    parser.add_argument("--rate", type=float, required=True, help="Target events per second")
    parser.add_argument("--seed", type=int, required=True, help="Random seed for repeatable data")
    parser.add_argument("--run-id", type=str, required=True, help="Unique identifier for this batch run")
    parser.add_argument("--topic", type=str, default="order-events", help="Target Kafka topic")
    parser.add_argument("--bootstrap-servers", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Kafka broker address")

    args = parser.parse_args()

    rng = random.Random(args.seed)
    delay_between_events = 1.0 / args.rate if args.rate > 0 else 0.0

    logger.info("Initializing OrderProducer with servers: %s", args.bootstrap_servers)
    producer = OrderProducer(bootstrap_servers=args.bootstrap_servers, client_id=f"sim-{args.run_id}")

    logger.info(
        "Starting simulation: count=%d, rate=%.1f msg/s, seed=%d, run_id=%s, topic=%s",
        args.count,
        args.rate,
        args.seed,
        args.run_id,
        args.topic,
    )

    generated_count = 0
    start_time = time.time()

    for seq in range(1, args.count + 1):
        event = generate_single_order(run_id=args.run_id, sequence_num=seq, rng=rng)

        # Validate before producing
        validate_order_created(event)
        serialized_bytes = serialize_event(event)

        # Key is order_id as specified in contract
        producer.produce(
            topic=args.topic,
            key=event["order_id"],
            value=serialized_bytes,
        )
        generated_count += 1

        if delay_between_events > 0 and seq < args.count:
            time.sleep(delay_between_events)

    logger.info("Flushing producer buffers...")
    remaining = producer.flush(timeout=15.0)
    elapsed = time.time() - start_time

    stats = producer.stats()
    delivered = stats["delivered"]
    failed = stats["failed"] + remaining

    logger.info(
        "--- SIMULATION SUMMARY ---"
        "\nRun ID: %s"
        "\nGenerated: %d"
        "\nDelivered: %d"
        "\nFailed: %d"
        "\nElapsed Time: %.2f seconds (%.2f msg/sec)",
        args.run_id,
        generated_count,
        delivered,
        failed,
        elapsed,
        delivered / elapsed if elapsed > 0 else 0,
    )

    if failed > 0:
        logger.error("Simulation finished with %d failed deliveries.", failed)
        sys.exit(1)
    else:
        logger.info("Simulation completed successfully without errors.")
        sys.exit(0)


if __name__ == "__main__":
    main()

import argparse
import logging
import os
import time
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lag_exporter")

KAFKA_LAG_GAUGE = Gauge(
    "bk_kafka_consumer_lag_records",
    "Kafka consumer lag in records by group, topic, and partition",
    ["group", "topic", "partition"],
)


def compute_lag(bootstrap_servers: str, groups: list[str], topics: list[str]) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "lag-exporter-internal-group",
        "enable.auto.commit": False,
    })

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    for group in groups:
        for topic in topics:
            try:
                metadata = consumer.list_topics(topic, timeout=5.0)
                if topic not in metadata.topics:
                    continue
                partitions = metadata.topics[topic].partitions.keys()

                for p in partitions:
                    tp = TopicPartition(topic, p)
                    low, high = consumer.get_watermark_offsets(tp, timeout=5.0)

                    # Get committed offset for group
                    temp_consumer = Consumer({
                        "bootstrap.servers": bootstrap_servers,
                        "group.id": group,
                        "enable.auto.commit": False,
                    })
                    committed = temp_consumer.committed([tp], timeout=5.0)
                    temp_consumer.close()

                    committed_offset = committed[0].offset if committed and committed[0].offset >= 0 else low
                    lag = max(0, high - committed_offset)

                    KAFKA_LAG_GAUGE.labels(group=group, topic=topic, partition=str(p)).set(lag)
                    logger.debug("Group=%s [%s:%d] High=%d Committed=%d Lag=%d", group, topic, p, high, committed_offset, lag)

            except Exception as err:
                logger.error("Error computing lag for %s / %s: %s", group, topic, err)

    consumer.close()


def main():
    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    default_port = int(os.getenv("LAG_EXPORTER_PORT", "9110"))

    parser = argparse.ArgumentParser(description="Prometheus Kafka Consumer Lag Exporter.")
    parser.add_argument("--bootstrap", type=str, default=default_bootstrap)
    parser.add_argument("--groups", type=str, default="realtime-analytics-v1")
    parser.add_argument("--topics", type=str, default="order-events,customer-activity-events")
    parser.add_argument("--port", type=int, default=default_port)
    parser.add_argument("--interval", type=int, default=5)

    args = parser.parse_args()
    group_list = [g.strip() for g in args.groups.split(",") if g.strip()]
    topic_list = [t.strip() for t in args.topics.split(",") if t.strip()]

    start_http_server(args.port)
    logger.info("Kafka Lag Exporter started on port %d. Monitoring groups: %s", args.port, group_list)

    while True:
        compute_lag(args.bootstrap, group_list, topic_list)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

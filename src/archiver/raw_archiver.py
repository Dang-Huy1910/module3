import argparse
import logging
import os
import time
from typing import Any, List, Optional
import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.archiver.s3_keys import build_s3_key
from src.common.serialization import deserialize_event
from src.common.metrics import (
    EVENTS_TOTAL,
    LAST_SUCCESS_UNIXTIME,
    PROCESSING_SECONDS,
    start_metrics_server,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("raw_archiver")


class RawArchiver:
    """Consumes Kafka topics and archives raw JSON payloads to S3-compatible storage.

    Strict semantics: PUT object to S3 must succeed BEFORE committing Kafka offset.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: List[str],
        s3_endpoint: str,
        bucket_name: str,
        aws_access_key: str = "anykey",
        aws_secret_key: str = "anysecret",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.bucket_name = bucket_name

        # Initialize Boto3 S3 Client (Path-style addressing for SeaweedFS/MinIO)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

        # Kafka Consumer
        self.conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
        }
        self.consumer = Consumer(self.conf)
        self.running = True

    def _ensure_bucket(self) -> None:
        """Idempotently creates the target S3 bucket if not exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception:
            logger.info("Creating S3 bucket: %s", self.bucket_name)
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            except Exception as err:
                logger.warning("Bucket creation warning (might already exist): %s", err)

    def process_message(self, msg: Any) -> bool:
        started_at = time.perf_counter()
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        raw_key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
        raw_bytes = msg.value()

        if raw_bytes is None:
            # Tombstone message - commit offset and skip
            self.consumer.commit(message=msg, asynchronous=False)
            self._record_metric("accepted", started_at)
            return True

        # Extract event_time for deterministic S3 partition key
        event_time = None
        event_id = None
        try:
            event_dict = deserialize_event(raw_bytes)
            event_time = event_dict.get("event_time")
            event_id = event_dict.get("event_id")
        except Exception:
            logger.warning("Could not extract event_time from [%s:%d@%d]. Archiving to invalid prefix.", topic, partition, offset)

        s3_key = build_s3_key(topic=topic, partition=partition, offset=offset, event_time=event_time)

        # S3 Metadata preserves Kafka provenance
        metadata = {
            "kafka-topic": str(topic),
            "kafka-partition": str(partition),
            "kafka-offset": str(offset),
        }
        if raw_key:
            metadata["kafka-key"] = raw_key
        if event_id:
            metadata["event-id"] = str(event_id)

        # Step 1: PUT object to S3 (Must succeed before commit)
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=raw_bytes,
                ContentType="application/json",
                Metadata=metadata,
            )
            logger.info(
                "[S3_PERSISTED] Key=%s (Topic=%s, Partition=%d, Offset=%d, EventID=%s)",
                s3_key,
                topic,
                partition,
                offset,
                event_id,
            )
        except Exception as upload_err:
            logger.error("Failed to upload object to S3 for [%s:%d@%d]: %s. Offset UNCOMMITTED.", topic, partition, offset, upload_err)
            self._record_metric("error", started_at)
            return False

        # Step 2: Synchronously commit Kafka offset
        try:
            self.consumer.commit(message=msg, asynchronous=False)
            logger.debug("[OFFSET_COMMITTED] Offset %d on partition %d", offset, partition)
            self._record_metric("accepted", started_at)
            return True
        except KafkaException as commit_err:
            logger.error("Failed to commit offset: %s", commit_err)
            self._record_metric("error", started_at)
            return False

    @staticmethod
    def _record_metric(result: str, started_at: float) -> None:
        """Record one terminal processing outcome for Prometheus."""
        EVENTS_TOTAL.labels(service="archiver", result=result).inc()
        PROCESSING_SECONDS.labels(service="archiver").observe(time.perf_counter() - started_at)
        if result == "accepted":
            LAST_SUCCESS_UNIXTIME.labels(service="archiver").set(time.time())

    def start(self, max_records: Optional[int] = None, idle_timeout: float = 15.0) -> None:
        self.consumer.subscribe(self.topics)
        logger.info("Archiver subscribed to topics: %s with group '%s'. Listening...", self.topics, self.group_id)

        records_processed = 0
        last_msg_time = time.time()

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    if max_records and (time.time() - last_msg_time > idle_timeout):
                        logger.info("Idle timeout reached (%s s). Exiting.", idle_timeout)
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
                        f"Failed to archive {msg.topic()}[{msg.partition()}]@{msg.offset()}; "
                        "stopping before a later commit can skip this offset"
                    )
                records_processed += 1

                if max_records and records_processed >= max_records:
                    logger.info("Archived target count (%d records). Stopping.", max_records)
                    break

        except KeyboardInterrupt:
            logger.info("Shutting down archiver...")
        finally:
            self.consumer.close()
            logger.info("Archiver closed. Total processed: %d", records_processed)


def main():
    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    default_s3 = os.getenv("S3_ENDPOINT_URL", "http://localhost:8333")
    default_bucket = os.getenv("S3_BUCKET_NAME", "ecommerce-raw")
    default_access = os.getenv("AWS_ACCESS_KEY_ID", "anykey")
    default_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "anysecret")
    metrics_port = int(os.getenv("METRICS_PORT", "9109"))

    parser = argparse.ArgumentParser(description="Kafka to S3 Raw Event Archiver.")
    parser.add_argument("--group", type=str, default="raw-archiver-m6b2-v1", help="Kafka consumer group")
    parser.add_argument("--topics", type=str, default="lab6-order-events,lab6-activity-events", help="Comma-separated topics")
    parser.add_argument("--bootstrap", type=str, default=default_bootstrap, help="Kafka bootstrap servers")
    parser.add_argument("--s3-endpoint", type=str, default=default_s3, help="S3 Endpoint URL")
    parser.add_argument("--bucket", type=str, default=default_bucket, help="S3 Bucket name")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after archiving N records")

    args = parser.parse_args()
    topic_list = [t.strip() for t in args.topics.split(",") if t.strip()]
    start_metrics_server(port=metrics_port)

    archiver = RawArchiver(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        topics=topic_list,
        s3_endpoint=args.s3_endpoint,
        bucket_name=args.bucket,
        aws_access_key=default_access,
        aws_secret_key=default_secret,
    )
    archiver.start(max_records=args.max_records)


if __name__ == "__main__":
    main()

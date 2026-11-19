import logging
from typing import Any, Callable, Dict, Optional
from confluent_kafka import KafkaException, Producer

logger = logging.getLogger(__name__)


class OrderProducer:
    """Wrapper around confluent_kafka.Producer with delivery callbacks and tracking."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", client_id: str = "order-producer-client"):
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 200,
        }
        self.producer = Producer(self.config)
        self.delivered_count = 0
        self.failed_count = 0

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback triggered by poll() or flush() once Kafka broker responds."""
        # TODO: Học viên hoàn thiện callback: nếu err is not None thì tăng failed_count, ngược lại tăng delivered_count
        raise NotImplementedError("Học viên cần triển khai hàm _delivery_callback")

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        callback: Optional[Callable[[Any, Any], None]] = None,
    ) -> None:
        """Publishes a single message to Kafka. Key is mandatory to ensure consistent partition hashing."""
        # TODO: Học viên gọi self.producer.produce() và self.producer.poll(0)
        raise NotImplementedError("Học viên cần triển khai hàm produce")

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all outstanding messages to be delivered."""
        return self.producer.flush(timeout)

    def stats(self) -> Dict[str, int]:
        return {
            "delivered": self.delivered_count,
            "failed": self.failed_count,
        }

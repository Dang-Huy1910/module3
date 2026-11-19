import argparse
from datetime import datetime, timezone
import logging
import os
from typing import Any, Dict, List
import boto3
from botocore.client import Config
import clickhouse_connect

from src.common.serialization import deserialize_event
from src.common.validation import validate_order_created, validate_product_viewed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("warehouse_loader")


class WarehouseLoader:
    """Reads raw JSON objects from S3 Data Lake, validates schemas,

    and loads into ClickHouse analytical tables.
    """

    def __init__(
        self,
        s3_endpoint: str,
        bucket_name: str,
        clickhouse_host: str,
        clickhouse_port: int,
        clickhouse_db: str,
        clickhouse_user: str = "default",
        clickhouse_password: str = "",
        aws_access_key: str = "anykey",
        aws_secret_key: str = "anysecret",
    ):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
            region_name="us-east-1",
        )
        self.ch_client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_db,
            username=clickhouse_user,
            password=clickhouse_password,
        )

    def load_from_prefix(self, prefix: str = "raw/") -> Dict[str, int]:
        # TODO: Học viên hoàn thiện logic đọc S3 và nạp vào ClickHouse:
        # Lỗi đọc S3/ClickHouse phải làm job thất bại để chạy lại; chỉ bỏ qua event sai schema.
        # 1. Liệt kê các object S3 theo prefix
        # 2. Đọc payload bytes và metadata nguồn (Kafka topic, partition, offset)
        # 3. Validate dữ liệu theo từng event_type (OrderCreated / ProductViewed)
        # 4. Insert batch vào ClickHouse bảng analytics.orders hoặc analytics.product_views
        raise NotImplementedError("Học viên cần triển khai hàm load_from_prefix")


def main():
    default_s3 = os.getenv("S3_ENDPOINT_URL", "http://object-storage:8333")
    default_bucket = os.getenv("S3_BUCKET_NAME", "ecommerce-raw")
    default_ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    default_ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    default_ch_db = os.getenv("CLICKHOUSE_DB", "analytics")
    default_ch_user = os.getenv("CLICKHOUSE_USER", "default")
    default_ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    default_access = os.getenv("AWS_ACCESS_KEY_ID", "anykey")
    default_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "anysecret")

    parser = argparse.ArgumentParser(description="Load S3 raw events to ClickHouse.")
    parser.add_argument("--prefix", type=str, default="raw/")
    args = parser.parse_args()

    loader = WarehouseLoader(
        s3_endpoint=default_s3,
        bucket_name=default_bucket,
        clickhouse_host=default_ch_host,
        clickhouse_port=default_ch_port,
        clickhouse_db=default_ch_db,
        clickhouse_user=default_ch_user,
        clickhouse_password=default_ch_password,
        aws_access_key=default_access,
        aws_secret_key=default_secret,
    )
    loader.load_from_prefix(prefix=args.prefix)


if __name__ == "__main__":
    main()

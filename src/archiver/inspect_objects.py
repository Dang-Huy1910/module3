import argparse
import json
import os
import boto3
from botocore.client import Config
from tabulate import tabulate


def main():
    default_s3 = os.getenv("S3_ENDPOINT_URL", "http://localhost:8333")
    default_bucket = os.getenv("S3_BUCKET_NAME", "ecommerce-raw")
    default_access = os.getenv("AWS_ACCESS_KEY_ID", "anykey")
    default_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "anysecret")

    parser = argparse.ArgumentParser(description="Inspect S3 Objects in SeaweedFS/MinIO.")
    parser.add_argument("--s3-endpoint", type=str, default=default_s3, help="S3 Endpoint URL")
    parser.add_argument("--bucket", type=str, default=default_bucket, help="S3 Bucket name")
    parser.add_argument("--prefix", type=str, default="raw/", help="Prefix to list")
    parser.add_argument("--max", type=int, default=20, help="Max objects to show")
    parser.add_argument("--list-buckets", action="store_true", help="List all buckets")
    parser.add_argument("--key", type=str, default=None, help="Inspect specific S3 object key content & metadata")

    args = parser.parse_args()

    s3_client = boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=default_access,
        aws_secret_access_key=default_secret,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
        region_name="us-east-1",
    )

    if args.list_buckets:
        res = s3_client.list_buckets()
        buckets = [b["Name"] for b in res.get("Buckets", [])]
        print(f"Buckets found: {buckets}")
        return

    if args.key:
        try:
            res = s3_client.get_object(Bucket=args.bucket, Key=args.key)
            body = res["Body"].read().decode("utf-8")
            meta = res.get("Metadata", {})
            print(f"\n--- S3 OBJECT: {args.key} ---")
            print(f"Metadata: {json.dumps(meta, indent=2)}")
            print("Payload:")
            print(json.dumps(json.loads(body), indent=2))
        except Exception as err:
            print(f"Error fetching object '{args.key}': {err}")
        return

    # List objects
    try:
        response = s3_client.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix, MaxKeys=args.max)
        contents = response.get("Contents", [])
        if not contents:
            print(f"No objects found in bucket '{args.bucket}' with prefix '{args.prefix}'.")
            return

        rows = []
        for obj in contents:
            rows.append([obj["Key"], obj["Size"], str(obj["LastModified"])])

        print(f"\n--- OBJECTS IN BUCKET '{args.bucket}' (Prefix: {args.prefix}) ---")
        try:
            print(tabulate(rows, headers=["Key", "Size (Bytes)", "Last Modified"], tablefmt="psql"))
        except Exception:
            for r in rows:
                print(r)
        print(f"Total shown: {len(contents)}\n")
    except Exception as err:
        print(f"Failed to list S3 objects: {err}")


if __name__ == "__main__":
    main()

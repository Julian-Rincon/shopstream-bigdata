from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import boto3
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload generated ShopStream CSV data to S3.")
    parser.add_argument("--local-dir", default="data", help="Local generated data directory.")
    parser.add_argument("--prefix", default="events", help="S3 prefix for event data.")
    return parser.parse_args()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def build_s3_client(region: str):
    session_kwargs = {"region_name": region}
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")

    if aws_access_key_id and aws_secret_access_key:
        session_kwargs.update(
            {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            }
        )
        if aws_session_token:
            session_kwargs["aws_session_token"] = aws_session_token

    return boto3.Session(**session_kwargs).client("s3")


def iter_csv_files(local_dir: Path) -> list[Path]:
    files = sorted(local_dir.glob("year=*/month=*/day=*/*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found under {local_dir}/year=*/month=*/day=*/*.csv")
    return files


def s3_key_for(file_path: Path, local_dir: Path, prefix: str) -> str:
    relative_path = file_path.relative_to(local_dir).as_posix()
    return f"{prefix.strip('/')}/{relative_path}"


def main() -> None:
    args = parse_args()
    load_dotenv()

    local_dir = Path(args.local_dir)
    region = required_env("AWS_REGION")
    bucket = required_env("S3_BUCKET_RAW")
    s3 = build_s3_client(region)

    files = iter_csv_files(local_dir)
    start = time.perf_counter()
    total_bytes = 0

    for index, file_path in enumerate(files, start=1):
        size_bytes = file_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        key = s3_key_for(file_path, local_dir, args.prefix)
        print(f"[{index}/{len(files)}] Uploading {file_path} -> s3://{bucket}/{key} ({size_mb:.2f} MB)")
        s3.upload_file(str(file_path), bucket, key)
        total_bytes += size_bytes

    elapsed = time.perf_counter() - start
    total_mb = total_bytes / (1024 * 1024)
    print("\nUpload summary:")
    print(f"files_uploaded: {len(files)}")
    print(f"total_mb: {total_mb:.2f}")
    print(f"elapsed_seconds: {elapsed:.2f}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Generate _MANIFEST.txt files for S3 folders that match:
  bureau_tables/yyyyMM/<dataset>/DATA/

Manifest format:
  CREATION_DATE: dd/mm/yyyy
  SNAPSHOT_DATE: yyyy-mm-dd
  FILE_NAME: _MANIFEST.txt
  NUMBER_OF_FILES: n
  TOTAL_RECORD_COUNT: rc
  parquet_file_1.parquet: count_1
  parquet_file_2.parquet: count_2
  ...
"""

from __future__ import annotations

import argparse
import calendar
import logging
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Tuple

import boto3
from botocore.config import Config
import pyarrow.fs as pafs
import pyarrow.parquet as pq


LOGGER = logging.getLogger("manifest-generator")
DATA_FOLDER_RE = re.compile(r"^bureau_tables/(\d{6})/[^/]+/DATA/$")


@dataclass(frozen=True)
class S3Object:
    bucket: str
    key: str


def month_end_from_yyyymm(yyyymm: str) -> date:
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    return date(year, month, calendar.monthrange(year, month)[1])


def iter_s3_objects(
    s3_client, bucket: str, prefix: str
) -> Iterable[Dict[str, str]]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj


def parquet_folders_with_files(
    s3_client, bucket: str, prefix: str
) -> Dict[str, List[S3Object]]:
    folders: Dict[str, List[S3Object]] = defaultdict(list)
    for obj in iter_s3_objects(s3_client, bucket, prefix):
        key = obj["Key"]
        if not key.lower().endswith(".parquet"):
            continue
        folder = key.rsplit("/", 1)[0] + "/"
        if DATA_FOLDER_RE.match(folder):
            folders[folder].append(S3Object(bucket=bucket, key=key))
    return folders


def parquet_row_count(fs: pafs.S3FileSystem, s3_obj: S3Object) -> int:
    metadata = pq.read_metadata(f"{s3_obj.bucket}/{s3_obj.key}", filesystem=fs)
    return metadata.num_rows


def format_manifest(yyyymm: str, file_counts: Dict[str, int]) -> str:
    end_dt = month_end_from_yyyymm(yyyymm)
    lines = [
        f"CREATION_DATE: {end_dt.strftime('%d/%m/%Y')}",
        f"SNAPSHOT_DATE: {end_dt.isoformat()}",
        "FILE_NAME: _MANIFEST.txt",
        f"NUMBER_OF_FILES: {len(file_counts)}",
        f"TOTAL_RECORD_COUNT: {sum(file_counts.values())}",
    ]
    for file_name in sorted(file_counts.keys()):
        lines.append(f"{file_name}: {file_counts[file_name]}")
    return "\n".join(lines) + "\n"


def write_manifest(s3_client, bucket: str, key: str, body: str, dry_run: bool) -> None:
    if dry_run:
        LOGGER.info("[DRY RUN] Would write s3://%s/%s", bucket, key)
        return

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="text/plain",
    )
    LOGGER.info("Wrote s3://%s/%s", bucket, key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate _MANIFEST.txt files for S3 parquet DATA folders."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--prefix",
        default="bureau_tables/",
        help="S3 prefix to scan (default: bureau_tables/)",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region. If omitted, SDK default resolution is used.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=64,
        help="Parallel workers for parquet metadata reads (default: 64)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log actions without writing manifests.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )
    return parser.parse_args()


def build_clients(region: str | None, workers: int):
    config = Config(
        max_pool_connections=max(workers, 10),
        retries={"max_attempts": 10, "mode": "adaptive"},
    )
    s3_client = boto3.client("s3", region_name=region, config=config)
    s3_fs = pafs.S3FileSystem(region=region)
    return s3_client, s3_fs


def collect_row_counts(
    s3_fs: pafs.S3FileSystem, parquet_objects: List[S3Object], workers: int
) -> Dict[Tuple[str, str], int]:
    total_files = len(parquet_objects)
    row_counts: Dict[Tuple[str, str], int] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_obj = {
            executor.submit(parquet_row_count, s3_fs, obj): obj for obj in parquet_objects
        }
        for i, future in enumerate(as_completed(future_to_obj), start=1):
            obj = future_to_obj[future]
            row_counts[(obj.bucket, obj.key)] = future.result()
            if i % 500 == 0 or i == total_files:
                LOGGER.info("Processed %d/%d parquet files", i, total_files)
    return row_counts


def generate_manifests(
    s3_client,
    s3_fs: pafs.S3FileSystem,
    bucket: str,
    prefix: str,
    workers: int,
    dry_run: bool,
) -> None:
    folders = parquet_folders_with_files(s3_client=s3_client, bucket=bucket, prefix=prefix)
    if not folders:
        LOGGER.warning(
            "No parquet files found under s3://%s/%s matching .../yyyyMM/*/DATA/",
            bucket,
            prefix,
        )
        return

    all_parquet_files = [obj for objects in folders.values() for obj in objects]
    LOGGER.info("Found %d parquet files in %d DATA folders", len(all_parquet_files), len(folders))

    row_counts = collect_row_counts(s3_fs, all_parquet_files, workers=workers)

    for folder in sorted(folders.keys()):
        match = DATA_FOLDER_RE.match(folder)
        if not match:
            continue

        yyyymm = match.group(1)
        file_counts: Dict[str, int] = {}
        for obj in folders[folder]:
            file_name = obj.key.rsplit("/", 1)[1]
            file_counts[file_name] = row_counts[(obj.bucket, obj.key)]

        manifest_content = format_manifest(yyyymm, file_counts)
        manifest_key = f"{folder}_MANIFEST.txt"
        write_manifest(s3_client, bucket, manifest_key, manifest_content, dry_run=dry_run)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    s3_client, s3_fs = build_clients(args.region, args.workers)
    generate_manifests(
        s3_client=s3_client,
        s3_fs=s3_fs,
        bucket=args.bucket,
        prefix=args.prefix,
        workers=args.workers,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

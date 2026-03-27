"""Analyze an S3 bucket: size, object count, and CloudWatch metrics."""

from __future__ import annotations

import boto3
from datetime import datetime, timedelta, timezone


def get_bucket_region(bucket: str) -> str:
    """Get the region of an S3 bucket."""
    s3 = boto3.client("s3")
    resp = s3.get_bucket_location(Bucket=bucket)
    loc = resp.get("LocationConstraint")
    return loc or "us-east-1"


def analyze_bucket(bucket: str, prefix: str = "") -> dict:
    """Walk an S3 bucket and return size/object stats."""
    region = get_bucket_region(bucket)
    s3 = boto3.client("s3", region_name=region)

    total_size = 0
    object_count = 0
    storage_classes: dict[str, int] = {}

    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            size = obj["Size"]
            sc = obj.get("StorageClass", "STANDARD")
            total_size += size
            object_count += 1
            storage_classes[sc] = storage_classes.get(sc, 0) + size

    return {
        "bucket": bucket,
        "prefix": prefix,
        "region": region,
        "total_bytes": total_size,
        "total_gb": total_size / (1024 ** 3),
        "total_tb": total_size / (1024 ** 4),
        "object_count": object_count,
        "storage_classes": storage_classes,
    }


def get_cloudwatch_metrics(bucket: str, region: str, days: int = 30) -> dict:
    """Fetch request and egress metrics from CloudWatch (best-effort)."""
    try:
        cw = boto3.client("cloudwatch", region_name=region)
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)

        def _get_sum(namespace, metric, dimensions, unit):
            try:
                resp = cw.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric,
                    Dimensions=dimensions,
                    StartTime=start,
                    EndTime=end,
                    Period=days * 86400,
                    Statistics=["Sum"],
                    Unit=unit,
                )
                points = resp.get("Datapoints", [])
                return sum(p["Sum"] for p in points)
            except Exception:
                return None

        dims = [{"Name": "BucketName", "Value": bucket}]
        filter_dims = dims + [{"Name": "FilterId", "Value": "EntireBucket"}]

        gets = _get_sum("AWS/S3", "GetRequests", dims, "Count")
        puts = _get_sum("AWS/S3", "PutRequests", dims, "Count")
        bytes_down = _get_sum("AWS/S3", "BytesDownloaded", filter_dims, "Bytes")

        return {
            "get_requests": int(gets) if gets else None,
            "put_requests": int(puts) if puts else None,
            "bytes_downloaded": int(bytes_down) if bytes_down else None,
            "egress_gb": bytes_down / (1024 ** 3) if bytes_down else None,
            "days": days,
        }
    except Exception:
        return {
            "get_requests": None,
            "put_requests": None,
            "bytes_downloaded": None,
            "egress_gb": None,
            "days": days,
        }

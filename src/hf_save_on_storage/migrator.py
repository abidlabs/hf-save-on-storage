"""Migrate objects from S3 to a Hugging Face Storage Bucket."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import boto3
from huggingface_hub import HfApi


def migrate_bucket(
    s3_bucket: str,
    hf_repo_id: str,
    prefix: str = "",
    repo_type: str = "dataset",
    private: bool = True,
    s3_region: str | None = None,
    progress_callback=None,
) -> dict:
    """Stream objects from S3 to an HF repo, file by file via temp dir."""
    api = HfApi()

    # Create the HF repo if it doesn't exist
    api.create_repo(
        repo_id=hf_repo_id,
        repo_type=repo_type,
        private=private,
        exist_ok=True,
    )

    s3 = boto3.client("s3", region_name=s3_region) if s3_region else boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": s3_bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    migrated = 0
    failed = 0
    total_bytes = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size = obj["Size"]

                if key.endswith("/"):
                    continue  # skip directory markers

                # Determine path in HF repo
                if prefix:
                    path_in_repo = key[len(prefix):].lstrip("/")
                else:
                    path_in_repo = key

                if not path_in_repo:
                    continue

                local_path = Path(tmpdir) / path_in_repo.replace("/", "_")

                try:
                    s3.download_file(s3_bucket, key, str(local_path))
                    api.upload_file(
                        path_or_fileobj=str(local_path),
                        path_in_repo=path_in_repo,
                        repo_id=hf_repo_id,
                        repo_type=repo_type,
                    )
                    migrated += 1
                    total_bytes += size
                    if progress_callback:
                        progress_callback(key, size, True)
                except Exception as e:
                    failed += 1
                    if progress_callback:
                        progress_callback(key, size, False, str(e))
                finally:
                    if local_path.exists():
                        local_path.unlink()

    return {
        "migrated": migrated,
        "failed": failed,
        "total_bytes": total_bytes,
    }

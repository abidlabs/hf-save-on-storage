"""Migrate objects from S3 to a Hugging Face Storage Bucket."""

from __future__ import annotations

import tempfile
from pathlib import Path

from huggingface_hub import HfApi

from .s3_analyzer import _make_s3_client, get_bucket_region


def migrate_bucket(
    s3_bucket: str,
    hf_bucket_id: str,
    prefix: str = "",
    private: bool = True,
    s3_region: str | None = None,
    progress_callback=None,
    batch_size: int = 50,
) -> dict:
    """Stream objects from S3 to an HF Storage Bucket, in batches."""
    api = HfApi()

    # Create the HF bucket if it doesn't exist
    api.create_bucket(hf_bucket_id, private=private, exist_ok=True)

    region = s3_region or get_bucket_region(s3_bucket)

    # Try unsigned (public) first, fall back to authenticated
    s3 = _make_s3_client(region, unsigned=True)
    try:
        s3.head_bucket(Bucket=s3_bucket)
    except Exception:
        s3 = _make_s3_client(region, unsigned=False)

    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": s3_bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    migrated = 0
    failed = 0
    total_bytes = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Collect files in batches for efficient upload
        batch: list[tuple[str, str, str, int]] = []  # (local_path, path_in_bucket, key, size)

        def flush_batch():
            nonlocal migrated, failed, total_bytes
            if not batch:
                return

            add_list = []
            for local_path, path_in_bucket, key, size in batch:
                add_list.append((local_path, path_in_bucket))

            try:
                api.batch_bucket_files(
                    hf_bucket_id,
                    add=add_list,
                )
                # All succeeded
                for local_path, path_in_bucket, key, size in batch:
                    migrated += 1
                    total_bytes += size
                    if progress_callback:
                        progress_callback(key, size, True)
            except Exception as e:
                # Batch failed — try individually to identify which files failed
                for local_path, path_in_bucket, key, size in batch:
                    try:
                        api.batch_bucket_files(
                            hf_bucket_id,
                            add=[(local_path, path_in_bucket)],
                        )
                        migrated += 1
                        total_bytes += size
                        if progress_callback:
                            progress_callback(key, size, True)
                    except Exception as inner_e:
                        failed += 1
                        if progress_callback:
                            progress_callback(key, size, False, str(inner_e))
            finally:
                # Clean up temp files
                for local_path, _, _, _ in batch:
                    p = Path(local_path)
                    if p.exists():
                        p.unlink()
                batch.clear()

        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size = obj["Size"]

                if key.endswith("/"):
                    continue  # skip directory markers

                # Determine path in HF bucket
                if prefix:
                    path_in_bucket = key[len(prefix):].lstrip("/")
                else:
                    path_in_bucket = key

                if not path_in_bucket:
                    continue

                local_path = str(Path(tmpdir) / path_in_bucket.replace("/", "_"))

                try:
                    s3.download_file(s3_bucket, key, local_path)
                    batch.append((local_path, path_in_bucket, key, size))
                except Exception as e:
                    failed += 1
                    if progress_callback:
                        progress_callback(key, size, False, str(e))
                    continue

                if len(batch) >= batch_size:
                    flush_batch()

        # Flush remaining files
        flush_batch()

    return {
        "migrated": migrated,
        "failed": failed,
        "total_bytes": total_bytes,
    }

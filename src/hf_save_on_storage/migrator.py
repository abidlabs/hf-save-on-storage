"""Migrate objects from S3 to a Hugging Face Storage Bucket."""

from __future__ import annotations

import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from huggingface_hub import HfApi
from huggingface_hub.utils import disable_progress_bars

from .s3_analyzer import _make_s3_client, get_bucket_region


def migrate_bucket(
    s3_bucket: str,
    hf_bucket_id: str,
    prefix: str = "",
    private: bool = True,
    s3_region: str | None = None,
    progress_callback=None,
    batch_size: int = 50,
    download_workers: int = 8,
) -> dict:
    """Stream objects from S3 to an HF Storage Bucket with parallel downloads.

    Downloads happen concurrently in a thread pool. As soon as a batch fills up,
    it is uploaded to HF while the next batch of downloads continues.
    """
    api = HfApi()

    # Suppress huggingface_hub's own progress bars — we show our own
    disable_progress_bars()

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
    lock = threading.Lock()

    def upload_batch(batch_items):
        """Upload a batch of already-downloaded files to HF."""
        nonlocal migrated, failed, total_bytes

        add_list = [(lp, pib) for lp, pib, _, _ in batch_items]

        try:
            api.batch_bucket_files(hf_bucket_id, add=add_list)
            # All succeeded
            for _, _, key, size in batch_items:
                with lock:
                    migrated += 1
                    total_bytes += size
                if progress_callback:
                    progress_callback(key, size, True)
        except Exception:
            # Batch failed — try individually to identify which files failed
            for local_path, path_in_bucket, key, size in batch_items:
                try:
                    api.batch_bucket_files(
                        hf_bucket_id,
                        add=[(local_path, path_in_bucket)],
                    )
                    with lock:
                        migrated += 1
                        total_bytes += size
                    if progress_callback:
                        progress_callback(key, size, True)
                except Exception as inner_e:
                    with lock:
                        failed += 1
                    if progress_callback:
                        progress_callback(key, size, False, str(inner_e))
        finally:
            for local_path, _, _, _ in batch_items:
                p = Path(local_path)
                if p.exists():
                    p.unlink()

    def download_one(s3_client, bucket, key, local_path):
        """Download a single file from S3. Returns the local path or raises."""
        s3_client.download_file(bucket, key, local_path)
        return local_path

    with tempfile.TemporaryDirectory() as tmpdir:
        file_counter = 0
        batch: list[tuple[str, str, str, int]] = []
        upload_future = None

        with (
            ThreadPoolExecutor(max_workers=download_workers) as dl_pool,
            ThreadPoolExecutor(max_workers=1) as ul_pool,
        ):

            def flush_batch():
                """Submit current batch for upload (non-blocking) and start a new batch."""
                nonlocal upload_future, batch
                if not batch:
                    return
                # Wait for any previous upload to finish before starting the next
                if upload_future is not None:
                    upload_future.result()
                batch_to_upload = batch
                batch = []
                upload_future = ul_pool.submit(upload_batch, batch_to_upload)

            for page in paginator.paginate(**kwargs):
                objects = page.get("Contents", [])
                # Filter and prepare download tasks
                to_download = []
                for obj in objects:
                    key = obj["Key"]
                    size = obj["Size"]

                    if key.endswith("/"):
                        continue

                    if prefix:
                        path_in_bucket = key[len(prefix) :].lstrip("/")
                    else:
                        path_in_bucket = key

                    if not path_in_bucket:
                        continue

                    # Use a counter to avoid filename collisions
                    file_counter += 1
                    local_path = str(
                        Path(tmpdir)
                        / f"{file_counter}_{path_in_bucket.replace('/', '_')}"
                    )
                    to_download.append((key, size, path_in_bucket, local_path))

                # Submit all downloads for this page concurrently
                futures = {
                    dl_pool.submit(download_one, s3, s3_bucket, key, local_path): (
                        key,
                        size,
                        path_in_bucket,
                        local_path,
                    )
                    for key, size, path_in_bucket, local_path in to_download
                }

                for future in as_completed(futures):
                    key, size, path_in_bucket, local_path = futures[future]
                    try:
                        future.result()
                        batch.append((local_path, path_in_bucket, key, size))
                    except Exception as e:
                        with lock:
                            failed += 1
                        if progress_callback:
                            progress_callback(key, size, False, str(e))
                        continue

                    if len(batch) >= batch_size:
                        flush_batch()

            # Flush remaining files
            flush_batch()

            # Wait for the final upload to complete
            if upload_future is not None:
                upload_future.result()

    return {
        "migrated": migrated,
        "failed": failed,
        "total_bytes": total_bytes,
    }

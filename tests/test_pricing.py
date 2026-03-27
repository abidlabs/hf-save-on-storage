"""Tests for pricing calculations."""

from hf_save_on_storage.pricing import (
    estimate_s3_monthly_cost,
    estimate_hf_monthly_cost,
)


def test_small_bucket():
    """1 TB bucket, 100 GB egress, 1M GETs."""
    s3 = estimate_s3_monthly_cost(
        size_gb=1024, egress_gb=100, get_requests=1_000_000
    )
    hf = estimate_hf_monthly_cost(1024, private=True)

    assert s3["storage"] > 0
    assert s3["egress"] > 0
    assert s3["requests"] > 0
    assert hf["egress"] == 0
    assert hf["requests"] == 0
    assert s3["total"] > hf["total"], "HF should be cheaper for 1TB"


def test_large_bucket():
    """100 TB bucket."""
    size_gb = 100 * 1024
    s3 = estimate_s3_monthly_cost(size_gb=size_gb, egress_gb=1000)
    hf_private = estimate_hf_monthly_cost(size_gb, private=True)
    hf_public = estimate_hf_monthly_cost(size_gb, private=False)

    assert hf_public["total"] < hf_private["total"]
    assert s3["total"] > hf_private["total"]


def test_storage_only():
    """Pure storage comparison, no egress/requests."""
    size_gb = 10 * 1024  # 10 TB
    s3 = estimate_s3_monthly_cost(size_gb=size_gb)
    hf = estimate_hf_monthly_cost(size_gb, private=True)

    # S3: ~$23/TB, HF private: $18/TB
    assert s3["total"] > hf["total"]


def test_zero_bucket():
    s3 = estimate_s3_monthly_cost(0)
    hf = estimate_hf_monthly_cost(0)
    assert s3["total"] == 0
    assert hf["total"] == 0


def test_public_cheaper_than_private():
    hf_pub = estimate_hf_monthly_cost(1024, private=False)
    hf_prv = estimate_hf_monthly_cost(1024, private=True)
    assert hf_pub["total"] < hf_prv["total"]

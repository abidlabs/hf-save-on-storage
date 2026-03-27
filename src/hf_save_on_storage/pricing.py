"""Pricing models for AWS S3 and HF Storage Buckets."""

# AWS S3 Standard storage pricing (US East, per GB/month)
# https://aws.amazon.com/s3/pricing/
S3_STORAGE_TIERS = [
    (50 * 1024, 0.023),  # First 50 TB: $0.023/GB
    (450 * 1024, 0.022),  # Next 450 TB: $0.022/GB
    (float("inf"), 0.021),  # Over 500 TB: $0.021/GB
]

# S3 egress pricing (per GB, US East)
S3_EGRESS_TIERS = [
    (10 * 1024, 0.09),  # First 10 TB: $0.09/GB
    (40 * 1024, 0.085),  # Next 40 TB: $0.085/GB
    (100 * 1024, 0.07),  # Next 100 TB: $0.07/GB
    (float("inf"), 0.05),  # Over 150 TB: $0.05/GB
]

# S3 request pricing
S3_GET_REQUEST_PRICE = 0.0004 / 1000  # $0.0004 per 1,000 GET
S3_PUT_REQUEST_PRICE = 0.005 / 1000  # $0.005 per 1,000 PUT
S3_LIST_REQUEST_PRICE = 0.005 / 1000  # $0.005 per 1,000 LIST

# HF Storage Buckets pricing (per TB/month)
HF_STORAGE_PUBLIC = [
    (50, 12.0),  # Base: $12/TB
    (200, 10.0),  # 50TB+: $10/TB (20% off)
    (500, 9.0),  # 200TB+: $9/TB (25% off)
    (float("inf"), 8.0),  # 500TB+: $8/TB (33% off)
]

HF_STORAGE_PRIVATE = [
    (50, 18.0),  # Base: $18/TB
    (200, 16.0),  # 50TB+: $16/TB
    (500, 14.0),  # 200TB+: $14/TB
    (float("inf"), 12.0),  # 500TB+: $12/TB
]


def calc_tiered_cost(size_gb: float, tiers: list[tuple[float, float]]) -> float:
    """Calculate cost using tiered pricing. Tiers are (threshold_gb, price_per_gb)."""
    total = 0.0
    remaining = size_gb
    prev_threshold = 0.0
    for threshold, price in tiers:
        tier_size = min(remaining, threshold - prev_threshold)
        if tier_size <= 0:
            break
        total += tier_size * price
        remaining -= tier_size
        prev_threshold = threshold
    return total


def calc_s3_storage_cost(size_gb: float) -> float:
    return calc_tiered_cost(size_gb, S3_STORAGE_TIERS)


def calc_s3_egress_cost(egress_gb: float) -> float:
    # First 1 GB free
    egress_gb = max(0, egress_gb - 1)
    return calc_tiered_cost(egress_gb, S3_EGRESS_TIERS)


def calc_s3_request_cost(gets: int, puts: int, lists: int) -> float:
    return (
        gets * S3_GET_REQUEST_PRICE
        + puts * S3_PUT_REQUEST_PRICE
        + lists * S3_LIST_REQUEST_PRICE
    )


def calc_hf_storage_cost(size_tb: float, private: bool = True) -> float:
    """HF Buckets pricing. Egress and requests are included."""
    tiers = HF_STORAGE_PRIVATE if private else HF_STORAGE_PUBLIC
    total = 0.0
    remaining = size_tb
    prev_threshold = 0.0
    for threshold, price_per_tb in tiers:
        tier_size = min(remaining, threshold - prev_threshold)
        if tier_size <= 0:
            break
        total += tier_size * price_per_tb
        remaining -= tier_size
        prev_threshold = threshold
    return total


def estimate_s3_monthly_cost(
    size_gb: float,
    egress_gb: float = 0,
    get_requests: int = 0,
    put_requests: int = 0,
    list_requests: int = 0,
) -> dict:
    storage = calc_s3_storage_cost(size_gb)
    egress = calc_s3_egress_cost(egress_gb)
    requests = calc_s3_request_cost(get_requests, put_requests, list_requests)
    return {
        "storage": storage,
        "egress": egress,
        "requests": requests,
        "total": storage + egress + requests,
    }


def estimate_hf_monthly_cost(size_gb: float, private: bool = True) -> dict:
    size_tb = size_gb / 1024
    storage = calc_hf_storage_cost(size_tb, private)
    return {
        "storage": storage,
        "egress": 0.0,  # included
        "requests": 0.0,  # included
        "total": storage,
    }

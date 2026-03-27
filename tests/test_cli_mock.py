"""End-to-end CLI test using mocked S3."""

import subprocess
import sys
from unittest.mock import patch, MagicMock

from hf_save_on_storage.s3_analyzer import analyze_bucket


def _mock_paginate(**kwargs):
    """Return fake S3 objects totaling ~5 GB."""
    page = {
        "Contents": [
            {
                "Key": f"data/file_{i}.parquet",
                "Size": 500 * 1024 * 1024,
                "StorageClass": "STANDARD",
            }
            for i in range(10)
        ]
    }
    return [page]


def test_analyze_bucket_mock():
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = _mock_paginate()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.get_bucket_location.return_value = {"LocationConstraint": "us-east-1"}

    with patch("hf_save_on_storage.s3_analyzer.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        info = analyze_bucket("test-bucket")

    assert info["object_count"] == 10
    assert info["total_gb"] > 4.5
    assert info["total_gb"] < 5.5
    assert "STANDARD" in info["storage_classes"]


def test_cli_analyze_only():
    """Test the CLI runs with --analyze-only using mocked S3."""
    result = subprocess.run(
        [sys.executable, "-m", "hf_save_on_storage.cli", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "save-on-storage" in result.stdout

<img width="1234" height="556" alt="image" src="./assets/hero.png" />

# `hf save-on-storage`

An unofficial [Hugging Face CLI extension](https://huggingface.co/docs/huggingface_hub/en/guides/cli-extensions) that analyzes your AWS S3 bucket and shows how much you'd save by migrating the data in the bucket to [HF Storage Buckets](https://huggingface.co/storage). If you like what you see, you can have it migrate the data for you too :)

## Quickstart

Try it right now against one of our public S3 buckets by running the following commands in your terminal (no login needed):

```bash
pip install --upgrade huggingface_hub
hf extensions install abidlabs/hf-save-on-storage
hf save-on-storage gradio-pypi-previews
```

This scans the public `gradio-pypi-previews` bucket (~547 GB, 9k+ objects) and shows you'd save at least ~24% by moving to HF Buckets (actually even higher if we were to include egress and API costs).

<img width="946" height="587" alt="image" src="https://github.com/user-attachments/assets/dc94ba87-ae24-42cc-8d2b-81af11f7a054" />


## Usage

```bash
# Analyze your own S3 bucket (requires you to be logged in via aws cli if it's a private bucket)
hf save-on-storage my-s3-bucket --analyze-only

# Include egress estimate (100 GB/month) for a fuller comparison
hf save-on-storage my-s3-bucket --egress 100 --analyze-only

# Compare against public repo pricing (cheaper)
hf save-on-storage my-s3-bucket --public --analyze-only

# Only analyze a specific prefix
hf save-on-storage my-s3-bucket --prefix models/v2/ --analyze-only

# Full run: analyze + migrate to an HF dataset repo
hf save-on-storage my-s3-bucket --hf-repo myuser/my-dataset
```

## What it does

1. **Scans your S3 bucket** — counts objects, total size, storage classes
2. **Fetches CloudWatch metrics** — GET/PUT requests and egress over the last 30 days (if available)
3. **Compares costs** — shows a line-by-line breakdown of S3 vs HF Buckets pricing
4. **Offers to migrate** — streams files from S3 to an HF repo if you agree


## Why HF Buckets are cheaper

| | AWS S3 | HF Buckets |
|---|---|---|
| Storage | $23/TB | $8–18/TB |
| Egress | $0.05–0.09/GB | **Free** (included) |
| API requests | $0.0004–0.005/1K | **Free** (included) |
| CDN | Extra cost | **Free** (included) |
| Deduplication | N/A | Built-in (up to 4x savings) |

## Options

| Flag | Description |
|---|---|
| `bucket` | S3 bucket name (required) |
| `--prefix` | Only analyze objects under this S3 prefix |
| `--public` | Use HF public repo pricing (default: private) |
| `--egress GB` | Manual monthly egress estimate in GB |
| `--hf-repo` | HF repo ID for migration (e.g. `user/dataset-name`) |
| `--repo-type` | `dataset`, `model`, or `space` (default: `dataset`) |
| `--analyze-only` | Show cost comparison without offering migration |

## Requirements

- Python >= 3.10
- AWS credentials configured (`aws configure` or env vars), only needed for private S3 buckets
- `hf` CLI logged in (`huggingface-cli login`), only needed if you'd like to migrate your data

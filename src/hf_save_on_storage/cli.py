"""CLI entry point for hf save-on-storage."""

from __future__ import annotations

import argparse
import sys

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TaskProgressColumn,
    TransferSpeedColumn,
    DownloadColumn,
    TimeRemainingColumn,
)
from rich import box

from .s3_analyzer import analyze_bucket, get_cloudwatch_metrics
from .pricing import estimate_s3_monthly_cost, estimate_hf_monthly_cost
from .migrator import migrate_bucket

console = Console()


def format_size(size_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} EB"


def format_money(amount: float) -> str:
    return f"${amount:,.2f}"


def run_analysis(args):
    bucket = args.bucket
    prefix = args.prefix or ""
    private = not args.public

    console.print()
    console.print(f"[bold blue]Analyzing S3 bucket:[/] [cyan]{bucket}[/]", end="")
    if prefix:
        console.print(f" (prefix: [cyan]{prefix}[/])", end="")
    console.print()

    # Analyze bucket
    with console.status("[bold green]Scanning S3 bucket..."):
        info = analyze_bucket(bucket, prefix)

    if info["object_count"] == 0:
        console.print("[yellow]Bucket is empty or prefix matched no objects.[/]")
        return

    # Display bucket info
    table = Table(title="S3 Bucket Summary", box=box.ROUNDED)
    table.add_column("Metric", style="bold")
    table.add_column("Value", style="cyan")
    table.add_row("Region", info["region"])
    table.add_row("Objects", f"{info['object_count']:,}")
    table.add_row("Total Size", format_size(info["total_bytes"]))
    for sc, size in info["storage_classes"].items():
        table.add_row(f"  {sc}", format_size(size))
    console.print(table)
    console.print()

    # Get CloudWatch metrics
    with console.status("[bold green]Fetching CloudWatch metrics (last 30 days)..."):
        metrics = get_cloudwatch_metrics(bucket, info["region"])

    egress_gb = metrics.get("egress_gb") or 0
    get_requests = metrics.get("get_requests") or 0
    put_requests = metrics.get("put_requests") or 0

    if metrics["get_requests"] is not None:
        mtable = Table(title="Usage Metrics (last 30 days)", box=box.ROUNDED)
        mtable.add_column("Metric", style="bold")
        mtable.add_column("Value", style="cyan")
        if metrics["get_requests"] is not None:
            mtable.add_row("GET requests", f"{get_requests:,}")
        if metrics["put_requests"] is not None:
            mtable.add_row("PUT requests", f"{put_requests:,}")
        if metrics["egress_gb"] is not None:
            mtable.add_row("Data downloaded", format_size(metrics["bytes_downloaded"]))
        console.print(mtable)
        console.print()
    else:
        console.print(
            "[dim]CloudWatch metrics unavailable (S3 request metrics may not be enabled).[/]"
        )
        console.print(
            "[dim]Using storage-only comparison. Enable S3 request metrics for a fuller picture.[/]"
        )
        console.print()

    # Allow manual overrides for egress if not available
    if metrics["egress_gb"] is None and not args.egress:
        console.print(
            "[dim]Tip: pass --egress <GB> to include estimated monthly egress in the comparison.[/]"
        )
    if args.egress:
        egress_gb = args.egress

    # Calculate costs
    s3_cost = estimate_s3_monthly_cost(
        size_gb=info["total_gb"],
        egress_gb=egress_gb,
        get_requests=get_requests,
        put_requests=put_requests,
        list_requests=0,
    )
    hf_cost = estimate_hf_monthly_cost(info["total_gb"], private=private)

    savings = s3_cost["total"] - hf_cost["total"]
    savings_pct = (savings / s3_cost["total"] * 100) if s3_cost["total"] > 0 else 0

    # Cost comparison table
    ctable = Table(
        title="Monthly Cost Comparison",
        box=box.DOUBLE_EDGE,
        show_footer=True,
    )
    ctable.add_column("Cost Component", style="bold", footer_style="bold")
    ctable.add_column("AWS S3", style="red", justify="right", footer_style="bold red")
    ctable.add_column(
        f"HF Buckets ({'Private' if private else 'Public'})",
        style="green",
        justify="right",
        footer_style="bold green",
    )
    ctable.add_column(
        "You Save", style="yellow", justify="right", footer_style="bold yellow"
    )

    ctable.add_row(
        "Storage",
        format_money(s3_cost["storage"]),
        format_money(hf_cost["storage"]),
        format_money(s3_cost["storage"] - hf_cost["storage"]),
    )
    ctable.add_row(
        "Egress / CDN",
        format_money(s3_cost["egress"]),
        format_money(0),
        format_money(s3_cost["egress"]),
    )
    ctable.add_row(
        "API Requests",
        format_money(s3_cost["requests"]),
        format_money(0),
        format_money(s3_cost["requests"]),
    )
    ctable.columns[0].footer = "Total"
    ctable.columns[1].footer = format_money(s3_cost["total"])
    ctable.columns[2].footer = format_money(hf_cost["total"])
    ctable.columns[3].footer = format_money(savings)

    console.print(ctable)
    console.print()

    no_egress_data = metrics.get("egress_gb") is None and not args.egress

    if savings > 0:
        egress_note = (
            "\n[bold yellow]⚠ Egress costs not included — actual savings are likely even higher![/]"
            if no_egress_data
            else ""
        )
        console.print(
            Panel(
                f"[bold green]You'd save {format_money(savings)}/month ({savings_pct:.0f}%) "
                f"by migrating to HF Storage Buckets![/]\n"
                f"[dim]That's {format_money(savings * 12)}/year.[/]"
                f"{egress_note}\n\n"
                "[dim]HF Buckets include: free egress & CDN, no per-request fees, "
                "Xet deduplication (up to 4x upload savings), and no file-count limits.[/]",
                title="Savings Summary",
                border_style="green",
            )
        )
    else:
        console.print(
            Panel(
                f"[yellow]HF Buckets would cost {format_money(-savings)}/month more for this bucket.[/]\n"
                "[dim]HF Buckets still include free egress, CDN, and deduplication — "
                "savings grow with egress-heavy workloads.[/]",
                title="Cost Comparison",
                border_style="yellow",
            )
        )

    console.print()

    # Offer migration
    if not args.analyze_only and savings > 0:
        if Confirm.ask(
            "[bold]Would you like to migrate this data to HF Storage Buckets?[/]"
        ):
            hf_bucket_id = args.hf_bucket or Prompt.ask(
                "HF bucket ID (e.g. username/my-bucket)",
            )

            console.print()
            console.print(
                f"[bold blue]Migrating to:[/] [cyan]hf://buckets/{hf_bucket_id}[/] (private={private})"
            )
            console.print()

            total_bytes = info["total_bytes"]

            with Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                dl_task = progress.add_task(
                    "[cyan]Downloading from S3", total=total_bytes
                )
                ul_task = progress.add_task(
                    "[green]Uploading to HF    ", total=total_bytes
                )

                def on_download(key, size):
                    progress.advance(dl_task, size)

                def on_upload(key, size, success, error=None):
                    if success:
                        progress.advance(ul_task, size)
                    else:
                        progress.console.print(f"  [red]\u2717[/] {key}: {error}")

                result = migrate_bucket(
                    s3_bucket=bucket,
                    hf_bucket_id=hf_bucket_id,
                    prefix=prefix,
                    private=private,
                    s3_region=info["region"],
                    progress_callback=on_upload,
                    download_callback=on_download,
                )

            console.print()
            console.print(
                Panel(
                    f"[bold green]Migration complete![/]\n"
                    f"Files migrated: {result['migrated']}\n"
                    f"Files failed: {result['failed']}\n"
                    f"Data transferred: {format_size(result['total_bytes'])}",
                    title="Migration Summary",
                    border_style="green",
                )
            )


def main():
    parser = argparse.ArgumentParser(
        prog="hf save-on-storage",
        description="Analyze your S3 bucket and see how much you'd save with HF Storage Buckets.",
    )
    parser.add_argument(
        "bucket",
        help="S3 bucket name (e.g. my-ml-data)",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Only analyze objects under this prefix",
    )
    parser.add_argument(
        "--public",
        action="store_true",
        default=False,
        help="Compare against HF public repo pricing (default: private)",
    )
    parser.add_argument(
        "--egress",
        type=float,
        default=None,
        help="Estimated monthly egress in GB (used if CloudWatch metrics unavailable)",
    )
    parser.add_argument(
        "--hf-bucket",
        default=None,
        help="HF bucket ID for migration (e.g. username/my-bucket)",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        default=False,
        help="Only show cost comparison, don't offer migration",
    )

    args = parser.parse_args()
    try:
        run_analysis(args)
    except KeyboardInterrupt:
        console.print("\n[yellow]Aborted.[/]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error:[/] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

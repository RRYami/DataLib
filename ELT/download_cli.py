"""Command-line interface for downloading financial data into DataProject database."""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ELT.extract_fred import TREASURY_CONSTANT_MATURITY, FredExtractor
from ELT.save_polygon import PolygonSaver
from logger.logger import get_logger

# Setup
load_dotenv("./secret/.env")
console = Console()
logger = get_logger(__name__)


def main():
    """Main entry point for the download CLI."""
    parser = create_argument_parser()

    # Show help if no arguments
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # Route to appropriate handler
    try:
        if args.command == "price":
            download_price_data(args)
        elif args.command == "treasury":
            download_treasury_data(args)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        console.print("\n[yellow]Download interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Download failed: {e}")
        sys.exit(1)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        prog="download",
        description="Download financial data into DataProject database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Price data:
    download price AAPL 2025-01-01 2025-01-07
    download price AAPL,MSFT,GOOGL
    download price AAPL

  Company details:
    download company AAPL
    download company AAPL,MSFT,GOOGL
    download company --all

  Treasury yields (FRED):
    download treasury 2024-01-01 2024-12-31
    download treasury
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Price command
    price_parser = subparsers.add_parser("price", help="Download price data (OHLCV)")
    price_parser.add_argument(
        "tickers",
        help="Ticker symbol(s) - comma-separated, no spaces (e.g., AAPL,MSFT)",
    )
    price_parser.add_argument(
        "start_date",
        nargs="?",
        help="Start date in YYYY-MM-DD format (default: 30 days ago)",
    )
    price_parser.add_argument(
        "end_date",
        nargs="?",
        help="End date in YYYY-MM-DD format (default: today)",
    )

    # Company command
    company_parser = subparsers.add_parser(
        "company", help="Download company/ticker details"
    )
    company_parser.add_argument(
        "tickers",
        help="Ticker symbol(s) - comma-separated, or --all for all tickers in database",
    )

    # Treasury command
    treasury_parser = subparsers.add_parser(
        "treasury", help="Download treasury yields from FRED"
    )
    treasury_parser.add_argument(
        "start_date",
        nargs="?",
        help="Start date in YYYY-MM-DD format (default: 90 days ago)",
    )
    treasury_parser.add_argument(
        "end_date",
        nargs="?",
        help="End date in YYYY-MM-DD format (default: today)",
    )

    return parser


def download_price_data(args):
    """Download price data for specified tickers."""
    console.print(Panel.fit("[bold cyan]Downloading Price Data[/bold cyan]"))

    # Parse tickers
    tickers = parse_tickers(args.tickers)
    if not tickers:
        console.print("[red]Error: No valid tickers provided[/red]")
        sys.exit(1)

    # Apply date defaults and validate
    start_date = args.start_date or get_default_start_date(days=30)
    end_date = args.end_date or get_default_end_date()

    if not validate_date_format(start_date) or not validate_date_format(end_date):
        console.print("[red]Error: Invalid date format. Use YYYY-MM-DD[/red]")
        sys.exit(1)

    if not validate_date_range(start_date, end_date):
        console.print("[red]Error: Start date must be before end date[/red]")
        sys.exit(1)

    # Check for 2-year limit (Polygon free tier)
    start_dt = date.fromisoformat(start_date)
    end_dt = date.fromisoformat(end_date)
    days_diff = (end_dt - start_dt).days

    if days_diff > 730:  # ~2 years
        console.print(
            "[yellow]Warning: Date range exceeds 2 years. Polygon free tier limits apply.[/yellow]"
        )

    # Display parameters
    console.print(f"[bold]Tickers:[/bold] {', '.join(tickers)}")
    console.print(f"[bold]Date Range:[/bold] {start_date} to {end_date}")
    console.print()

    # Create saver
    try:
        saver = PolygonSaver()
    except Exception as e:
        console.print(f"[red]Error initializing saver: {e}[/red]")
        sys.exit(1)

    # Download data
    console.print("[bold]Processing...[/bold]")

    try:
        saver.save_daily_bars(tickers, start_date, end_date)
        console.print(f"[green]✓[/green] Saved daily bars for {len(tickers)} tickers")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Price data download failed: {e}")
        sys.exit(1)

    # Display summary
    console.print()
    summary_table = Table(title="Summary", show_header=False)
    summary_table.add_row("[bold]Total Tickers[/bold]", str(len(tickers)))
    summary_table.add_row("[bold]Date Range[/bold]", f"{start_date} to {end_date}")
    console.print(summary_table)


def download_treasury_data(args):
    """Download treasury yields from FRED."""
    console.print(
        Panel.fit("[bold cyan]Downloading Treasury Yields (FRED)[/bold cyan]")
    )

    # Apply date defaults and validate
    start_date = args.start_date or get_default_start_date(days=90)
    end_date = args.end_date or get_default_end_date()

    if not validate_date_format(start_date) or not validate_date_format(end_date):
        console.print("[red]Error: Invalid date format. Use YYYY-MM-DD[/red]")
        sys.exit(1)

    if not validate_date_range(start_date, end_date):
        console.print("[red]Error: Start date must be before end date[/red]")
        sys.exit(1)

    # Display parameters
    console.print(f"[bold]Date Range:[/bold] {start_date} to {end_date}")
    console.print(
        f"[bold]Series:[/bold] {', '.join(TREASURY_CONSTANT_MATURITY.keys())}"
    )
    console.print()

    # Create extractor and loader
    try:
        extractor = FredExtractor()
    except Exception as e:
        console.print(f"[red]Error initializing extractors: {e}[/red]")
        sys.exit(1)

    # Download data
    console.print("[bold]Fetching data...[/bold]")

    try:
        raw_data = extractor.get_constant_maturity_yields(
            observation_start=start_date,
            observation_end=end_date,
        )

        if raw_data is not None and not raw_data.is_empty():
            n_maturities = len(TREASURY_CONSTANT_MATURITY)
            console.print(f"  [green]✓[/green] {n_maturities} maturities fetched")
            console.print(f"  [green]✓[/green] {len(raw_data)} observations")

            # Display summary
            console.print()
            summary_table = Table(title="Summary", show_header=False)
            summary_table.add_row("[bold]Records Loaded[/bold]", str(len(raw_data)))
            summary_table.add_row(
                "[bold]Date Range[/bold]", f"{start_date} to {end_date}"
            )
            console.print(summary_table)

        else:
            console.print("[yellow]No data returned from FRED API[/yellow]")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.error(f"Treasury data download failed: {e}")
        sys.exit(1)


def parse_tickers(tickers_str: str) -> list[str]:
    """Parse comma-separated ticker string into list."""
    if not tickers_str or tickers_str == "--all":
        return []

    tickers = [t.strip().upper() for t in tickers_str.split(",")]
    return [t for t in tickers if t]  # Remove empty strings


def get_default_start_date(days: int = 30) -> str:
    """Get default start date (N days ago)."""
    return (date.today() - timedelta(days=days)).isoformat()


def get_default_end_date() -> str:
    """Get default end date (today)."""
    return date.today().isoformat()


def validate_date_format(date_str: str) -> bool:
    """Validate date string is in YYYY-MM-DD format."""
    try:
        date.fromisoformat(date_str)
        return True
    except ValueError:
        return False


def validate_date_range(start_date: str, end_date: str) -> bool:
    """Validate start date is before end date and not in future."""
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        today = date.today()

        if start > end:
            return False
        if end > today:
            console.print(
                "[yellow]Warning: End date is in the future. Using today instead.[/yellow]"
            )
            return True
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    main()

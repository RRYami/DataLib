"""Command-line interface for downloading financial data (Typer edition)."""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import typer

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import settings  # noqa: F401  triggers .env load
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ELT.extract_fred import TREASURY_CONSTANT_MATURITY, FredExtractor
from ELT.save_polygon import PolygonSaver
from ELT.save_yfinance import YFinanceSaver
from logger.logger import get_logger, setup_logging

app = typer.Typer(
    name="download",
    help="Download financial data into DataProject database",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()
logger = get_logger(__name__)


def _default_start(days: int = 30) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _default_end() -> str:
    return date.today().isoformat()


def _parse_tickers(tickers_str: str) -> list[str]:
    if not tickers_str or tickers_str == "--all":
        return []
    tickers = [t.strip().upper() for t in tickers_str.split(",")]
    return [t for t in tickers if t]


@app.callback()
def callback() -> None:
    """Setup logging before every command."""
    setup_logging()


# ─── Price command ──────────────────────────────────────────────────────────

@app.command("price")
def download_price(
    tickers: Annotated[
        str,
        typer.Argument(help="Ticker symbol(s) – comma-separated, no spaces (e.g. AAPL,MSFT)"),
    ],
    start_date: Annotated[
        str | None,
        typer.Argument(help="Start date YYYY-MM-DD (default: 30 days ago)"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Argument(help="End date YYYY-MM-DD (default: today)"),
    ] = None,
) -> None:
    """Download daily OHLCV price data."""
    console.print(Panel.fit("[bold cyan]Downloading Price Data[/bold cyan]"))

    parsed = _parse_tickers(tickers)
    if not parsed:
        console.print("[red]Error: No valid tickers provided[/red]")
        raise typer.Exit(code=1)

    start = start_date or _default_start(days=30)
    end = end_date or _default_end()

    # Quick validation
    try:
        date.fromisoformat(start)
        date.fromisoformat(end)
    except ValueError:
        console.print("[red]Error: Invalid date format. Use YYYY-MM-DD[/red]")
        raise typer.Exit(code=1)

    if date.fromisoformat(start) > date.fromisoformat(end):
        console.print("[red]Error: Start date must be before end date[/red]")
        raise typer.Exit(code=1)

    days_diff = (date.fromisoformat(end) - date.fromisoformat(start)).days
    if days_diff > 730:
        console.print(
            "[yellow]Warning: Date range exceeds 2 years. "
            "Polygon free tier limits may apply.[/yellow]"
        )

    console.print(f"[bold]Tickers:[/bold] {', '.join(parsed)}")
    console.print(f"[bold]Date Range:[/bold] {start} to {end}")
    console.print()

    saver = PolygonSaver()
    console.print("[bold]Processing...[/bold]")
    result = saver.save_daily_bars(parsed, start, end)

    if result.ok:
        console.print(f"[green]✓[/green] Saved daily bars for {len(result.saved)} tickers")
    else:
        for item, reason in result.failed:
            console.print(f"[yellow]⚠ {item} failed:[/yellow] {reason}")

    console.print()
    summary = Table(title="Summary", show_header=False)
    summary.add_row("[bold]Total Tickers[/bold]", str(len(parsed)))
    summary.add_row("[bold]Date Range[/bold]", f"{start} to {end}")
    summary.add_row("[bold]Saved[/bold]", str(len(result.saved)))
    summary.add_row("[bold]Failed[/bold]", str(len(result.failed)))
    console.print(summary)


# ─── Treasury command ───────────────────────────────────────────────────────

@app.command("treasury")
def download_treasury(
    start_date: Annotated[
        str | None,
        typer.Argument(help="Start date YYYY-MM-DD (default: 90 days ago)"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Argument(help="End date YYYY-MM-DD (default: today)"),
    ] = None,
) -> None:
    """Download Treasury yields from FRED."""
    console.print(
        Panel.fit("[bold cyan]Downloading Treasury Yields (FRED)[/bold cyan]")
    )

    start = start_date or _default_start(days=90)
    end = end_date or _default_end()

    try:
        date.fromisoformat(start)
        date.fromisoformat(end)
    except ValueError:
        console.print("[red]Error: Invalid date format. Use YYYY-MM-DD[/red]")
        raise typer.Exit(code=1)

    if date.fromisoformat(start) > date.fromisoformat(end):
        console.print("[red]Error: Start date must be before end date[/red]")
        raise typer.Exit(code=1)

    console.print(f"[bold]Date Range:[/bold] {start} to {end}")
    console.print(
        f"[bold]Series:[/bold] {', '.join(TREASURY_CONSTANT_MATURITY.keys())}"
    )
    console.print()

    extractor = FredExtractor()
    console.print("[bold]Fetching data...[/bold]")

    try:
        raw = extractor.get_constant_maturity_yields(
            observation_start=start,
            observation_end=end,
        )
        if not raw.is_empty():
            n_maturities = len(TREASURY_CONSTANT_MATURITY)
            console.print(f"  [green]✓[/green] {n_maturities} maturities fetched")
            console.print(f"  [green]✓[/green] {len(raw):,} observations")
            console.print()
            summary = Table(title="Summary", show_header=False)
            summary.add_row("[bold]Records Loaded[/bold]", str(len(raw)))
            summary.add_row("[bold]Date Range[/bold]", f"{start} to {end}")
            console.print(summary)
        else:
            console.print("[yellow]No data returned from FRED API[/yellow]")
            raise typer.Exit(code=1)
    except Exception as exc:
        console.print(f"[red]Error: {exc}[/red]")
        logger.error(f"Treasury data download failed: {exc}")
        raise typer.Exit(code=1)


# ─── Options command ────────────────────────────────────────────────────────


def _parse_expiries(expiries_str: str) -> list[str] | None:
    if not expiries_str or expiries_str.upper() == "ALL":
        return None
    return [e.strip() for e in expiries_str.split(",") if e.strip()]


@app.command("options")
def download_options(
    tickers: Annotated[
        str,
        typer.Argument(
            help="Ticker symbol(s) – comma-separated, no spaces (e.g. AAPL,MSFT). "
            "Defaults to AAPL,MSFT,NVDA,SPY."
        ),
    ] = "",
    expiries: Annotated[
        str,
        typer.Argument(
            help="Option expiry date(s) – comma-separated YYYY-MM-DD, or 'ALL' (default)"
        ),
    ] = "ALL",
) -> None:
    """Download Yahoo Finance option-chain snapshots."""
    console.print(Panel.fit("[bold cyan]Downloading Option Chain Data[/bold cyan]"))

    parsed_tickers = _parse_tickers(tickers)
    parsed_expiries = _parse_expiries(expiries)

    if tickers and not parsed_tickers:
        console.print("[red]Error: No valid tickers provided[/red]")
        raise typer.Exit(code=1)

    saver = YFinanceSaver(calls_per_minute=settings.yfinance_rate_limit)
    tickers_to_run = parsed_tickers or saver.DEFAULT_TICKERS

    console.print(f"[bold]Tickers:[/bold] {', '.join(tickers_to_run)}")
    if parsed_expiries:
        console.print(f"[bold]Expiries:[/bold] {', '.join(parsed_expiries)}")
    else:
        console.print("[bold]Expiries:[/bold] ALL available")
    console.print()

    console.print("[bold]Processing...[/bold]")
    result = saver.save_option_chains(tickers_to_run, parsed_expiries)

    if result.ok:
        console.print(
            f"[green]✓[/green] Saved option chains for {len(result.saved)} tickers"
        )
    else:
        for item, reason in result.failed:
            console.print(f"[yellow]⚠ {item} failed:[/yellow] {reason}")

    console.print()
    summary = Table(title="Summary", show_header=False)
    summary.add_row("[bold]Total Tickers[/bold]", str(len(tickers_to_run)))
    summary.add_row("[bold]Saved[/bold]", str(len(result.saved)))
    summary.add_row("[bold]Failed[/bold]", str(len(result.failed)))
    summary.add_row("[bold]Skipped[/bold]", str(len(result.skipped)))
    console.print(summary)


if __name__ == "__main__":
    app()

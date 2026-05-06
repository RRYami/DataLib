#!/usr/bin/env python3
"""Professional Parquet File Inspector CLI.

A rich, interactive command-line tool for inspecting Apache Parquet files
using Polars for blazing-fast reads and Rich for beautiful terminal output.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Annotated

import polars as pl
import typer
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ─── Configuration ───────────────────────────────────────────────────────────

DEFAULT_DATA_PATH = Path("data/parquet")
console = Console()
app = typer.Typer(
    name="parquet-view",
    help="Inspect Parquet files with style.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _resolve_path(file_name: str) -> Path:
    """Resolve a file name to an absolute path within the data directory."""
    path = DEFAULT_DATA_PATH / file_name
    if not path.exists():
        console.print(f"[red]✖ File not found:[/red] {path}")
        raise typer.Exit(code=1)
    if not path.is_file():
        console.print(f"[red]✖ Not a file:[/red] {path}")
        raise typer.Exit(code=1)
    return path


def _read_parquet(path: Path) -> pl.DataFrame:
    """Safely read a parquet file into a Polars DataFrame."""
    try:
        return pl.read_parquet(path)
    except Exception as exc:
        console.print(f"[red]✖ Failed to read parquet:[/red] {exc}")
        raise typer.Exit(code=1) from exc


def _file_size(path: Path) -> str:
    """Return a human-readable file size."""
    size = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def _modified_time(path: Path) -> str:
    """Return a formatted modified timestamp."""
    mtime = path.stat().st_mtime
    dt = datetime.datetime.fromtimestamp(mtime)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _make_rich_table(df: pl.DataFrame, title: str | None = None) -> Table:
    """Convert a Polars DataFrame to a Rich Table."""
    table = Table(
        title=title,
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
        row_styles=["", "dim"],
        expand=True,
    )
    for col in df.columns:
        table.add_column(str(col), overflow="fold")
    for row in df.iter_rows():
        table.add_row(*(str(cell) for cell in row))
    return table


# ─── Commands ─────────────────────────────────────────────────────────────────


@app.command("list")
def list_files(
    path: Annotated[
        Path | None,
        typer.Option(
            "--path",
            "-p",
            help="Directory to scan for parquet files.",
            exists=True,
            file_okay=False,
            dir_okay=True,
        ),
    ] = None,
) -> None:
    """List all parquet files with metadata."""
    target = path or DEFAULT_DATA_PATH

    if not target.exists():
        console.print(f"[red]✖ Directory not found:[/red] {target}")
        raise typer.Exit(code=1)

    files = sorted(target.rglob("*.parquet"))
    if not files:
        console.print(f"[yellow]⚠ No parquet files found in[/yellow] {target}")
        raise typer.Exit()

    table = Table(
        title=f"Parquet Files in {target}",
        box=box.ROUNDED,
        header_style="bold cyan",
        row_styles=["", "dim"],
    )
    table.add_column("#", justify="right", style="bold")
    table.add_column("Path", style="bright_green")
    table.add_column("Size", justify="right", style="yellow")
    table.add_column("Modified", style="blue")
    table.add_column("Rows", justify="right", style="magenta")
    table.add_column("Cols", justify="right", style="magenta")

    for idx, f in enumerate(files, 1):
        try:
            # Fast metadata scan without loading data into memory
            lf = pl.scan_parquet(f)
            cols = len(lf.collect_schema().names())
            count_result = lf.select(pl.len()).collect()
            assert isinstance(count_result, pl.DataFrame)
            rows = count_result.item()
        except Exception:
            rows, cols = "?", "?"

        # Show relative path so subdirectories are visible
        display_path = f.relative_to(target) if f.is_relative_to(target) else f.name

        table.add_row(
            str(idx),
            str(display_path),
            _file_size(f),
            _modified_time(f),
            f"{rows:,}" if isinstance(rows, int) else str(rows),
            str(cols),
        )

    console.print(table)


@app.command("info")
def file_info(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
) -> None:
    """Show general file and DataFrame information."""
    path = _resolve_path(file)
    df = _read_parquet(path)
    rows, cols = df.shape

    # File metadata panel
    file_table = Table.grid(padding=1)
    file_table.add_column(style="bold cyan", justify="right")
    file_table.add_column(style="white")
    file_table.add_row("Path:", str(path.resolve()))
    file_table.add_row("Size:", _file_size(path))
    file_table.add_row("Modified:", _modified_time(path))

    # DataFrame metadata panel
    df_table = Table.grid(padding=1)
    df_table.add_column(style="bold cyan", justify="right")
    df_table.add_column(style="white")
    df_table.add_row("Rows:", f"{rows:,}")
    df_table.add_row("Columns:", str(cols))
    df_table.add_row(
        "Memory (est.):",
        f"{df.estimated_size():,} bytes ({df.estimated_size() / 1024 / 1024:.2f} MB)",
    )
    df_table.add_row("Schema Version:", "2.0" if hasattr(df, "_df") else "N/A")

    console.print(
        Columns(
            [
                Panel(
                    file_table,
                    title="[bold green]File Info[/bold green]",
                    border_style="green",
                ),
                Panel(
                    df_table,
                    title="[bold blue]DataFrame Info[/bold blue]",
                    border_style="blue",
                ),
            ],
            equal=True,
        )
    )


@app.command("schema")
def show_schema(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
) -> None:
    """Display the full column schema with types and nullability."""
    path = _resolve_path(file)
    df = _read_parquet(path)

    table = Table(
        title=f"Schema for [cyan]{file}[/cyan]",
        box=box.ROUNDED,
        header_style="bold magenta",
        row_styles=["", "dim"],
    )
    table.add_column("#", justify="right", style="bold")
    table.add_column("Column Name", style="bright_green")
    table.add_column("Data Type", style="yellow")
    table.add_column("Null Count", justify="right", style="red")
    table.add_column("Non-Null Count", justify="right", style="green")
    table.add_column("Null %", justify="right", style="dim")

    rows, _ = df.shape
    for idx, (name, dtype) in enumerate(df.schema.items(), 1):
        null_count = df[name].null_count()
        non_null = rows - null_count
        pct = (null_count / rows * 100) if rows else 0
        table.add_row(
            str(idx),
            name,
            str(dtype),
            f"{null_count:,}",
            f"{non_null:,}",
            f"{pct:.1f}%" if null_count else "[dim]0.0%[/dim]",
        )

    console.print(table)


@app.command("head")
def show_head(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
    n: Annotated[int, typer.Option("--n", "-n", help="Number of rows to show.")] = 10,
) -> None:
    """Display the first N rows."""
    path = _resolve_path(file)
    df = _read_parquet(path)
    table = _make_rich_table(
        df.head(n), title=f"Head: [cyan]{file}[/cyan] ({min(n, df.height)} rows)"
    )
    console.print(table)


@app.command("tail")
def show_tail(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
    n: Annotated[int, typer.Option("--n", "-n", help="Number of rows to show.")] = 10,
) -> None:
    """Display the last N rows."""
    path = _resolve_path(file)
    df = _read_parquet(path)
    table = _make_rich_table(
        df.tail(n), title=f"Tail: [cyan]{file}[/cyan] ({min(n, df.height)} rows)"
    )
    console.print(table)


@app.command("sample")
def show_sample(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
    n: Annotated[int, typer.Option("--n", "-n", help="Number of rows to sample.")] = 10,
    seed: Annotated[
        int | None,
        typer.Option("--seed", "-s", help="Random seed for reproducibility."),
    ] = None,
) -> None:
    """Display a random sample of N rows."""
    path = _resolve_path(file)
    df = _read_parquet(path)
    if df.height == 0:
        console.print("[yellow]⚠ DataFrame is empty.[/yellow]")
        return
    sample_df = df.sample(n=min(n, df.height), seed=seed)
    table = _make_rich_table(
        sample_df, title=f"Sample: [cyan]{file}[/cyan] ({sample_df.height} rows)"
    )
    console.print(table)


@app.command("stats")
def show_stats(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
) -> None:
    """Show column-level statistics (nulls, uniques, min/max/mean)."""
    path = _resolve_path(file)
    df = _read_parquet(path)

    table = Table(
        title=f"Column Statistics for [cyan]{file}[/cyan]",
        box=box.ROUNDED,
        header_style="bold magenta",
        row_styles=["", "dim"],
    )
    table.add_column("Column", style="bright_green")
    table.add_column("Type", style="yellow")
    table.add_column("Nulls", justify="right", style="red")
    table.add_column("Uniques", justify="right", style="cyan")
    table.add_column("Min", justify="right", style="blue")
    table.add_column("Max", justify="right", style="blue")
    table.add_column("Mean", justify="right", style="green")

    for name, dtype in df.schema.items():
        series = df[name]
        nulls = series.null_count()
        uniques = series.n_unique()

        min_val = "—"
        max_val = "—"
        mean_val = "—"

        if series.dtype.is_numeric():
            min_val = f"{series.min():,.4f}" if series.min() is not None else "—"
            max_val = f"{series.max():,.4f}" if series.max() is not None else "—"
            mean_val = f"{series.mean():,.4f}" if series.mean() is not None else "—"
        elif series.dtype in (pl.Date, pl.Datetime, pl.Duration):
            min_val = str(series.min()) if series.min() is not None else "—"
            max_val = str(series.max()) if series.max() is not None else "—"
        elif series.dtype == pl.Boolean:
            true_count = series.sum() if series.sum() is not None else 0
            mean_val = (
                f"{true_count / (len(series) - nulls) * 100:.1f}% true"
                if (len(series) - nulls) > 0
                else "—"
            )
        elif series.dtype == pl.Utf8:
            min_val = (
                f"len {series.str.len_chars().min()}"
                if series.str.len_chars().min() is not None
                else "—"
            )
            max_val = (
                f"len {series.str.len_chars().max()}"
                if series.str.len_chars().max() is not None
                else "—"
            )

        table.add_row(
            name, str(dtype), f"{nulls:,}", f"{uniques:,}", min_val, max_val, mean_val
        )

    console.print(table)


@app.command("summary")
def show_summary(
    file: Annotated[str, typer.Argument(help="Parquet file name.")],
) -> None:
    """Display a comprehensive summary of everything."""
    path = _resolve_path(file)
    df = _read_parquet(path)
    rows, cols = df.shape

    # ── Header ────────────────────────────────────────────────────────────────
    header = Text()
    header.append("📦 ", style="")
    header.append(file, style="bold cyan")
    header.append(f"  ({rows:,} rows × {cols} cols)", style="dim")
    console.print(Panel(header, border_style="bright_magenta"))

    # ── File Info ─────────────────────────────────────────────────────────────
    file_grid = Table.grid(padding=1)
    file_grid.add_column(style="bold cyan", justify="right")
    file_grid.add_column(style="white")
    file_grid.add_row("Path:", str(path.resolve()))
    file_grid.add_row("Size:", _file_size(path))
    file_grid.add_row("Modified:", _modified_time(path))
    file_grid.add_row(
        "Memory:",
        f"{df.estimated_size():,} bytes ({df.estimated_size() / 1024 / 1024:.2f} MB)",
    )

    console.print(
        Panel(file_grid, title="[bold green]File[/bold green]", border_style="green")
    )

    # ── Schema ────────────────────────────────────────────────────────────────
    schema_table = Table(
        box=box.SIMPLE_HEAD, header_style="bold magenta", show_edge=False
    )
    schema_table.add_column("Column", style="bright_green")
    schema_table.add_column("Type", style="yellow")
    schema_table.add_column("Nulls", justify="right", style="red")
    schema_table.add_column("Uniques", justify="right", style="cyan")

    for name, dtype in df.schema.items():
        nulls = df[name].null_count()
        uniques = df[name].n_unique()
        schema_table.add_row(name, str(dtype), f"{nulls:,}", f"{uniques:,}")

    console.print(
        Panel(
            schema_table,
            title="[bold blue]Schema Overview[/bold blue]",
            border_style="blue",
        )
    )

    # ── Preview ───────────────────────────────────────────────────────────────
    preview_table = _make_rich_table(df.head(10))
    console.print(
        Panel(
            preview_table,
            title="[bold yellow]First 10 Rows[/bold yellow]",
            border_style="yellow",
        )
    )

    # ── Numeric Summary ───────────────────────────────────────────────────────
    numeric_cols = [name for name, dtype in df.schema.items() if dtype.is_numeric()]
    if numeric_cols:
        num_table = Table(
            box=box.SIMPLE_HEAD, header_style="bold magenta", show_edge=False
        )
        num_table.add_column("Column", style="bright_green")
        num_table.add_column("Min", justify="right", style="blue")
        num_table.add_column("Max", justify="right", style="blue")
        num_table.add_column("Mean", justify="right", style="green")
        num_table.add_column("Std", justify="right", style="dim")

        for col in numeric_cols:
            s = df[col]
            num_table.add_row(
                col,
                f"{s.min():,.4f}" if s.min() is not None else "—",
                f"{s.max():,.4f}" if s.max() is not None else "—",
                f"{s.mean():,.4f}" if s.mean() is not None else "—",
                f"{s.std():,.4f}" if s.std() is not None else "—",
            )
        console.print(
            Panel(
                num_table,
                title="[bold magenta]Numeric Summary[/bold magenta]",
                border_style="magenta",
            )
        )


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app()

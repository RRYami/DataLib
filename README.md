# DataDownloader

A production-ready ELT (Extract-Load-Transform) pipeline for downloading, persisting, and inspecting financial market data from multiple sources — FRED, ECB, Eurostat, Polygon.io, and Alpha Vantage.

Built with **Polars** for fast DataFrame operations, **Parquet** for efficient columnar storage, and a robust architecture featuring atomic writes, automatic retries, data validation gates, and structured observability.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the Pipeline](#running-the-pipeline)
  - [CLI Tools](#cli-tools)
  - [Programmatic API](#programmatic-api)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Development](#development)
- [Scheduling](#scheduling)

---

## Features

- **Multi-source extraction**: FRED (US Treasury yields), ECB (interest rates, HICP, monetary aggregates), Eurostat (yield curves, GDP, HICP), Polygon.io (OHLCV bars, ticker details), Alpha Vantage (fundamentals, time series)
- **Incremental, idempotent updates**: Only fetches new or revised data using a configurable lookback window
- **Atomic Parquet writes**: Temp-file + `os.replace()` ensures you never get a corrupt Parquet file, even if the process crashes mid-write
- **Automatic HTTP retries**: Exponential backoff for transient failures (503, 504, 429, etc.)
- **Data validation gates**: Every DataFrame is validated before persistence (not-empty, required columns, no all-null critical columns, date range checks)
- **Graceful partial failure**: Batch operations return structured `SaveResult` objects so you know exactly which tickers/symbols succeeded or failed
- **Pipeline orchestration**: Run all sources in sequence with per-task error isolation and structured reporting
- **Data lineage sidecars**: Every Parquet file gets a `.meta.json` sidecar with schema, row count, and write timestamp
- **Failure state tracking**: JSONL-backed state file tracks `last_successful_fetch` per symbol for selective retry
- **Rich CLI tools**: Both `download` and `parquet-view` CLIs use Typer with beautiful Rich terminal output

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Extractors    │────▶│  ParquetSaver   │────▶│   Parquet Files │
│  (API clients)  │     │  (base class)   │     │  + .meta.json   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │
        ▼                       ▼
  HTTP retry logic        Validation gates
  Rate limiting           Atomic writes
                          Deduplication
                          State tracking
```

### Design Principles

1. **Single-responsibility extractors** — Each source has its own `extract_*.py` module handling authentication, rate limiting, and response parsing.
2. **Shared persistence layer** — All savers inherit from `ParquetSaver` in `ELT/base.py`, eliminating duplicated I/O and deduplication logic.
3. **Fail-safe by default** — Retries, atomic writes, validation, and per-item error isolation mean the pipeline keeps running even when individual API calls fail.
4. **Observable** — Structured logs (JSONL), sidecar metadata, and state tracking make debugging and auditing straightforward.

---

## Quick Start

### Prerequisites

- Python >= 3.13
- API keys for the sources you want to use (see [Configuration](#configuration))

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd DataDownloader

# Create virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate

# Install project + dependencies
uv pip install -e .

# Or with pip
pip install -e .
```

### First Run

```bash
# Copy the example env file and add your API keys
cp .env.example .env
# Edit .env with your keys

# Run the default pipeline (ECB + Eurostat + FRED)
python main.py
```

---

## Configuration

Configuration is managed through environment variables and a `.env` file, loaded automatically by `pydantic-settings`.

Create a `.env` file in the project root:

```bash
# API Keys (required for respective sources)
FRED_KEY=your_fred_key_here
POLYGON_KEY=your_polygon_key_here
ALPHA_VANTAGE_KEY=your_av_key_here

# Optional overrides
DATA_DIR=data/parquet          # Default data directory
LOG_LEVEL=INFO                 # DEBUG, INFO, WARNING, ERROR
DEFAULT_LOOKBACK_DAYS=7        # Default lookback for incremental updates
```

All settings are defined in `config/__init__.py` and validated at import time.

---

## Usage

### Running the Pipeline

The `main.py` entry point runs all configured sources through the `Pipeline` orchestrator:

```bash
python main.py
```

This executes:
1. **ECB** — interest rates, HICP, monetary aggregates
2. **Eurostat** — yield curves, government bond yields, HICP, GDP
3. **FRED** — Treasury constant maturity, zero-coupon yields, forward rates, term premiums

Exit code is `0` on full success, `1` if any task failed (ideal for cron).

### CLI Tools

#### `download` — Fetch data on demand

```bash
# Download daily OHLCV bars for tickers
python -m ELT.download_cli price AAPL,MSFT,GOOGL 2024-01-01 2024-12-31

# Download Treasury yields from FRED
python -m ELT.download_cli treasury 2024-01-01 2024-12-31

# Show help
python -m ELT.download_cli --help
```

#### `parquet-view` — Inspect Parquet files

```bash
# List all parquet files with metadata
python -m parquet_view list

# Show schema
python -m parquet_view schema interest_rates.parquet

# Show first/last/sample rows
python -m parquet_view head interest_rates.parquet --n 5
python -m parquet_view tail interest_rates.parquet --n 5
python -m parquet_view sample interest_rates.parquet --n 5

# Column statistics
python -m parquet_view stats interest_rates.parquet

# Full summary
python -m parquet_view summary interest_rates.parquet
```

### Programmatic API

```python
from ELT.save_fred import FredSaver
from ELT.save_ecb import EcbSaver
from orchestrator import Pipeline

# Run a single source
saver = FredSaver()
saver.save_all(lookback_days=7)

# Or use the pipeline
pipeline = Pipeline()
pipeline.add_task("fred", lambda: FredSaver().save_all())
pipeline.add_task("ecb", lambda: EcbSaver().save_all())
result = pipeline.run()

print(result.to_dict())
# {
#   "ok": True,
#   "tasks_run": 2,
#   "tasks_failed": 0,
#   "duration_seconds": 12.34,
#   "task_results": { ... }
# }
```

---

## Data Sources

| Source | Data | File Pattern | Update Frequency |
|--------|------|-------------|------------------|
| **FRED** | US Treasury yields (constant maturity, zero-coupon, forwards, term premiums) | `data/parquet/yield_curve_usa/*.parquet` | Daily |
| **ECB** | Interest rates, HICP, M1/M3 | `data/parquet/ecb/*.parquet` | Monthly |
| **Eurostat** | Euro area yields, gov bond yields, HICP, GDP | `data/parquet/eurostat/**/*.parquet` | Monthly/Quarterly |
| **Polygon** | Daily OHLCV bars, ticker details | `data/parquet/daily_bars/{TICKER}.parquet` | Daily |
| **Alpha Vantage** | Income statements, balance sheets, cash flow, earnings, overview | `data/parquet/{statement}/{TICKER}.parquet` | Quarterly |

---

## Project Structure

```
DataDownloader/
├── config/                 # Centralized pydantic-settings configuration
│   └── __init__.py
├── ELT/
│   ├── base.py             # ParquetSaver base class (I/O, dedupe, validation)
│   ├── extract_*.py        # One extractor per data source (5 files)
│   ├── save_*.py           # One saver per data source (5 files)
│   ├── download_cli.py     # Typer CLI for on-demand downloads
│   └── __init__.py
├── logger/
│   └── logger.py           # Structured JSON logging configuration
├── log_config/
│   └── config.json         # Logging dictConfig
├── utils/
│   ├── http.py             # Retry session builder + generic retry_call
│   ├── validators.py       # Data validation suite
│   ├── results.py          # SaveResult dataclass
│   └── state.py            # JSONL failure state tracker
├── tests/                  # pytest suite
│   ├── conftest.py         # Shared fixtures
│   ├── test_base.py        # Unit tests for base logic
│   ├── test_extractors.py  # Mocked API tests
│   ├── test_savers.py      # Integration tests
│   └── test_orchestrator.py # Pipeline + state tests
├── orchestrator.py         # Pipeline runner
├── main.py                 # Default entry point
├── parquet_view.py         # Parquet inspection CLI
├── get_api_keys.py         # API key resolution
├── pyproject.toml          # Project metadata + dependencies
└── README.md               # This file
```

---

## Development

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=ELT --cov=utils --cov=orchestrator
```

### Linting

```bash
# Check all files
python -m ruff check .

# Auto-fix issues
python -m ruff check --fix .
```

### Adding a New Data Source

1. Create `ELT/extract_{source}.py` — implement the API client with rate limiting
2. Create `ELT/save_{source}.py` — inherit from `ParquetSaver`, implement `save_*` methods
3. Add tests in `tests/test_extractors.py` and `tests/test_savers.py`
4. Register in `main.py` or `orchestrator.py`

---

## Scheduling

The pipeline is designed to run unattended via cron or systemd timers.

### Cron Example

```cron
# Run daily at 6:00 AM
0 6 * * * cd /path/to/DataDownloader && /path/to/.venv/bin/python main.py >> /var/log/datadownloader.log 2>&1
```

### Systemd Timer Example

Create `/etc/systemd/system/datadownloader.service`:

```ini
[Unit]
Description=DataDownloader ELT Pipeline
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/path/to/DataDownloader
ExecStart=/path/to/.venv/bin/python main.py
User=datadownloader
```

Create `/etc/systemd/system/datadownloader.timer`:

```ini
[Unit]
Description=Run DataDownloader daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now datadownloader.timer
```

### Monitoring

- Check exit code (`0` = success, `1` = partial failure)
- Inspect `logs/app.log.jsonl` for structured JSON logs
- Check `data/state/fetch_state.jsonl` for per-symbol failure tracking
- Look at `.parquet.meta.json` sidecars for data lineage

---

## License

MIT

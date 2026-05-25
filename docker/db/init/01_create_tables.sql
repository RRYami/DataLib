-- ============================================================
-- Financial Data Schema with TimescaleDB Hypertables
-- DataDownloader ELT Pipeline
-- ============================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- 1. FRED Yield Curves (long format)
-- ============================================================
CREATE TABLE IF NOT EXISTS fred_yield_curves (
    date                    DATE NOT NULL,
    tenor                   VARCHAR(10) NOT NULL,
    value                   DOUBLE PRECISION,
    fred_series_id          VARCHAR(20) NOT NULL,
    component               VARCHAR(50),
    source                  VARCHAR(50) DEFAULT 'FRED',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, fred_series_id)
);

-- Convert to hypertable for time-series performance
SELECT create_hypertable(
    'fred_yield_curves',
    'date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ============================================================
-- 2. ECB Key Interest Rates
-- ============================================================
CREATE TABLE IF NOT EXISTS ecb_key_rates (
    date                    DATE NOT NULL,
    value                   DOUBLE PRECISION,
    series_label            VARCHAR(50),
    series_id               VARCHAR(100) NOT NULL,
    source                  VARCHAR(50) DEFAULT 'ECB',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, series_id)
);

SELECT create_hypertable(
    'ecb_key_rates',
    'date',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- 3. ECB HICP (Inflation)
-- ============================================================
CREATE TABLE IF NOT EXISTS ecb_hicp (
    date                    DATE NOT NULL,
    value                   DOUBLE PRECISION,
    series_label            VARCHAR(50),
    series_id               VARCHAR(100) NOT NULL,
    source                  VARCHAR(50) DEFAULT 'ECB',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, series_id)
);

SELECT create_hypertable(
    'ecb_hicp',
    'date',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- 4. ECB Monetary Aggregates
-- ============================================================
CREATE TABLE IF NOT EXISTS ecb_monetary_aggregates (
    date                    DATE NOT NULL,
    value                   DOUBLE PRECISION,
    series_label            VARCHAR(50),
    series_id               VARCHAR(100) NOT NULL,
    source                  VARCHAR(50) DEFAULT 'ECB',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, series_id)
);

SELECT create_hypertable(
    'ecb_monetary_aggregates',
    'date',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- 5. Eurostat Yield Curve
-- ============================================================
CREATE TABLE IF NOT EXISTS eurostat_yield_curve (
    date                    DATE NOT NULL,
    maturity                VARCHAR(10) NOT NULL,
    value                   DOUBLE PRECISION,
    curve_type              VARCHAR(20),
    bond_type               VARCHAR(20),
    country                 VARCHAR(10),
    indicator               VARCHAR(50),
    source                  VARCHAR(50) DEFAULT 'EUROSTAT',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, maturity, curve_type)
);

SELECT create_hypertable(
    'eurostat_yield_curve',
    'date',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- 6. Eurostat HICP
-- ============================================================
CREATE TABLE IF NOT EXISTS eurostat_hicp (
    date                    DATE NOT NULL,
    country                 VARCHAR(10) NOT NULL,
    value                   DOUBLE PRECISION,
    item                    VARCHAR(50),
    indicator               VARCHAR(50),
    source                  VARCHAR(50) DEFAULT 'EUROSTAT',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, country, item)
);

SELECT create_hypertable(
    'eurostat_hicp',
    'date',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- 7. Eurostat GDP
-- ============================================================
CREATE TABLE IF NOT EXISTS eurostat_gdp (
    date                    DATE NOT NULL,
    country                 VARCHAR(10) NOT NULL,
    value                   DOUBLE PRECISION,
    indicator               VARCHAR(50),
    source                  VARCHAR(50) DEFAULT 'EUROSTAT',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, country)
);

SELECT create_hypertable(
    'eurostat_gdp',
    'date',
    chunk_time_interval => INTERVAL '1 year',
    if_not_exists => TRUE
);

-- ============================================================
-- 8. Polygon Market Data Daily Bars
-- ============================================================
CREATE TABLE IF NOT EXISTS market_data_daily (
    ticker                  VARCHAR(20) NOT NULL,
    date                    DATE NOT NULL,
    open                    DOUBLE PRECISION,
    high                    DOUBLE PRECISION,
    low                     DOUBLE PRECISION,
    close                   DOUBLE PRECISION,
    volume                  DOUBLE PRECISION,
    vwap                    DOUBLE PRECISION,
    transactions            BIGINT,
    source                  VARCHAR(50) DEFAULT 'POLYGON',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, date)
);

SELECT create_hypertable(
    'market_data_daily',
    'date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ============================================================
-- 9. Alpha Vantage Fundamentals: Income Statement
-- Regular PostgreSQL table (not time-series)
-- ============================================================
CREATE TABLE IF NOT EXISTS fundamentals_income_stmt (
    ticker                  VARCHAR(20) NOT NULL,
    fiscal_date_ending      DATE NOT NULL,
    report_type             VARCHAR(20) NOT NULL,
    reported_currency       VARCHAR(10),
    gross_profit            DOUBLE PRECISION,
    total_revenue           DOUBLE PRECISION,
    net_income              DOUBLE PRECISION,
    ebitda                  DOUBLE PRECISION,
    source                  VARCHAR(50) DEFAULT 'ALPHA_VANTAGE',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, fiscal_date_ending, report_type)
);

-- ============================================================
-- 10. Alpha Vantage Fundamentals: Company Overview
-- Regular PostgreSQL table (not time-series)
-- ============================================================
CREATE TABLE IF NOT EXISTS fundamentals_overview (
    ticker                  VARCHAR(20) NOT NULL PRIMARY KEY,
    asset_type              VARCHAR(50),
    name                    VARCHAR(255),
    description             TEXT,
    cik                     VARCHAR(20),
    exchange                VARCHAR(20),
    currency                VARCHAR(10),
    country                 VARCHAR(50),
    sector                  VARCHAR(100),
    industry                VARCHAR(100),
    market_capitalization   BIGINT,
    source                  VARCHAR(50) DEFAULT 'ALPHA_VANTAGE',
    last_fetched_at         TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Verification: list all hypertables
-- ============================================================
DO $$
DECLARE
    ht record;
BEGIN
    RAISE NOTICE '=== Hypertables Created ===';
    FOR ht IN
        SELECT hypertable_name, chunk_time_interval
        FROM timescaledb_information.hypertables
        ORDER BY hypertable_name
    LOOP
        RAISE NOTICE '  - % (chunk: %)', ht.hypertable_name, ht.chunk_time_interval;
    END LOOP;
END $$;

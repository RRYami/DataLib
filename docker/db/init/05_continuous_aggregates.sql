-- ============================================================
-- Continuous Aggregates (Real-time Materialized Views)
-- Pre-computed rollups for common query patterns
-- Automatically refreshed by TimescaleDB background worker
-- ============================================================

-- 1. FRED Yield Curves: Monthly average by component
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_fred_yield_monthly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', date) AS bucket,
    component,
    tenor,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS obs_count
FROM fred_yield_curves
GROUP BY bucket, component, tenor
WITH NO DATA;

-- Refresh policy: every 1 day
SELECT add_continuous_aggregate_policy(
    'cagg_fred_yield_monthly',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 2. Market Data: Monthly OHLCV per ticker
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_market_monthly_ohlcv
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', date) AS bucket,
    ticker,
    FIRST(open, date) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, date) AS close,
    SUM(volume) AS total_volume,
    AVG(vwap) AS avg_vwap,
    COUNT(*) AS trading_days
FROM market_data_daily
GROUP BY bucket, ticker
WITH NO DATA;

-- Refresh policy: every 1 day
SELECT add_continuous_aggregate_policy(
    'cagg_market_monthly_ohlcv',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 3. ECB HICP: Year-over-year monthly comparison
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_ecb_hicp_monthly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', date) AS bucket,
    series_label,
    AVG(value) AS avg_value,
    LAST(value, date) AS latest_value,
    COUNT(*) AS obs_count
FROM ecb_hicp
GROUP BY bucket, series_label
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'cagg_ecb_hicp_monthly',
    start_offset => INTERVAL '6 months',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 4. Eurostat GDP: Quarterly aggregates by country
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_estat_gdp_quarterly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('3 months', date) AS bucket,
    country,
    AVG(value) AS avg_gdp,
    LAST(value, date) AS latest_gdp,
    COUNT(*) AS obs_count
FROM eurostat_gdp
GROUP BY bucket, country
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'cagg_estat_gdp_quarterly',
    start_offset => INTERVAL '2 years',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 week',
    if_not_exists => TRUE
);

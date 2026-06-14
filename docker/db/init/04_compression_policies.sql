-- ============================================================
-- Compression Policies
-- Compress old chunks to save ~90% storage on time-series data
-- Uses TimescaleDB native compression
-- ============================================================

-- Enable compression on all hypertables with segmentby for efficient queries

-- FRED yield curves: compress chunks older than 6 months
ALTER TABLE fred_yield_curves SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'component, fred_series_id'
);

SELECT add_compression_policy(
    'fred_yield_curves',
    INTERVAL '6 months',
    if_not_exists => TRUE
);

-- Market data daily: compress chunks older than 3 months
ALTER TABLE market_data_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ticker'
);

SELECT add_compression_policy(
    'market_data_daily',
    INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ECB tables: compress chunks older than 1 year
ALTER TABLE ecb_key_rates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_label'
);

SELECT add_compression_policy(
    'ecb_key_rates',
    INTERVAL '1 year',
    if_not_exists => TRUE
);

ALTER TABLE ecb_hicp SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_label'
);

SELECT add_compression_policy(
    'ecb_hicp',
    INTERVAL '1 year',
    if_not_exists => TRUE
);

ALTER TABLE ecb_monetary_aggregates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_label'
);

SELECT add_compression_policy(
    'ecb_monetary_aggregates',
    INTERVAL '1 year',
    if_not_exists => TRUE
);

-- Eurostat tables: compress chunks older than 1 year
ALTER TABLE eurostat_yield_curve SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'maturity, curve_type'
);

SELECT add_compression_policy(
    'eurostat_yield_curve',
    INTERVAL '1 year',
    if_not_exists => TRUE
);

ALTER TABLE eurostat_hicp SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'country, item'
);

SELECT add_compression_policy(
    'eurostat_hicp',
    INTERVAL '1 year',
    if_not_exists => TRUE
);

ALTER TABLE eurostat_gdp SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'country'
);

SELECT add_compression_policy(
    'eurostat_gdp',
    INTERVAL '1 year',
    if_not_exists => TRUE
);

-- Yahoo Finance option-chain snapshots: compress chunks older than 7 days
-- (option chains refresh intraday, so daily chunks are a natural unit and
--  compressing older ones is a big win — same contract repeats across days).
ALTER TABLE yfinance_options SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'underlying, type',
    timescaledb.compress_orderby = 'snapshot_ts DESC'
);

SELECT add_compression_policy(
    'yfinance_options',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

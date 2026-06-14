-- ============================================================
-- Performance Indexes for TimescaleDB Hypertables
-- Automatically applied to all chunks
-- ============================================================

-- FRED yield curves indexes
CREATE INDEX IF NOT EXISTS idx_fred_component ON fred_yield_curves(component);
CREATE INDEX IF NOT EXISTS idx_fred_tenor ON fred_yield_curves(tenor);

-- ECB indexes
CREATE INDEX IF NOT EXISTS idx_ecb_rates_label ON ecb_key_rates(series_label);
CREATE INDEX IF NOT EXISTS idx_ecb_hicp_label ON ecb_hicp(series_label);
CREATE INDEX IF NOT EXISTS idx_ecb_money_label ON ecb_monetary_aggregates(series_label);

-- Eurostat indexes
CREATE INDEX IF NOT EXISTS idx_estat_yc_maturity ON eurostat_yield_curve(maturity);
CREATE INDEX IF NOT EXISTS idx_estat_yc_curve_type ON eurostat_yield_curve(curve_type);
CREATE INDEX IF NOT EXISTS idx_estat_hicp_country ON eurostat_hicp(country);
CREATE INDEX IF NOT EXISTS idx_estat_gdp_country ON eurostat_gdp(country);

-- Market data indexes
CREATE INDEX IF NOT EXISTS idx_market_ticker ON market_data_daily(ticker);

-- Audit trail indexes (useful for incremental syncs)
CREATE INDEX IF NOT EXISTS idx_fred_fetched ON fred_yield_curves(last_fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_fetched ON market_data_daily(last_fetched_at DESC);

-- Yahoo Finance option-chain indexes
CREATE INDEX IF NOT EXISTS idx_yfopt_underlying        ON yfinance_options(underlying);
CREATE INDEX IF NOT EXISTS idx_yfopt_expiry            ON yfinance_options(expiry);
CREATE INDEX IF NOT EXISTS idx_yfopt_contract          ON yfinance_options(contract_symbol);
CREATE INDEX IF NOT EXISTS idx_yfopt_underlying_type   ON yfinance_options(underlying, type);
CREATE INDEX IF NOT EXISTS idx_yfopt_snapshot_date     ON yfinance_options(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_yfopt_fetched           ON yfinance_options(last_fetched_at DESC);

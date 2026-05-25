-- ============================================================
-- Data Retention Policies
-- Automatically drops old chunks based on age
-- ============================================================

-- Market data (Polygon daily bars): Keep 2 years, drop older
SELECT add_retention_policy(
    'market_data_daily',
    INTERVAL '2 years',
    if_not_exists => TRUE
);

-- FRED yield curves: Keep 10 years (macro data is valuable long-term)
SELECT add_retention_policy(
    'fred_yield_curves',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

-- ECB data: Keep 10 years
SELECT add_retention_policy(
    'ecb_key_rates',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'ecb_hicp',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'ecb_monetary_aggregates',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

-- Eurostat: Keep 10 years
SELECT add_retention_policy(
    'eurostat_yield_curve',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'eurostat_hicp',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'eurostat_gdp',
    INTERVAL '10 years',
    if_not_exists => TRUE
);

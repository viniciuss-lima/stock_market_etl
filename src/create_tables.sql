-- DROP TABLE IF EXISTS dim_date CASCADE;
-- DROP TABLE IF EXISTS dim_secutity CASCADE;
-- DROP TABLE IF EXISTS fact_market_data;

CREATE TABLE IF NOT EXISTS dim_date(
    date_key INTEGER PRIMARY KEY,
    date DATE,
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT,
    month_name VARCHAR(20),
    day_of_month SMALLINT,
    day_of_week SMALLINT,
    day_name VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_security(
    ticker VARCHAR(10) PRIMARY KEY,
    security_name VARCHAR(255),
    primary_exchange VARCHAR(10),
    security_type VARCHAR(10),
    country VARCHAR(100),
    ipo_year SMALLINT,
    industry VARCHAR(100),
    sector VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_market_data(
    ticker VARCHAR(10) REFERENCES dim_security(ticker),
    date_key INTEGER REFERENCES dim_date(date_key),

    volume BIGINT,
    volume_weighted NUMERIC(18,4),
    transactions INTEGER,

    open_price NUMERIC(12,4),
    close_price NUMERIC(12,4),
    highest_price NUMERIC(12,4),
    lowest_price NUMERIC(12,4),
    
    market_cap BIGINT,

    daily_return NUMERIC(12,4),
    daily_return_pct NUMERIC(8,4),
    price_range NUMERIC(12,4),

    volume_per_transactions NUMERIC(18,4),
    financial_volume NUMERIC(20,2),

    is_positive_day BOOLEAN,

    PRIMARY KEY (ticker, date_key)
);
-- KIE Square Data Engineering Assignment
-- Schema: Star schema for e-commerce event analysis
-- Author: Jiya Sharma

-- Dimension Table 1: Date and Time
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,  -- Format: YYYYMMDDHH
    full_ts     TIMESTAMP NOT NULL,
    year        SMALLINT  NOT NULL,
    month       SMALLINT  NOT NULL,
    day         SMALLINT  NOT NULL,
    hour        SMALLINT  NOT NULL,
    day_of_week SMALLINT  NOT NULL,   -- 0=Mon, 6=Sun
    is_weekend  BOOLEAN   NOT NULL
);

-- Dimension Table 2: Products
CREATE TABLE IF NOT EXISTS dim_product (
    product_key   INTEGER PRIMARY KEY,
    product_id    BIGINT  NOT NULL,
    category_id   BIGINT,             -- NULL if not categorised
    category_code VARCHAR(200),        -- NULL in raw data for some products
    category_main VARCHAR(100),        -- derived: 'electronics' from 'electronics.smartphone'
    category_sub  VARCHAR(100),        -- derived: 'smartphone'
    brand         VARCHAR(100)         -- NULL in raw data for some products
);

-- Dimension Table 3: Users
CREATE TABLE IF NOT EXISTS dim_user (
    user_key INTEGER PRIMARY KEY,
    user_id  BIGINT NOT NULL UNIQUE
);

-- Fact Table: Events
CREATE TABLE IF NOT EXISTS fact_events (
    event_id     BIGINT  PRIMARY KEY,
    date_key     INTEGER REFERENCES dim_date(date_key),
    user_key     INTEGER REFERENCES dim_user(user_key),
    product_key  INTEGER REFERENCES dim_product(product_key),
    event_type   VARCHAR(10) NOT NULL,  -- 'view', 'cart', 'purchase'
    price        DECIMAL(10,2),          -- NULL allowed: view events often have no price
    user_session VARCHAR(100) NOT NULL,
    event_month  SMALLINT NOT NULL       -- 10=Oct, 11=Nov, for fast monthly filtering
);

-- Indexes (justify each in your A3 writeup)
CREATE INDEX IF NOT EXISTS idx_events_type    ON fact_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_session ON fact_events(user_session);
CREATE INDEX IF NOT EXISTS idx_events_user    ON fact_events(user_key);
CREATE INDEX IF NOT EXISTS idx_events_month   ON fact_events(event_month);
CREATE INDEX IF NOT EXISTS idx_events_product ON fact_events(product_key);
CREATE INDEX IF NOT EXISTS idx_product_id     ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_user_id        ON dim_user(user_id);

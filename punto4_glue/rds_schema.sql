CREATE SCHEMA IF NOT EXISTS shopstream_dwh;

SET search_path TO shopstream_dwh;

CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(100),
    avg_price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS dim_page (
    page_url VARCHAR(500) PRIMARY KEY,
    page_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT
);

CREATE TABLE IF NOT EXISTS fact_top_pages (
    date DATE,
    page_url VARCHAR(500),
    avg_time_seconds DECIMAL(8,2),
    session_count INT,
    rank INT
);

CREATE TABLE IF NOT EXISTS fact_bounce_rate (
    date DATE,
    page_type VARCHAR(50),
    bounce_rate DECIMAL(5,2),
    total_sessions INT,
    bounced_sessions INT
);

CREATE TABLE IF NOT EXISTS fact_conversion_funnel (
    date DATE,
    stage VARCHAR(50),
    user_count INT,
    conversion_rate DECIMAL(5,4)
);

CREATE TABLE IF NOT EXISTS fact_high_view_low_cart (
    date DATE,
    product_id VARCHAR(50),
    category VARCHAR(100),
    avg_price DECIMAL(10,2),
    view_count INT,
    cart_add_count INT,
    conversion_rate DECIMAL(5,4)
);

CREATE TABLE IF NOT EXISTS fact_navigation_paths (
    date DATE,
    path TEXT,
    session_count INT,
    rank INT
);

CREATE TABLE IF NOT EXISTS fact_device_country_time (
    date DATE,
    device_type VARCHAR(20),
    country VARCHAR(100),
    avg_time_seconds DECIMAL(8,2),
    session_count INT
);

CREATE TABLE IF NOT EXISTS fact_anomalies (
    date DATE,
    session_id VARCHAR(100),
    user_id VARCHAR(100),
    page_type VARCHAR(50),
    time_on_page DECIMAL(8,2),
    z_score DECIMAL(6,3),
    is_iqr_outlier BOOLEAN,
    anomaly_type VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_fact_top_pages_date ON fact_top_pages (date);
CREATE INDEX IF NOT EXISTS idx_fact_bounce_rate_date ON fact_bounce_rate (date);
CREATE INDEX IF NOT EXISTS idx_fact_conversion_funnel_date ON fact_conversion_funnel (date);
CREATE INDEX IF NOT EXISTS idx_fact_high_view_low_cart_date ON fact_high_view_low_cart (date);
CREATE INDEX IF NOT EXISTS idx_fact_navigation_paths_date ON fact_navigation_paths (date);
CREATE INDEX IF NOT EXISTS idx_fact_device_country_time_date ON fact_device_country_time (date);
CREATE INDEX IF NOT EXISTS idx_fact_anomalies_date ON fact_anomalies (date);

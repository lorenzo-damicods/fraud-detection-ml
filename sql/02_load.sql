-- 02_load.sql
-- Fraud Analytics SQL Companion Project (PostgreSQL)
-- Loads dimensions from staging and populates the partitioned fact table

SET search_path TO fraud;

-- ============================
-- 1) Load dimension: geo_area
-- ============================

INSERT INTO dim_geo_area (geo_area_name)
SELECT DISTINCT geo_area
FROM stg_transactions
WHERE geo_area IS NOT NULL
ON CONFLICT (geo_area_name) DO NOTHING;

-- ============================
-- 2) Load dimension: customer
-- ============================

INSERT INTO dim_customer (customer_id, customer_type, age)
SELECT DISTINCT customer_id, customer_type, age
FROM stg_transactions
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_id)
DO UPDATE SET
  customer_type = EXCLUDED.customer_type,
  age = EXCLUDED.age;

-- ============================
-- 3) Load fact table
-- ============================
-- Base timestamp used to convert 'time_seconds' into a real datetime.
-- Partition routing is based on transaction_date.
-- ============================

WITH base AS (
  SELECT '2025-01-01 00:00:00+00'::timestamptz AS base_ts
)
INSERT INTO fact_transactions (
  time_seconds,
  transaction_ts,
  transaction_date,
  customer_id,
  geo_area_id,
  transaction_type,
  is_3ds,
  is_tokenized,
  amount,
  v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,
  v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,
  v21,v22,v23,v24,v25,v26,v27,v28,
  class
)
SELECT
  s.time_seconds,
  (b.base_ts + make_interval(secs => s.time_seconds)) AS transaction_ts,
  ((b.base_ts + make_interval(secs => s.time_seconds))::date) AS transaction_date,
  s.customer_id,
  g.geo_area_id,
  s.transaction_type,
  (s.is_3ds = 1) AS is_3ds,
  (s.is_tokenized = 1) AS is_tokenized,
  s.amount,
  s.v1,s.v2,s.v3,s.v4,s.v5,s.v6,s.v7,s.v8,s.v9,s.v10,
  s.v11,s.v12,s.v13,s.v14,s.v15,s.v16,s.v17,s.v18,s.v19,s.v20,
  s.v21,s.v22,s.v23,s.v24,s.v25,s.v26,s.v27,s.v28,
  s.class
FROM stg_transactions s
JOIN base b ON TRUE
JOIN dim_geo_area g ON g.geo_area_name = s.geo_area
WHERE s.customer_id IS NOT NULL
  AND s.amount IS NOT NULL
  AND s.class IS NOT NULL;

-- 01_schema.sql
-- Fraud Analytics SQL Companion Project (PostgreSQL)
-- Creates schema + staging + dimensions + partitioned fact + score/alert tables

DROP SCHEMA IF EXISTS fraud CASCADE;

CREATE SCHEMA fraud;
SET search_path TO fraud;

-- =========================
-- 1) Dimension tables
-- =========================

CREATE TABLE dim_geo_area (
  geo_area_id BIGSERIAL PRIMARY KEY,
  geo_area_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_customer (
  customer_id BIGINT PRIMARY KEY,
  customer_type TEXT NOT NULL CHECK (customer_type IN ('EXPAT','LOCAL')),
  age INT NOT NULL CHECK (age BETWEEN 0 AND 120)
);

-- =========================
-- 2) Staging (raw ingest)
-- CSV order expected:
-- Time,V1..V28,Amount,Class,transaction_type,Time_days,geo_area,customer_id,customer_type,age,is_3DS,is_tokenized
-- NOTE: COPY/\copy maps by POSITION, not by header names.
-- =========================

CREATE TABLE stg_transactions (
  time_seconds DOUBLE PRECISION,
  v1  DOUBLE PRECISION, v2  DOUBLE PRECISION, v3  DOUBLE PRECISION, v4  DOUBLE PRECISION,
  v5  DOUBLE PRECISION, v6  DOUBLE PRECISION, v7  DOUBLE PRECISION, v8  DOUBLE PRECISION,
  v9  DOUBLE PRECISION, v10 DOUBLE PRECISION, v11 DOUBLE PRECISION, v12 DOUBLE PRECISION,
  v13 DOUBLE PRECISION, v14 DOUBLE PRECISION, v15 DOUBLE PRECISION, v16 DOUBLE PRECISION,
  v17 DOUBLE PRECISION, v18 DOUBLE PRECISION, v19 DOUBLE PRECISION, v20 DOUBLE PRECISION,
  v21 DOUBLE PRECISION, v22 DOUBLE PRECISION, v23 DOUBLE PRECISION, v24 DOUBLE PRECISION,
  v25 DOUBLE PRECISION, v26 DOUBLE PRECISION, v27 DOUBLE PRECISION, v28 DOUBLE PRECISION,
  amount NUMERIC(18,6),
  class SMALLINT CHECK (class IN (0,1)),
  transaction_type TEXT CHECK (transaction_type IN ('B2C','B2B')),
  time_days DOUBLE PRECISION,
  geo_area TEXT,
  customer_id BIGINT,
  customer_type TEXT,
  age INT,
  is_3ds SMALLINT CHECK (is_3ds IN (0,1)),
  is_tokenized SMALLINT CHECK (is_tokenized IN (0,1))
);

-- =========================
-- 3) Core fact table (partitioned by transaction_date)
-- IMPORTANT (Postgres rule):
-- On partitioned tables, PRIMARY KEY/UNIQUE must include the partitioning column.
-- =========================

CREATE TABLE fact_transactions (
  transaction_id BIGSERIAL,
  time_seconds DOUBLE PRECISION NOT NULL,
  transaction_ts TIMESTAMPTZ NOT NULL,
  transaction_date DATE NOT NULL,

  customer_id BIGINT NOT NULL REFERENCES dim_customer(customer_id),
  geo_area_id BIGINT NOT NULL REFERENCES dim_geo_area(geo_area_id),
  transaction_type TEXT NOT NULL CHECK (transaction_type IN ('B2C','B2B')),
  is_3ds BOOLEAN NOT NULL,
  is_tokenized BOOLEAN NOT NULL,

  amount NUMERIC(18,6) NOT NULL CHECK (amount >= 0),
  v1  DOUBLE PRECISION, v2  DOUBLE PRECISION, v3  DOUBLE PRECISION, v4  DOUBLE PRECISION,
  v5  DOUBLE PRECISION, v6  DOUBLE PRECISION, v7  DOUBLE PRECISION, v8  DOUBLE PRECISION,
  v9  DOUBLE PRECISION, v10 DOUBLE PRECISION, v11 DOUBLE PRECISION, v12 DOUBLE PRECISION,
  v13 DOUBLE PRECISION, v14 DOUBLE PRECISION, v15 DOUBLE PRECISION, v16 DOUBLE PRECISION,
  v17 DOUBLE PRECISION, v18 DOUBLE PRECISION, v19 DOUBLE PRECISION, v20 DOUBLE PRECISION,
  v21 DOUBLE PRECISION, v22 DOUBLE PRECISION, v23 DOUBLE PRECISION, v24 DOUBLE PRECISION,
  v25 DOUBLE PRECISION, v26 DOUBLE PRECISION, v27 DOUBLE PRECISION, v28 DOUBLE PRECISION,

  class SMALLINT NOT NULL CHECK (class IN (0,1)),
  is_fraud BOOLEAN GENERATED ALWAYS AS (class = 1) STORED,

  cost_if_fraud NUMERIC(18,6) GENERATED ALWAYS AS (amount * 4.19) STORED,
  flag_over_50k BOOLEAN GENERATED ALWAYS AS (amount >= 50000) STORED,

  PRIMARY KEY (transaction_id, transaction_date)
) PARTITION BY RANGE (transaction_date);

-- Partitions (base date used in load step will map into these dates)
CREATE TABLE fact_transactions_p1 PARTITION OF fact_transactions
  FOR VALUES FROM ('2025-01-01') TO ('2025-01-02');

CREATE TABLE fact_transactions_p2 PARTITION OF fact_transactions
  FOR VALUES FROM ('2025-01-02') TO ('2025-01-03');

CREATE TABLE fact_transactions_p3 PARTITION OF fact_transactions
  FOR VALUES FROM ('2025-01-03') TO ('2025-01-04');

CREATE TABLE fact_transactions_p_default PARTITION OF fact_transactions DEFAULT;

-- Indexes (analytics + monitoring)
CREATE INDEX idx_fact_ts ON fact_transactions (transaction_ts);
CREATE INDEX idx_fact_customer_ts ON fact_transactions (customer_id, transaction_ts);
CREATE INDEX idx_fact_geo_ts ON fact_transactions (geo_area_id, transaction_ts);
CREATE INDEX idx_fact_is_fraud ON fact_transactions (is_fraud);

-- =========================
-- 4) Scoring outputs
-- Must reference the composite PK (transaction_id, transaction_date)
-- =========================

CREATE TABLE model_scores (
  transaction_id BIGINT NOT NULL,
  transaction_date DATE NOT NULL,
  model_version TEXT NOT NULL,
  scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  risk_score DOUBLE PRECISION NOT NULL CHECK (risk_score >= 0 AND risk_score <= 1),
  threshold_used DOUBLE PRECISION NOT NULL CHECK (threshold_used >= 0 AND threshold_used <= 1),
  flag_xgb BOOLEAN NOT NULL,
  PRIMARY KEY (transaction_id, transaction_date, model_version, scored_at),
  FOREIGN KEY (transaction_id, transaction_date)
    REFERENCES fact_transactions(transaction_id, transaction_date)
    ON DELETE CASCADE
);

CREATE INDEX idx_scores_scored_at ON model_scores (scored_at);
CREATE INDEX idx_scores_flag ON model_scores (flag_xgb);

CREATE TABLE anomaly_scores (
  transaction_id BIGINT NOT NULL,
  transaction_date DATE NOT NULL,
  model_version TEXT NOT NULL,
  scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  iforest_score DOUBLE PRECISION,
  iforest_flag BOOLEAN,
  lof_flag BOOLEAN,
  PRIMARY KEY (transaction_id, transaction_date, model_version, scored_at),
  FOREIGN KEY (transaction_id, transaction_date)
    REFERENCES fact_transactions(transaction_id, transaction_date)
    ON DELETE CASCADE
);

-- =========================
-- 5) Alerts table
-- Must reference the composite PK (transaction_id, transaction_date)
-- =========================

CREATE TABLE alerts (
  alert_id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  transaction_id BIGINT NOT NULL,
  transaction_date DATE NOT NULL,
  alert_type TEXT NOT NULL,
  severity SMALLINT NOT NULL CHECK (severity BETWEEN 1 AND 5),
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  FOREIGN KEY (transaction_id, transaction_date)
    REFERENCES fact_transactions(transaction_id, transaction_date)
    ON DELETE CASCADE
);

CREATE INDEX idx_alerts_created_at ON alerts (created_at);
CREATE INDEX idx_alerts_type ON alerts (alert_type);

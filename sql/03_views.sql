-- 03_views.sql
-- Fraud Analytics SQL Companion Project (PostgreSQL)
-- Analytics views + monitoring + expected loss + helper views

SET search_path TO fraud;

-- ============================
-- 1) Fraud rate by geography
-- ============================

CREATE OR REPLACE VIEW v_fraud_rate_by_geo AS
SELECT
  g.geo_area_name,
  COUNT(*) AS n_tx,
  SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS n_fraud,
  ROUND(100.0 * AVG(CASE WHEN f.is_fraud THEN 1 ELSE 0 END), 4) AS fraud_rate_pct,
  ROUND(SUM(f.amount), 2) AS total_amount,
  ROUND(SUM(CASE WHEN f.is_fraud THEN f.amount ELSE 0 END), 2) AS fraud_amount
FROM fact_transactions f
JOIN dim_geo_area g ON g.geo_area_id = f.geo_area_id
GROUP BY g.geo_area_name
ORDER BY fraud_rate_pct DESC, n_tx DESC;

-- ==========================================
-- 2) Fraud rate by controls (3DS/token/type)
-- ==========================================

CREATE OR REPLACE VIEW v_fraud_rate_by_controls AS
SELECT
  transaction_type,
  is_3ds,
  is_tokenized,
  COUNT(*) AS n_tx,
  SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS n_fraud,
  ROUND(100.0 * AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END), 4) AS fraud_rate_pct,
  ROUND(SUM(amount), 2) AS total_amount,
  ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount
FROM fact_transactions
GROUP BY transaction_type, is_3ds, is_tokenized
ORDER BY fraud_rate_pct DESC, n_tx DESC;

-- ============================
-- 3) Daily monitoring view
-- ============================

CREATE OR REPLACE VIEW v_daily_monitoring AS
SELECT
  transaction_date,
  COUNT(*) AS n_tx,
  SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS n_fraud,
  ROUND(100.0 * AVG(CASE WHEN is_fraud THEN 1 ELSE 0 END), 4) AS fraud_rate_pct,
  ROUND(SUM(amount), 2) AS total_amount,
  ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount,
  ROUND(AVG(amount), 2) AS avg_amount,
  ROUND(AVG(CASE WHEN is_fraud THEN amount ELSE NULL END), 2) AS avg_amount_fraud
FROM fact_transactions
GROUP BY transaction_date
ORDER BY transaction_date;

-- ======================================================
-- 4) Customer risk profile (fraud rate & high-amount flag)
-- ======================================================

CREATE OR REPLACE VIEW v_customer_risk_profile AS
SELECT
  c.customer_id,
  c.customer_type,
  c.age,
  COUNT(*) AS n_tx,
  SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS n_fraud,
  ROUND(100.0 * AVG(CASE WHEN f.is_fraud THEN 1 ELSE 0 END), 4) AS fraud_rate_pct,
  ROUND(SUM(f.amount), 2) AS total_amount,
  ROUND(MAX(f.amount), 2) AS max_amount,
  SUM(CASE WHEN f.flag_over_50k THEN 1 ELSE 0 END) AS n_over_50k
FROM fact_transactions f
JOIN dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.customer_type, c.age
ORDER BY fraud_rate_pct DESC, n_tx DESC;

-- ==========================================
-- 5) Latest score per transaction helper view
-- ==========================================
-- Uses LATERAL to pick the latest score record per transaction
-- (based on scored_at). This is used by v_expected_loss.

CREATE OR REPLACE VIEW v_latest_model_score AS
SELECT
  f.transaction_id,
  f.transaction_date,
  s.model_version,
  s.scored_at,
  s.risk_score,
  s.threshold_used,
  s.flag_xgb
FROM fact_transactions f
JOIN LATERAL (
  SELECT *
  FROM model_scores ms
  WHERE ms.transaction_id = f.transaction_id
    AND ms.transaction_date = f.transaction_date
  ORDER BY ms.scored_at DESC
  LIMIT 1
) s ON TRUE;

-- ==========================================
-- 6) Expected loss view (cost-aware scoring)
-- ==========================================

CREATE OR REPLACE VIEW v_expected_loss AS
SELECT
  f.transaction_id,
  f.transaction_date,
  f.transaction_ts,
  f.customer_id,
  g.geo_area_name,
  f.transaction_type,
  f.is_3ds,
  f.is_tokenized,
  f.amount,
  f.cost_if_fraud,
  ls.model_version,
  ls.scored_at,
  ls.risk_score,
  (ls.risk_score * f.cost_if_fraud) AS expected_loss_aed,
  ls.threshold_used,
  ls.flag_xgb
FROM fact_transactions f
JOIN dim_geo_area g ON g.geo_area_id = f.geo_area_id
JOIN v_latest_model_score ls
  ON ls.transaction_id = f.transaction_id
 AND ls.transaction_date = f.transaction_date;

-- ==========================================
-- 7) Investigation queue (top expected loss)
-- ==========================================

CREATE OR REPLACE VIEW v_investigation_queue AS
SELECT
  transaction_id,
  transaction_date,
  transaction_ts,
  customer_id,
  geo_area_name,
  transaction_type,
  is_3ds,
  is_tokenized,
  amount,
  risk_score,
  expected_loss_aed,
  model_version,
  scored_at
FROM v_expected_loss
ORDER BY expected_loss_aed DESC, risk_score DESC
LIMIT 500;

-- ==========================================
-- 8) Sanity / quick checks (optional queries)
-- ==========================================
-- Uncomment to run manually:
-- SELECT * FROM v_fraud_rate_by_geo LIMIT 10;
-- SELECT * FROM v_fraud_rate_by_controls LIMIT 20;
-- SELECT * FROM v_daily_monitoring;
-- SELECT * FROM v_customer_risk_profile LIMIT 50;
-- SELECT * FROM v_investigation_queue LIMIT 50;

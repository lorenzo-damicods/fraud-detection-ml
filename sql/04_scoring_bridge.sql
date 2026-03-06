SET search_path TO fraud;

CREATE OR REPLACE VIEW v_scoring_bridge AS
SELECT
  transaction_id,
  transaction_date,
  time_seconds,
  customer_id,
  amount,
  transaction_type,
  geo_area_id
FROM fact_transactions;

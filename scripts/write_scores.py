import os
import joblib
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ============================================================
# Paths
# ============================================================
BASE = "/home/lorenzods/Scrivania/Fraud detection/SQL_FRAUD_PROJECT"

CSV_PATH      = os.getenv("CSV_PATH",      "/home/lorenzods/Scrivania/Fraud detection/SQL_FRAUD_PROJECT/dataset/creditcard_enhanced_UAE_FINAL.csv")
MODEL_PATH    = os.getenv("MODEL_PATH",    os.path.join(BASE, "xgb_v4.pkl"))
SCALER_PATH   = os.getenv("SCALER_PATH",   os.path.join(BASE, "scaler_v4.pkl"))
COLUMNS_PATH  = os.getenv("COLUMNS_PATH",  os.path.join(BASE, "train_columns_v4.pkl"))

THRESHOLD     = float(os.getenv("THRESHOLD",    "0.1621"))
MODEL_VERSION = os.getenv("MODEL_VERSION",       "xgb_v4")

# ============================================================
# 1. Load model, scaler and feature columns
# ============================================================
print("Loading model, scaler, columns...")
model         = joblib.load(MODEL_PATH)
scaler        = joblib.load(SCALER_PATH)
train_columns = joblib.load(COLUMNS_PATH)
print("  OK")

# ============================================================
# 2. Load dataset
# ============================================================
print("Loading dataset...")
df = pd.read_csv(CSV_PATH)
print(f"  Rows loaded: {len(df)}")

# ============================================================
# 3. Preprocessing (identical to v4 notebook)
# ============================================================
binary_mapping = {
    "transaction_type": {"B2C": 0, "B2B": 1},
    "customer_type":    {"EXPAT": 0, "LOCAL": 1},
}

drop_cols = ["Class", "cost_if_fraud", "flag_over_50k", "customer_id", "Time"]
X_raw = df.drop(columns=[c for c in drop_cols if c in df.columns])

# Binary encoding
for col, mapping in binary_mapping.items():
    if col in X_raw.columns:
        X_raw[col] = X_raw[col].map(mapping)

# One-hot encoding for geo_area
if "geo_area" in X_raw.columns:
    X_raw = pd.get_dummies(X_raw, columns=["geo_area"])

# Align columns to training set (same order and names)
X_raw = X_raw.reindex(columns=train_columns, fill_value=0)

# Scaling
X_scaled = scaler.transform(X_raw)

# ============================================================
# 4. Compute real risk scores using the v4 model
# ============================================================
print("Computing risk scores...")
df["risk_score"] = model.predict_proba(X_scaled)[:, 1]
df["flag_xgb"]   = df["risk_score"] >= THRESHOLD
print(f"  Transactions flagged as fraud: {df['flag_xgb'].sum()}")

# ============================================================
# 5. Connect to the database
# ============================================================
conn = psycopg2.connect(
    host=os.getenv("DB_HOST",     "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME",   "frauddb"),
    user=os.getenv("DB_USER",     "utente1"),
    password=os.getenv("DB_PASSWORD", "nuovapassword")
)

# ============================================================
# 6. Fetch the scoring bridge from the DB to retrieve transaction_id
# ============================================================
print("Fetching scoring bridge from database...")
bridge_query = """
SELECT transaction_id, transaction_date, time_seconds, customer_id, amount
FROM fraud.v_scoring_bridge;
"""
bridge = pd.read_sql(bridge_query, conn)
print(f"  Bridge rows: {len(bridge)}")

# ============================================================
# 7. Merge dataset with bridge
# ============================================================
keys = df[["Time", "customer_id", "Amount", "risk_score", "flag_xgb"]].copy()
keys.columns = ["time_seconds", "customer_id", "amount", "risk_score", "flag_xgb"]

merged = keys.merge(
    bridge,
    on=["time_seconds", "customer_id", "amount"],
    how="inner"
)
print(f"  Rows after merge: {len(merged)}")

merged["threshold_used"] = THRESHOLD
merged["model_version"]  = MODEL_VERSION

# ============================================================
# 8. Insert into fraud.model_scores
# ============================================================
rows = list(
    merged[["transaction_id", "transaction_date", "model_version",
            "risk_score", "threshold_used", "flag_xgb"]]
    .itertuples(index=False, name=None)
)

insert_sql = """
INSERT INTO fraud.model_scores
(transaction_id, transaction_date, model_version, risk_score, threshold_used, flag_xgb)
VALUES %s;
"""

print("Inserting into database...")
with conn.cursor() as cur:
    execute_values(cur, insert_sql, rows, page_size=10000)

conn.commit()
conn.close()

print(f"Done. Rows inserted into fraud.model_scores: {len(rows)}")
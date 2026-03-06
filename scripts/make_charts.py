import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

BASE = "/home/lorenzods/Scrivania/SQL_FRAUD_PROJECT/assets"
OUT = os.path.join(BASE, "charts")
os.makedirs(OUT, exist_ok=True)

def save(fig, name):
    path = os.path.join(OUT, name)
    fig.tight_layout()
    fig.savefig(path, dpi=220)
    plt.close(fig)

def tf_flag(x):
    s = str(x).strip().lower()
    if s in ("t", "true", "1"):
        return "T"
    if s in ("f", "false", "0"):
        return "F"
    return str(x)

def fmt_millions(x, pos):
    return f"{x:.1f}M"

def fmt_thousands(x, pos):
    return f"{x:.0f}k"

# -----------------------------
# 1) Fraud rate (%) over time
# -----------------------------
dm_path = os.path.join(BASE, "daily_monitoring_full.csv")
dm = pd.read_csv(dm_path, parse_dates=["transaction_date"])
dm = dm.sort_values("transaction_date")

fig = plt.figure(figsize=(8, 5.5))
plt.plot(dm["transaction_date"], dm["fraud_rate_pct"], marker="o")
plt.title("Fraud rate (%) over time")
plt.xlabel("Date")
plt.ylabel("Fraud rate (%)")

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.DayLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
plt.xticks(rotation=25)

save(fig, "01_fraud_rate_over_time.png")

# --------------------------------------
# Load expected_loss_full once (big file)
# --------------------------------------
el_path = os.path.join(BASE, "expected_loss_full.csv")
usecols = [
    "transaction_id",
    "transaction_date",
    "geo_area_name",
    "transaction_type",
    "is_3ds",
    "is_tokenized",
    "expected_loss_aed",
]
el = pd.read_csv(el_path, usecols=usecols, parse_dates=["transaction_date"])
el["expected_loss_aed"] = pd.to_numeric(el["expected_loss_aed"], errors="coerce")
el = el.dropna(subset=["expected_loss_aed"])

# ---------------------------------
# 2) Expected loss distribution
# ---------------------------------
fig = plt.figure(figsize=(8, 5.5))
plt.hist(el["expected_loss_aed"], bins=70, log=True)
plt.title("Expected loss distribution (log count)")
plt.xlabel("Expected loss (AED)")
plt.ylabel("Count (log)")
save(fig, "02_expected_loss_distribution.png")

# ---------------------------------------------------------
# 3) Top segments by expected loss (geo/type/control flags)
#    Improvement: show in million AED (no scientific notation)
# ---------------------------------------------------------
seg = (
    el.groupby(["geo_area_name", "transaction_type", "is_3ds", "is_tokenized"], dropna=False)["expected_loss_aed"]
      .sum()
      .reset_index()
      .sort_values("expected_loss_aed", ascending=False)
      .head(15)
)

seg["segment"] = (
    seg["geo_area_name"].astype(str)
    + " | " + seg["transaction_type"].astype(str)
    + " | 3DS=" + seg["is_3ds"].map(tf_flag)
    + " | TOK=" + seg["is_tokenized"].map(tf_flag)
)

seg["expected_loss_m"] = seg["expected_loss_aed"] / 1_000_000.0

fig = plt.figure(figsize=(11, 6.5))
plt.barh(seg["segment"][::-1], seg["expected_loss_m"][::-1])
plt.title("Top segments by total expected loss")
plt.xlabel("Total expected loss (million AED)")
plt.ylabel("Segment")

ax = plt.gca()
ax.xaxis.set_major_formatter(FuncFormatter(fmt_millions))

save(fig, "03_top_segments_expected_loss.png")

# --------------------------------------------
# 4) Top 20 transactions by expected loss
#    Improvement: shorter labels + optional k-format
# --------------------------------------------
top20 = el.sort_values("expected_loss_aed", ascending=False).head(20).copy()
top20["label"] = top20["transaction_id"].astype(str) + " | " + top20["geo_area_name"].astype(str)
top20["expected_loss_k"] = top20["expected_loss_aed"] / 1_000.0

fig = plt.figure(figsize=(10, 7.5))
plt.barh(top20["label"][::-1], top20["expected_loss_k"][::-1])
plt.title("Top 20 transactions by expected loss")
plt.xlabel("Expected loss (k AED)")
plt.ylabel("Transaction")

ax = plt.gca()
ax.xaxis.set_major_formatter(FuncFormatter(fmt_thousands))

save(fig, "04_top20_expected_loss.png")

print("Done. Charts saved to:", OUT)
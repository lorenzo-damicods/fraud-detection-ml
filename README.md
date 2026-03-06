# fraud-detection-ml
XGBoost fraud detection model (threshold-tuned, cost-aware) with full SQL backend — schema, ETL, analytics views, and automated investigation queue. Built on enhanced UAE credit card dataset.


fraud-detection-uae/
│
├── README.md
├── .gitignore                  # ignora .pkl, dataset pesanti, .env
├── requirements.txt
│
├── notebooks/
│   └── fraud_detection_v4.ipynb
│
├── sql/
│   ├── 01_schema.sql
│   ├── 02_load.sql
│   ├── 03_views.sql
│   └── 04_scoring_bridge.sql
│
├── scripts/
│   ├── write_scores.py
│   └── make_charts.py
│
├── assets/
│   ├── cm_xgboost.png
│   ├── cm_comparison.png
│   ├── prob_distribution_val.png
│   ├── threshold_tuning_curve.png
│   ├── 01_fraud_rate_over_time.png
│   ├── 02_expected_loss_distribution.png
│   ├── 03_top_segments_expected_loss.png
│   └── 04_top20_expected_loss.png
│
├── data/
│   ├── investigation_queue_full.csv
│   └── daily_monitoring_full.csv
│
└── models/                     # gitignore i .pkl se > 100MB, altrimenti includi
    └── (xgb_v4.pkl, scaler_v4.pkl, train_columns_v4.pkl)

# 💎 Customer Lifetime Value & Cross-Sell Predictor

> **An industry-standard ML pipeline for Amex GBT corporate travel** — predicting Customer Lifetime Value, modeling churn risk via survival analysis, segmenting clients into actionable tiers, and recommending cross-sell products with calibrated propensity scores.

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![XGBoost](https://img.shields.io/badge/XGBoost-2.0-orange)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green?logo=fastapi)
![Streamlit](https://img.shields.io/badge/Streamlit-1.30-red?logo=streamlit)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 🏗️ Architecture

```
                    ┌──────────────────────────────────────┐
                    │         Apache Airflow (DAGs)        │
                    │   data_ingestion │ feature_engineering│
                    └────────┬────────────────┬────────────┘
                             │                │
                    ┌────────▼────────┐ ┌─────▼──────────────┐
                    │   Snowflake     │ │   Databricks       │
                    │  RAW → STAGING  │ │  Feature Eng +     │
                    │  → FEATURES     │ │  Model Training    │
                    └────────┬────────┘ └─────┬──────────────┘
                             │                │
                             └───────┬────────┘
                                     │
                    ┌────────────────▼─────────────────────┐
                    │          MLflow Tracking              │
                    │   Experiments · Models · Artifacts    │
                    └────────────────┬─────────────────────┘
                                     │
                    ┌────────────────▼─────────────────────┐
                    │     FastAPI Inference Service         │
                    │  /predict/clv  /predict/churn         │
                    │  /predict/cross-sell  /accounts/{id}  │
                    └────────────────┬─────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
     ┌────────▼────────┐  ┌──────────▼──────────┐  ┌───────▼───────┐
     │   Streamlit      │  │   Prometheus        │  │   Grafana     │
     │   Dashboard      │  │   Metrics           │  │   Dashboards  │
     └─────────────────┘  └─────────────────────┘  └───────────────┘
```

---

## 📊 Model Performance

| Model | Algorithm | Key Metric | Details |
|---|---|---|---|
| **CLV Regressor** | LightGBM | **R²=0.61** | MAE=$14.8K, 42 numeric features |
| **Churn Survival** | Cox PH | **C-Index=0.89** | Median survival 1,079 days |
| **Segmentation** | HDBSCAN+UMAP | **Silhouette=0.77** | 3 clusters (At-Risk, Low-Engagement, Growth) |
| **Cross-Sell** | XGBoost Multi-Output | **AUC=0.92+** | 4 products: Neo, Egencia, M&E, Consulting |

---

## 📁 Project Structure

```
customer_lifetime_value_and_cross_sell_predictor/
├── .github/workflows/ci.yml       # CI/CD: lint, test, Docker build
├── airflow/dags/                   # Orchestration DAGs
│   ├── data_ingestion_dag.py       #   Synthesis → Snowflake load
│   └── feature_engineering_dag.py  #   Feature computation pipeline
├── api/                            # FastAPI inference service
│   ├── main.py                     #   6 REST endpoints
│   ├── Dockerfile                  #   Container definition
│   └── tests/test_api.py           #   Endpoint contract tests
├── dashboard/                      # Streamlit UI
│   ├── app.py                      #   4-page dashboard
│   └── Dockerfile
├── data/
│   ├── generate_synthetic_data.py  # 5K accounts, 1.4M bookings generator
│   ├── snowflake_loader.py         # Snowflake PUT/COPY loader
│   └── schema/snowflake_ddl.sql    # RAW → STAGING → FEATURES schemas
├── features/
│   └── feature_engineering.py      # 49-feature pipeline (RFM, trajectory, etc.)
├── models/
│   ├── clv_model.py                # XGBoost + LightGBM CLV regression
│   ├── survival_model.py           # Cox PH + Kaplan-Meier churn analysis
│   ├── segmentation.py             # UMAP + HDBSCAN client clustering
│   └── cross_sell_model.py         # Multi-label product propensity
├── monitoring/prometheus.yml       # Prometheus scrape config
├── docs/model_card.md              # ML Model Card
├── docker-compose.yml              # 5-service stack
├── requirements.txt                # Pinned dependencies
└── README.md                       # This file
```

---

## 🚀 Quick Start

### 1. Setup Environment

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

pip install -r requirements.txt
```

### 2. Generate Synthetic Data

```bash
python -m data.generate_synthetic_data \
  --output-dir data/synthetic \
  --seed 42 \
  --format both
```

**Generates**: 5,000 corporate accounts, 62K travelers, 1.4M bookings, 8.2K contracts, 119K tickets.

### 3. Run Feature Engineering

```bash
python -m features.feature_engineering \
  --data-dir data/synthetic \
  --output-dir data/features \
  --format both
```

**Produces**: 5,000 accounts × 49 features (zero nulls).

### 4. Train All Models

```bash
# CLV Model (XGBoost + LightGBM)
python -m models.clv_model \
  --features data/features/account_features.parquet \
  --output-dir models/artifacts/clv

# Survival Analysis (Cox PH)
python -m models.survival_model \
  --features data/features/account_features.parquet \
  --output-dir models/artifacts/survival

# Client Segmentation (UMAP + HDBSCAN)
python -m models.segmentation \
  --features data/features/account_features.parquet \
  --output-dir models/artifacts/segmentation

# Cross-Sell Propensity (Multi-Label XGBoost)
python -m models.cross_sell_model \
  --features data/features/account_features.parquet \
  --output-dir models/artifacts/cross_sell
```

### 5. Launch API & Dashboard

```bash
# Start FastAPI
uvicorn api.main:app --host 0.0.0.0 --port 8000

# Start Streamlit (separate terminal)
streamlit run dashboard/app.py --server.port 8501
```

### 6. Docker Compose (Full Stack)

```bash
docker compose up --build
```

**Services launched**:
- **API**: http://localhost:8000 (+ docs at /docs)
- **Dashboard**: http://localhost:8501
- **MLflow**: http://localhost:5000
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health + loaded models |
| `POST` | `/predict/clv` | 12-month CLV prediction |
| `POST` | `/predict/churn` | Churn risk & survival probability |
| `POST` | `/predict/cross-sell` | Ranked product recommendations |
| `GET` | `/accounts/{id}` | Full account profile |
| `GET` | `/accounts` | List/filter accounts |
| `GET` | `/segments/summary` | Aggregate segment statistics |

**Example**:
```bash
curl -X POST http://localhost:8000/predict/clv \
  -H "Content-Type: application/json" \
  -d '{"account_id": "ACCT-00001"}'
```

---

## 📈 Dashboard Pages

1. **🏠 Portfolio Health** — KPIs (total CLV, churn rate, product penetration), CLV distribution by tier, segment pie chart, revenue-at-risk bar chart
2. **🔍 Account Explorer** — Searchable/filterable account table with detail view (CLV, churn risk, segment, behavioral metrics)
3. **🗺️ Segment Map** — Interactive UMAP scatter plot colored by segment with CLV-sized markers
4. **🛒 Cross-Sell Matrix** — Product propensity heatmap across top accounts, segment-level recommendation summaries

---

## 🧪 Testing

```bash
# Unit tests
pytest api/tests/ -v

# Linting
ruff check .
ruff format --check .
```

---

## ☁️ Cloud Integration

| Service | Purpose | Status |
|---|---|---|
| **Snowflake** | Feature store (RAW → STAGING → FEATURES schemas) | DDL ready, loader built |
| **Databricks** | Feature engineering + model training (MLflow) | Notebook-compatible scripts |
| **Azure ML** | Survival analysis (Cox PH) | Script ready for Azure submission |

---

## 📋 Tech Stack Alignment (Amex GBT)

This project mirrors Amex GBT's known engineering practices:

- **Workflow orchestration**: Apache Airflow (Amex GBT uses Airflow for ETL/ELT)
- **Model serving**: FastAPI + Docker (simulates Amex GBT's migration from SageMaker → EKS)
- **Monitoring**: Prometheus + Grafana (simulates Datadog-style inference monitoring)
- **Full lifecycle ownership**: Data synthesis → feature engineering → training → serving → monitoring

---

## 📄 License

MIT

---

## 👤 Author

Built as a portfolio project demonstrating end-to-end ML pipeline design for corporate travel analytics, aligned with Amex GBT's technology stack and business domain.

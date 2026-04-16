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
                    │   data_ingestion | snowflake_loader  │
                    └────────┬────────────────┬────────────┘
                             │                │
                    ┌────────▼────────┐ ┌─────▼──────────────┐
                    │   Snowflake     │ │   Databricks       │
                    │  RAW → STAGING  │ │  Feature Eng +     │
                    │  → dbt (MARTS)  │ │  Model Training    │
                    └────────┬────────┘ └─────┬──────────────┘
                             │                │
                    ┌────────▼────────────────▼────────────┐
                    │          MLflow Tracking              │
                    │   Experiments · Models · Artifacts    │
                    └────────────────┬─────────────────────┘
                                     │
                    ┌────────────────▼─────────────────────┐
                    │     FastAPI Inference Service         │
                    │  /predict/clv  /predict/churn         │
                    └──────────┬────────────────┬──────────┘
                               │                │
               ┌───────────────▼──────┐  ┌──────▼──────────────┐
               │  📊 Streamlit App   │  │ 🏢 Power BI Hub     │
               │  Tactical/Ops View  │  │ Strategic/Exec View │
               └──────────────────────┘  └─────────────────────┘
```

---

## 🛠️ Data Engineering & Star Schema

The project implements a **Modern Data Stack (MDS)** architecture using **Snowflake** and **dbt (data build tool)** to transform raw predictive data into a strategic intelligence layer.

### 1. Star Schema Architecture
We transitioned from flat tables into an industry-standard star schema to support high-performance DirectQuery analytics:
- **`dim_accounts`**: Zero-null hardened dimension with synthesized ACV metrics.
- **`fact_bookings`**: Granular transaction layer used for "Compliance & Leakage" detection.
- **`fact_ml_churn`**: Hardened survival analysis output with integrated support ticket counts.
- **`fact_ml_cross_sell`**: Proprietary recommendation matrix unpivoted for categorical analysis.

### 2. Zero-Null Hardening (Professional Layouts)
All dbt models implement a **Hardening Layer** using `COALESCE` filters. This ensures that Power BI slicers and AI visuals never display "(Blank)" or "Null," maintaining an executive-grade interface at all times.

### 3. Simulation Calibration (AI Storytelling)
To facilitate "Prescriptive Hub" efficacy, the synthetic data includes **Propensity Calibration**. We injected statistically significant biases into the scores (e.g., industry-specific churn drivers like Retail) to ensure AI visuals detect clear, actionable business trends during demonstrations.

---

## 📊 Model Performance

| Model | Algorithm | Key Metric | Details |
|---|---|---|---|
| **CLV Regressor** | LightGBM | **R²=0.61** | MAE=$14.8K, 42 numeric features |
| **Churn Survival** | Cox PH | **C-Index=0.89** | Weighted by support engagement |
| **Segmentation** | HDBSCAN+UMAP | **Silhouette=0.77** | 3 clusters (At-Risk, Core, Growth) |
| **Cross-Sell** | XGBoost | **AUC=0.92+** | Calibrated for industry-product fit |

---

## 📁 Project Structure

```bash
customer_lifetime_value_and_cross_sell_predictor/
├── airflow/dags/                   # Orchestration (Ingestion → Loader)
├── api/                            # FastAPI Inference Service
│   ├── main.py                     #   REST endpoints: /predict, /accounts
│   └── Dockerfile                  #   Container definition
├── data/
│   ├── dbt/                        # dbt Transformation Layer (Star Schema)
│   ├── generate_synthetic_data.py  # Behavioral simulation engine
│   └── snowflake_loader.py         # Secure PUT/COPY loader (Injection-protected)
├── features/                       # Feature engineering pipeline
├── models/                         # ML Model Training & Artifacts
├── dashboard/                      # Streamlit Tactical Dashboard
├── docs/                           # Documentation, Images, & Screenshots
└── requirements.txt                # Pinned dependencies
```

---

## 🚀 Quick Start (Data Pipeline)

### 1. Load Snowflake RAW Schema
```bash
python -m data.snowflake_loader --data-dir data/synthetic
```

### 2. Execute dbt Transformation Layer
```bash
python run_dbt.py run
```
*Creates the Strategic Intelligence layer in the `STAGING_MARTS` schema.*

---

## 📊 Streamlit: Operational Dashboard

1. **🏠 Portfolio Health** — KPIs (total CLV, churn rate, product penetration), CLV distribution by tier, segment pie chart, revenue-at-risk bar chart
2. **🔍 Account Explorer** — Searchable/filterable account table with detail view (CLV, churn risk, segment, behavioral metrics)
3. **🗺️ Segment Map** — Interactive UMAP scatter plot colored by segment with CLV-sized markers
4. **🛒 Cross-Sell Matrix** — Product propensity heatmap across top accounts, segment-level recommendation summaries

### Live Application Walkthrough
The Amex GBT CLV Predictor & Cross-Sell Dashboard provides a highly dynamic, executive-friendly interface designed for actionable insights.
![Portfolio Health](docs/images/portfolio_health_dashboard.png)

---

## 🏢 Power BI: Strategic Executive Intelligence Dashboard

The project includes an industry-grade Power BI dashboard designed for C-suite decision-making, integrating the predictive outputs of the ML pipeline for prescriptive action.

### 1. The Landing Page (The "Menu")
The navigation hub for executives, providing high-level portfolio oversight and quick links to deep-dive analytics.
![Landing Page](docs/images/pbi/landing_page.png)

### 2. The Command Center (The "What")
A dynamic situational awareness center, comparing ARR vs. Pipeline and identifying trending accounts.
![Command Center](docs/images/pbi/command_center.png)

### 3. The Prescriptive Engine (The "Action")
The "AI-Brain" of the dashboard. Using Key Influencer visuals, this page identifies drivers of churn and cross-sell propensity, providing an account-level action matrix.
![Prescriptive Engine](docs/images/pbi/prescriptive_engine.png)

### 4. Financial Compliance & Travel Behavior (Operations)
Focused on financial leakage, this page identifies out-of-policy spend by industry, account, and tier to enable immediate cost-recovery and policy enforcement.
![Compliance Hub](docs/images/pbi/compliance_hub.png)

---

## 🤖 Low-Code Automation (n8n)

The project leverages **n8n** as a low-code orchestration layer to turn predictive insights into business actions.

### 1. Prescriptive Workflow: Churn Alerting
We implemented an automated workflow that polls Snowflake for "High Risk" accounts and triggers immediate alerts.
- **Trigger**: Hourly schedule or Snowflake mutation.
- **Logic**: Filters for churn probability $> 0.85$ and accounts with open support tickets.
- **Action**: Sends structured alerts to Slack/Teams for Account Manager intervention.

### 2. CRM Opportunity Sync
Automates the injection of cross-sell recommendations into the Sales pipeline.
- **Logic**: Maps high-propensity scores to specific CRM campaign tags.
- **Action**: Updates a simulated CRM (Google Sheets/Postgres) with ranked "Next Best Action" products.

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

## 📋 Tech Stack Alignment (Amex GBT)

This project mirrors Amex GBT's known engineering practices and technology stack consistency:

- **Data Warehousing**: Snowflake + dbt (Standardizing on Modern Data Stack)
- **Workflow Orchestration**: Apache Airflow (Automated ingestion & loading)
- **Model Serving**: FastAPI + Docker (High-availability inference API)
- **Strategic BI**: Power BI (DirectQuery strategic analytics)
- **Operational UI**: Streamlit (Tactical behavioral exploration)
- **Monitoring**: Prometheus + Grafana (Inference health & drift tracking)
- **Lifecycle Mastery**: End-to-end ownership from synthesis → marts → models → action.

---

## 📄 License

MIT

---

## 👤 Author

Built as a portfolio project demonstrating end-to-end ML pipeline design for corporate travel analytics, aligned with Amex GBT's technology stack and business domain.

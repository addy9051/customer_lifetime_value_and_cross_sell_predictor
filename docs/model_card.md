# CLV & Cross-Sell Predictor — Model Card

## Model Overview

| Model | Algorithm | Purpose | Key Metric |
|---|---|---|---|
| **CLV Regressor** | LightGBM (XGBoost benchmark) | Predict 12-month Customer Lifetime Value | R²=0.61, MAE=$14.8K |
| **Churn Survival** | Cox Proportional Hazards | Model customer tenure & churn probability | Concordance=0.89 |
| **Client Segmentation** | HDBSCAN + UMAP | Group clients into actionable business tiers | Silhouette=0.77 |
| **Cross-Sell Propensity** | XGBoost Multi-Output Classifier | Predict next-best-product per account | AUC-ROC=0.92+ |

---

## Intended Use

- **Primary users**: Account Managers, Revenue Operations, and Business Development teams at Amex GBT
- **Use cases**:
  - Identify high-value accounts and allocate relationship resources accordingly
  - Flag at-risk accounts for proactive retention campaigns
  - Prioritize cross-sell outreach for Neo, Egencia Analytics, Meetings & Events, and Travel Consulting
  - Forecast portfolio revenue and plan capacity

---

## Training Data

### Source
Synthetically generated corporate travel data modeled after B2B travel management patterns.

### Scale
| Entity | Records |
|---|---|
| Corporate Accounts | 5,000 |
| Traveler Profiles | 62,222 |
| Bookings | 1,454,287 |
| Service Contracts | 8,236 |
| Support Tickets | 119,291 |
| **Total** | **1,654,036** |

### Observation Window
- Historical data: January 2021 – December 2024
- Feature cutoff: January 1, 2023
- Forward label window: 12 months (Jan 2023 – Jan 2024)

### Feature Engineering
49 features across 5 groups:
- **RFM** (12 features): Booking count, spend, avg amount at 30/90/180-day windows
- **Behavioral trajectory** (5 features): Volume trend, spend acceleration, cancellation rates
- **Service adoption** (8 features): Active products, diversity score, renewal count, product flags
- **Support health** (8 features): Ticket rate, resolution time, escalation ratio, category concentration
- **Policy compliance** (5 features): Out-of-policy rates, destination diversity, lead time

---

## Model Details

### CLV Regressor (LightGBM)
- **Target**: `clv_12m` — 12-month forward revenue (log-transformed during training)
- **Split**: 70/15/15 stratified by client tier
- **Top features**: `booking_count_90d`, `total_spend_90d`, `active_contract_value`, `tier_rank`

| Metric | XGBoost | LightGBM |
|---|---|---|
| MAE | $16,582 | $14,811 |
| RMSE | $52,156 | $50,156 |
| R² | 0.5797 | **0.6113** |
| Median AE | $2,868 | $2,627 |

### Churn Survival (Cox PH)
- **Duration**: Account tenure in days
- **Event**: Binary churn indicator
- **Key hazard factors** (increasing churn risk):
  - `ticket_rate_per_month` (HR=1.35)
  - `cancellation_rate_90d` (HR=1.18)
  - `tier_rank` higher tiers associate with higher hazard in this model due to expectation mismatch
- **Protective factors** (decreasing churn risk):
  - `tenure_days` (HR=0.07) — longer-tenured clients much less likely to churn
  - `destination_diversity` (HR=0.50) — broader travel programs indicate stickiness
  - `num_active_products` (HR=0.85) — multi-product clients have lower churn

### Client Segmentation (HDBSCAN)
- **Input**: 13 value and behavioral features, standardized
- **Reduction**: UMAP (2D, 30 neighbors, min_dist=0.3)
- **Clustering**: HDBSCAN (min_cluster_size=100, min_samples=20)
- **Output**: 3 clusters + noise

| Segment | Accounts | Avg CLV | Churn Rate |
|---|---|---|---|
| At-Risk Accounts | 1,086 | $19,469 | 17% |
| Low-Engagement | 817 | $1,526 | 11% |
| Unassigned (majority) | 3,097 | $60,229 | 15% |

### Cross-Sell Propensity (XGBoost Multi-Output)
- **Target products**: Neo, Egencia Analytics, Meetings & Events, Travel Consulting
- **Class imbalance**: ~11% adoption rate per product; handled with `scale_pos_weight=5`

| Product | AUC-ROC | Avg Precision | F1 |
|---|---|---|---|
| Neo | 0.922 | 0.555 | 0.540 |
| Egencia Analytics Studio | 0.926 | 0.599 | 0.543 |
| Meetings & Events | 0.926 | 0.554 | 0.517 |
| Travel Consulting | 0.923 | 0.559 | 0.576 |

---

## Limitations

1. **Synthetic data**: All data is synthetically generated. Real-world performance may differ significantly from these metrics due to missing distributional nuances and temporal dynamics.
2. **Static features**: Features are computed at a single cutoff point. A production system would require streaming or sliding-window feature computation.
3. **CLV proxy**: The CLV label is a simplified proxy (booking revenue + contract value – support cost). Actual CLV calculations may involve margin data, opportunity costs, and discount rates.
4. **Segmentation noise**: HDBSCAN classified 62% of accounts as noise. Production tuning with domain expert input would be required to achieve full coverage.
5. **Cross-sell cold start**: New accounts with no history would require a separate onboarding model.

---

## Ethical Considerations

- **Fairness**: Segmentation and CLV predictions should not be used to systematically deprioritize small businesses. Lower-tier clients may still represent significant growth opportunities.
- **Transparency**: SHAP explanations are provided for the CLV model to ensure explainability of individual predictions. All model decisions should be auditable.
- **Privacy**: Although synthetic, the data schema mirrors PII-adjacent structures (company names, traveler roles). In production, all data handling must comply with GDPR, CCPA, and Amex GBT's internal data governance policies.
- **Human-in-the-loop**: Cross-sell recommendations should augment, not replace, account manager judgment. Recommendations should be validated with the client context before outreach.

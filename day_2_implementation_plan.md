# Production Migration Roadmap — Amex GBT Predictive Analytics

This plan outlines the next evolutionary phase of the project: migrating the current local MVP architecture (which successfully validated the data science objectives) into a fully distributed, enterprise-grade production environment capable of supporting millions of corporate accounts globally for Amex GBT.

## User Review Required

> [!IMPORTANT]
> **Strategic Checkpoint**
> This roadmap requires transitioning from local compute paradigms to distributed cloud infrastructure. Please review the architectural shifts below. If you approve, I can begin scaffolding the codebase updates necessary for **Phase 1 (Data & Feature Engineering at Scale)**.

## Phase 1: Data & Feature Engineering at Scale

Currently, feature engineering relies on in-memory Pandas and basic Airflow `BashOperators` running local scripts.

### 1. PySpark (Databricks) Migration
To handle massive data volumes spanning millions of bookings:
- **Migration**: Refactor `features/feature_engineering.py` to utilize `pyspark.sql` and the Pandas API on Spark.
- **Benefits**: Ensures the 49-feature pipeline can horizontally scale across a Databricks compute cluster without out-of-memory crashes.

### 2. dbt (Data Build Tool) Integration
Currently, our Snowflake models are built with raw Python `CREATE OR REPLACE` commands.
- **Migration**: Introduce `dbt` to manage Snowflake transformations. Create dbt models for `RAW` -> `STAGING` -> `FEATURES`.
- **Benefits**: Provides modular SQL transformations, built-in data quality testing (null checks, referential integrity), and automatic documentation.

## Phase 2: Remote Model Training & Registry

We have proven out the models locally and built a single `azure_train.py` submission script.

### 1. Databricks MLflow Integration
Currently, MLflow runs locally via Docker Compose.
- **Migration**: Re-wire `clv_model.py` and `cross_sell_model.py` to target the remote Databricks MLflow Tracking URI or Azure ML Registry.
- **Benefits**: Creates a centralized, firm-wide model registry ensuring absolute model versioning, lineage tracking, and compliance.

### 2. Automated Retraining DAGs
- **Migration**: Update `airflow/dags/data_ingestion_dag.py` to include `DatabricksSubmitRunOperator` and Azure ML Pipeline steps.
- **Benefits**: Establishes continuous training (CT) workflows that automatically trigger upon detecting data drift.

## Phase 3: High-Availability Model Serving

The current FastAPI and Streamlit apps run in local Docker Compose.

### 1. Kubernetes (AKS/EKS) Deployment
As this microservice will power real-time dashboards and CRM integrations globally:
- **Migration**: Author standard Helm charts and Kubernetes manifests (`deployment.yaml`, `service.yaml`, `ingress.yaml`) for the FastAPI application.
- **Benefits**: Delivers auto-scaling pods, load-balancing, and zero-downtime rolling updates.

### 2. Centralized Observability
Currently, Prometheus scrapes a local target.
- **Migration**: Inject OpenTelemetry SDKs into the FastAPI endpoints for distributed tracing. Push metrics to Datadog or Azure Application Insights.
- **Benefits**: Provides robust SIEM monitoring on inference latency, 500-error rates, and model data drift.

## Phase 4: CI/CD Pipeline Maturity

We have basic GitHub Actions for linting and test execution.

### 1. Infrastructure as Code (IaC)
- **Migration**: Write Terraform scripts (`main.tf`, `variables.tf`) to provision the Snowflake resources, Azure ML clusters, and Kubernetes infrastructure declaratively.
- **Benefits**: Ensures the cloud environment can be securely spun up, audited, and destroyed automatically.

### 2. Container Registry Deployments
- **Migration**: Expand `.github/workflows/ci.yml` to authenticate via OIDC, build the Docker containers, and push them to Azure Container Registry (ACR).

---

## Open Questions

1. **Cloud Preference**: Which cloud provider is Amex GBT aiming to use for the Kubernetes deployment? (Azure AKS, strictly Databricks Model Serving, or AWS EKS?)
2. **IaC Tooling**: Do you prefer Terraform or Pulumi for defining the scalable infrastructure?
3. **Execution**: Would you like me to begin Phase 1 immediately and start rewriting the feature engineering logic into PySpark?

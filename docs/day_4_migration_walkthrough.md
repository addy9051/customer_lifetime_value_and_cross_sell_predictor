# Migration Walkthrough: Local MVP to Cloud-Native Production 🚀

We successfully migrated the Amex GBT Customer Lifetime Value and Cross-Sell prediction system from a local `pandas`/`joblib` architecture to a highly scalable, distributed enterprise cloud environment!

## 1. Massive Data Scalability (Phase 1)
We solved the local memory constraints of processing hundreds of millions of corporate travel bookings:
- **Snowflake & dbt**: We scaffolded the `.sql` staging models (`stg_corporate_accounts.sql`, `stg_bookings.sql`, etc.) under `data/dbt/` to securely extract and cleanse the raw ingestion data directly inside the Snowflake data warehouse.
- **Databricks PySpark**: For complex behavioral trajectories that SQL struggles with, we converted our Pandas feature engineering matrix into natively distributed PySpark logic (`feature_engineering_pyspark.py`).

## 2. Remote Tracking & MLFlow Model Registry (Phase 2)
We decoupled model state from the local disk:
- **Training Hooks**: We wired all four model scripts (`clv_model`, `survival_model`, `cross_sell_model`, `segmentation`) to auto-detect the `DATABRICKS_HOST` and push their tracking logs directly to your Databricks workspace path (`/Users/alzedrigo.addya@gmail.com/`).
- **Rich Artifacts**: The models now natively register their artifacts (XGBoost instances, UMAP clusters, Kaplan-Meier plots) inside the MLflow unified registry.
- **API Fetching**: Modified the Fast API backend (`api/main.py`) to stream the `Production`-tagged models dynamically directly from the registry upon container start.

## 3. High Availability Serving & IaC (Phase 3 & 4)
We moved away from local terminal hosting to fault-tolerant infrastructure:
- **Terraform Success**: We wrote and applied Azure Terraform definitions that explicitly generated an Azure Resource Group, an Azure Container Registry (`amexgbtmlcontainers`), and an auto-scaling Azure Kubernetes Service (`amex-gbt-clv-aks`).
- **Kubernetes Routing**: Built `api-deployment.yaml` and `api-service.yaml` to spin up 3 rolling-update pods attached to an internal load balancer.
- **CI/CD Auto-Rollouts**: Implemented a GitHub Action (`.github/workflows/deploy.yml`) that triggers on merge to `main`. It builds the Docker Image, pushes it to your ACR, and automatically maps the new SHA hash to `kubectl apply` for true zero-downtime microservice staging.

## 4. Orchestration
- **Airflow Model Retraining**: Added an Airflow DAG (`model_training_dag.py`) that waits for PySpark to finish, then fans-out to trigger all our remote-logging MLflow scripts concurrently every Sunday at 2:00 AM.

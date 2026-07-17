# Implementation Plan — Remote MLflow Integration (Phase 2)

This plan covers the transition of all local training scripts to utilize the **Databricks MLflow Tracking Server** as the centralized model registry. 

## User Review Required

> [!IMPORTANT]
> **Databricks Credentials**
> This implementation assumes that `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are correctly configured in your `.env` or GitHub Secrets. 
> 
> **Experiment Paths**
> I will be using a standard Databricks workspace path for experiments: `/Users/<user_email>/amex-gbt-clv-predictor/<model_name>`. You may need to update the base path if your Databricks workspace structure differs.

## Proposed Changes

We will modify the training scripts to detect if the environment is set for Databricks and adjust the `mlflow` configuration accordingly.

### [Model Training]

#### [MODIFY] [clv_model.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/models/clv_model.py)
- Update `log_to_mlflow` to support remote tracking.
- Set `mlflow.set_tracking_uri("databricks")`.
- Implement robust exception handling for remote network calls.

#### [MODIFY] [cross_sell_model.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/models/cross_sell_model.py)
- Implement `log_to_mlflow` function (currently missing).
- Log multi-label classification metrics and precision-recall artifacts.

#### [MODIFY] [survival_model.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/models/survival_model.py)
- Inject MLflow logging for Cox PH coefficients and Churn Risk distributions.

#### [MODIFY] [segmentation.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/models/segmentation.py)
- Inject MLflow logging for UMAP/HDBSCAN parameters and cluster silhouette scores.

---

## Open Questions

1. **Databricks Workspace Base Path**: Does `/Users/<your_email>/amex-gbt-clv-predictor/` work as a base path for your experiments, or do you have a specific workspace folder you'd like me to target?
2. **Model Registry Names**: Shall I use standard names like `amex-gbt-clv`, `amex-gbt-survival`, etc. for the logged models in the Databricks Model Registry?

## Verification Plan

### Automated Tests
- Running the training scripts with `DATABRICKS_HOST` set and verifying that they attempt to connect to the remote server.

### Manual Verification
- Verify in the Databricks UI that the experiments appear under the specified path and models are registered.

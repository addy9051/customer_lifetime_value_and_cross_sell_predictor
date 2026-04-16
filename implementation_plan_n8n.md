# Implementation Plan - n8n Workflow Automation Integration

This plan outlines the integration of **n8n** into the project as a low-code automation layer. n8n will provide the "Action" component of the Prescriptive Engine, allowing stakeholders to automate business processes based on ML predictions.

## User Review Required

> [!IMPORTANT]
> I will be reusing the existing `clv-postgres` container to host the n8n database (in a separate logical DB). This minimizes resource usage but requires a database initialization step.

## Proposed Changes

### [MODIFY] [docker-compose.yml](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/docker-compose.yml)
- **New Service: `n8n`**:
  - Image: `n8nio/n8n:latest`
  - Port: `5678`
  - Environment: DB configuration pointing to the local `postgres` container.
- **Service Dependency**: n8n will depend on the `postgres` healthcheck.

### [MODIFY] [.env](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/.env)
- **New Variables**:
  - `N8N_ENCRYPTION_KEY`: A unique key for securing credentials within n8n.
  - `N8N_POSTGRES_DB`: Set to `n8n`.
  - `N8N_POSTGRES_USER`: Reuse existing or create new.
  - `N8N_POSTGRES_PASSWORD`: Reuse existing.

### [NEW] [n8n_churn_workflow.json](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/n8n/n8n_churn_workflow.json)
- A pre-configured workflow JSON that can be imported into n8n.
- **Workflow Logic**:
  1. **Schedule Trigger**: Polls ogni 10 minuti (or manually).
  2. **Snowflake Node**: Query `fact_ml_churn` for accounts where `risk_level = 'High'`.
  3. **Conditional Logic**: Filter for "Top 5 Worst Offenders".
  4. **Webhook Node**: Sends the alert payload to a target URL (e.g., Slack).

## Verification Plan

### Automated Verification
- Run `docker compose up n8n -d` and check if the service is reachable at `http://localhost:5678`.
- Verify n8n can connect to the shared Postgres instance.

### Manual Verification
- Import the provided workflow into n8n.
- Test the Snowflake connection within the n8n UI using the existing credentials.

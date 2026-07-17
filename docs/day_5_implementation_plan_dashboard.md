# Deploy Frontend Dashboard to AKS

This plan outlines the steps to deploy the Streamlit Dashboard (`dashboard/app.py`) to the Azure Kubernetes Service (AKS) cluster, making it accessible alongside the existing Inference API.

## Proposed Changes

### 1. Kubernetes Manifests

We will create two new Kubernetes manifests to orchestrate the Streamlit dashboard on AKS within the `amex-gbt-ml` namespace.

#### [NEW] [dashboard-deployment.yaml](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/kubernetes/dashboard-deployment.yaml)
*   **Kind**: Deployment
*   **Replicas**: 1 (or 2 for high availability)
*   **Image**: Streamlit dashboard container (`amexgbtacr.azurecr.io/ml/clv-dashboard:latest`)
*   **Ports**: Expose container port `8501`.
*   **Environment**: Inject API connection string pointing to the internal K8s service `http://clv-inference-service:80`.
*   **Probes**: Liveness and Readiness probes pointing to `/_stcore/health`.

#### [NEW] [dashboard-service.yaml](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/kubernetes/dashboard-service.yaml)
*   **Kind**: Service
*   **Type**: LoadBalancer (so we can immediately get a public external IP to view the dashboard over the internet).
*   **Ports**: Map port `80` to target port `8501`.

### 2. CI/CD Pipeline Integration

We will append the dashboard build and deployment steps seamlessly to the existing GitHub Actions workflow.

#### [MODIFY] [deploy.yml](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/.github/workflows/deploy.yml)
*   **Env Variables**: Add `DASHBOARD_IMAGE_NAME: 'ml/clv-dashboard'`.
*   **Build Step**: Introduce `docker build`, `tag`, and `push` for `dashboard/Dockerfile`.
*   **Deploy Step**: Use `sed` to dynamically replace the dashboard image SHA in `dashboard-deployment.yaml`.
*   **Apply Step**: Run `kubectl apply -f kubernetes/dashboard-deployment.yaml` and `dashboard-service.yaml`.
*   **Verify Step**: Add `kubectl rollout status deployment/clv-dashboard -n amex-gbt-ml`.

## Verification Plan

### Automated Tests
*   Wait for the GitHub Actions pipeline to run and return a successful deployment status for the pipeline.

### Manual Verification
*   Run `kubectl get svc -n amex-gbt-ml` to fetch the external IP assigned to `clv-dashboard-service`.
*   Visit the external IP in a browser to confirm the Streamlit interface loads and correctly visualizes the portfolio health and cross-sell matrix.

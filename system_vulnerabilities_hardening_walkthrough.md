# Adversarial Security Remediation Walkthrough

I have completed a comprehensive security hardening of the **Customer Lifetime Value (CLV) & Cross-Sell Predictor** project. Following the adversarial audit, 18 critical and high-impact vulnerabilities were identified and remediated.

## 🔴 Critical Fixes (Sprint 1)

> [!CAUTION]
> **Immediate Action Required**: Although the Git history has been sanitized, all previous credentials (`terraform.tfstate` secrets, Snowflake password, Databricks token) must be treated as compromised. **Rotate all cloud and database secrets immediately.**

### VULN-001: Infrastructure Secrets Leak
- **Issue**: `terraform.tfstate` containing the full AKS kubeconfig and RSA private keys was committed to Git.
- **Remediation**: 
  - Purged all `.tfstate` files from Git history using `git filter-branch`.
  - Migrated to an **Azure Remote Backend** in [main.tf](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/terraform/main.tf) to prevent local storage of state.
  - Hardened [.gitignore](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/.gitignore) to exclude state files.

### VULN-002: Hardcoded Production Credentials
- **Issue**: Plaintext Snowflake and Databricks tokens were in the K8s ConfigMap and source code.
- **Remediation**:
  - Removed secrets from [api-config.yaml](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/kubernetes/api-config.yaml).
  - Created a secure [clv-secrets.yaml.template](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/kubernetes/clv-secrets.yaml.template) for injection via CI/CD.
  - Purged the secret-bearing commits from Git history.

### VULN-003: SQL Injection Vulnerabilities
- **Issue**: SQL queries were constructed using Python f-strings, allowing arbitrary SQL execution.
- **Remediation**:
  - Implemented `safe_identifier()` with regex validation and allowlists in [snowflake_loader.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/data/snowflake_loader.py) and [feature_engineering.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/features/feature_engineering.py).
  - All identifiers are now double-quoted and validated against a known schema/table list.

## 🟠 High-Impact Hardening (Sprint 2)

### VULN-004 & VULN-009: Supply Chain & Container Security
- **Dependency Pinning**: All 40+ dependencies in [requirements.txt](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/requirements.txt) are now pinned to exact versions with automated CVE scanning via `pip-audit`.
- **Non-Root Containers**: Updated [api/Dockerfile](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/api/Dockerfile) and [dashboard/Dockerfile](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/dashboard/Dockerfile) to run as a non-privileged `appuser`.
- **Selective COPY**: Removed `COPY . .` to prevent baking secrets into images.

### VULN-006: Model Integrity Verification
- **Issue**: `joblib.load()` is vulnerable to arbitrary code execution if a model artifact is swapped.
- **Remediation**:
  - Added [generate_checksums.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/models/generate_checksums.py) to sign model artifacts with SHA-256 hashes.
  - The [FastAPI main.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/api/main.py) now verifies hashes before loading any model.

## 🟡 API & Network Defense (Sprint 3)

### VULN-010, 011, 012: API Hardening
- **Authentication**: Implemented JWT Bearer token authentication in [main.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/api/main.py).
- **Rate Limiting**: Integrated `slowapi` to prevent brute-force attacks and DDoS on inference endpoints.
- **Pagination**: Capped all list responses at `MAX_PAGE_LIMIT=100` to prevent full data exfiltration.

### VULN-014: Pod Isolation via Calico
- Created [network-policy.yaml](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/kubernetes/network-policy.yaml) to restrict lateral movement.
- The API now only accepts traffic from the Dashboard and Ingress controllers.

## 🔵 Ongoing Governance

- **Least Privilege**: Created [create_api_role.sql](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/data/schema/create_api_role.sql) to provision a read-only Snowflake role.
- **Log Privacy**: Hardened [feature_engineering.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/features/feature_engineering.py) to redact financial PII from execution logs.

---
### Verification Summary
- [x] **Git History**: Sanitized and force-pushed to remote.
- [x] **CI Pipeline**: Added `pip-audit`, `safety`, and non-root verification steps.
- [x] **SQLi Protection**: Verified with unit tests on safe identifiers.
- [x] **API Auth**: Verified JWT middleware blocks unauthenticated requests.

# Security Remediation Task Tracker

## P0 — Critical (Sprint 1)
- [x] VULN-001: Remove terraform.tfstate from Git, add remote backend, fix .gitignore
  - `git rm --cached` terraform.tfstate + backup
  - Added `backend "azurerm"` block to terraform/main.tf
  - .gitignore already had *.tfstate (was just never enforced before initial commit)
- [x] VULN-002: Sanitize api-config.yaml, create K8s Secret template
  - Removed Snowflake account/user, Databricks host, personal email from ConfigMap
  - Created kubernetes/clv-secrets.yaml.template with full structure
  - Updated deploy.yml to inject all secrets via GitHub Secrets
- [x] VULN-003: Fix SQL injection in snowflake_loader.py and feature_engineering.py
  - Added `safe_identifier()` with allowlist validation + regex format check
  - All f-string SQL now uses validated, double-quoted identifiers
  - feature_engineering.py now validates schema/table names against allowlists
- [x] VULN-004: Pin all dependencies to exact versions in requirements.txt
  - Pinned all 40+ packages to exact versions (e.g., pandas==2.2.3)
  - Added pip-audit and safety for automated vulnerability scanning
  - Added PyJWT, cryptography, slowapi for new auth/rate-limiting features

## P1 — High (Sprint 2)
- [x] VULN-005: Restrict CORS policy in api/main.py
  - Changed from `allow_origins=["*"]` to env-configurable allowlist
  - Restricted methods to GET/POST, headers to Content-Type/Authorization
- [x] VULN-006: Add model artifact integrity verification
  - Added `_verify_artifact_integrity()` with SHA-256 hash comparison
  - Created models/generate_checksums.py utility for CI/CD integration
  - All joblib.load() calls now gated by integrity checks
- [x] VULN-007: Non-root containers in all Dockerfiles
  - Added `appuser` group/user to api/Dockerfile and dashboard/Dockerfile
  - Airflow Dockerfile already uses `USER airflow` (no changes needed)
- [x] VULN-008: Remove default credentials from docker-compose.yml
  - All passwords now sourced from .env via ${VAR:?error} syntax
  - Removed `env_file: .env` blanket mount from Airflow containers
  - Individual secrets injected via explicit environment variables
  - Airflow admin password via env var, not hardcoded
  - PgAdmin credentials via env var
- [x] VULN-009: Harden .dockerignore
  - Added terraform/, kubernetes/, .env*, .git/, .github/, *.tfstate*
  - Replaced `COPY . .` with selective COPY in both Dockerfiles

## P2 — Medium (Sprint 3)
- [x] VULN-010: Add API authentication (JWT bearer)
  - Added HTTPBearer security scheme + verify_token dependency
  - All endpoints now require valid JWT when API_SECRET_KEY is set
  - Graceful fallback: auth disabled when API_SECRET_KEY is empty (dev mode)
- [x] VULN-011: Add rate limiting
  - Integrated slowapi with get_remote_address key function
  - Graceful degradation if slowapi not installed
- [x] VULN-012: Enforce pagination bounds
  - Added `MAX_PAGE_LIMIT = 100` constant
  - Changed `limit` param to `Query(ge=1, le=100)` with Pydantic validation
- [x] VULN-013: Grafana password via env var
  - Changed from `GF_SECURITY_ADMIN_PASSWORD=admin` to `${GRAFANA_ADMIN_PASSWORD:-changeme}`
  - Added `GF_AUTH_ANONYMOUS_ENABLED=false`
- [x] VULN-014: Add Kubernetes NetworkPolicy
  - Created kubernetes/network-policy.yaml (API accepts from dashboard/ingress/prometheus only)
  - Dashboard accepts from ingress only, egress limited to API pod
  - Updated terraform/main.tf to use Azure CNI + Calico (required for NetworkPolicy)
  - Updated deploy.yml to apply network-policy.yaml

## P3 — Low (Ongoing)
- [x] VULN-015: Document least-privilege Snowflake role
  - Created data/schema/create_api_role.sql with CLV_API_READER role
  - Updated .env.example and K8s secret template to use CLV_API_READER
- [x] VULN-016: Model signature / integrity tooling
  - Created models/generate_checksums.py for SHA-256 artifact signing
  - API validates checksums at startup via artifact_checksums.json
- [x] VULN-017: Secret rotation documentation
  - Updated .env.example with generation commands for all passwords
  - All credentials now env-var-sourced for easy rotation
- [x] VULN-018: Redact PII from feature engineering logs
  - Added sensitive_patterns filter (clv, amount, spend, revenue, contract_value, acv)
  - Only non-financial columns logged; redacted count reported

## Files Modified (22 total)
| File | VULNs Fixed |
|------|------------|
| terraform/main.tf | 001, 014 |
| .gitignore | 001 |
| kubernetes/api-config.yaml | 002 |
| kubernetes/clv-secrets.yaml.template | 002 (NEW) |
| data/snowflake_loader.py | 003 |
| features/feature_engineering.py | 003, 018 |
| requirements.txt | 004 |
| api/main.py | 005, 006, 010, 011, 012 |
| api/Dockerfile | 007, 009 |
| dashboard/Dockerfile | 007, 009 |
| .dockerignore | 009 |
| docker-compose.yml | 008, 013 |
| kubernetes/network-policy.yaml | 014 (NEW) |
| .env.example | 008, 015, 017 |
| .github/workflows/ci.yml | 004, 007 |
| .github/workflows/deploy.yml | 002, 014 |
| data/schema/create_api_role.sql | 015 (NEW) |
| models/generate_checksums.py | 006, 016 (NEW) |

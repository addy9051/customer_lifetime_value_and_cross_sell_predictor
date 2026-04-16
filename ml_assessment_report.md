# Adversarial Security Assessment: Traditional ML & API Defense

Following a pivot from LLM-specific threats, an adversarial assessment of the **CLV & Cross-Sell Predictor** architecture (XGBoost, LightGBM, and FastAPI) has uncovered a new tier of vulnerabilities. While basic infrastructure flaws were previously patched, advanced logic, authorization, and data ingestion flaws remain.

Here are the functional exploit vectors currently present in the system:

---

### VULN-019: Broken Object Level Authorization (BOLA / IDOR) in Prediction Endpoints
- **Severity:** Critical
- **Threat Vector:** The FastAPI endpoints (`/predict/clv`, `/predict/churn`, `/accounts/{account_id}`) successfully validate that the user holds a valid JWT token, but entirely fail to verify if the token's subject (`sub`) is authorized to view the requested `account_id` record.
- **PoC:** 
  ```bash
  # Authenticated as an unprivileged user (or a different account manager)
  curl -X POST https://api.amexgbt.internal/predict/clv \
    -H "Authorization: Bearer <VALID_JWT_FOR_USER_A>" \
    -H "Content-Type: application/json" \
    -d '{"account_id": "TARGET_CUSTOMER_B"}'
  ```
- **Impact:** Any authenticated user can enumerate account IDs and systematically exfiltrate highly sensitive ML predictions (CLV, churn risk, cross-sell probability) for *all other customers* on the platform.
- **Remediation:** Implement robust authorization controls within the endpoints. Compare `request.account_id` to an access control list (ACL) or the `_user.get("allowed_accounts")` context extracted directly from the JWT payload.

---

### VULN-020: "Fail-Open" API Authentication via Missing Key Fallback
- **Severity:** Critical
- **Threat Vector:** In `api/main.py:verify_token()`, if the `API_SECRET_KEY` environment variable is null or missing, the system silently sets `API_AUTH_ENABLED = False` and returns a spoofed admin payload (`{"sub": "anonymous", "role": "admin"}`) instead of blocking access.
- **PoC:** If a Kubernetes pod restarts and the Secret fails to mount, or if an attacker successfully unsets the environment variable via an infrastructure vulnerability, the API immediately becomes entirely unauthenticated.
  ```bash
  # Simply sending a request with no auth header succeeds
  curl -X GET https://api.amexgbt.internal/health
  ```
- **Impact:** A simple configuration drift or transient infrastructure error instantly opens the massive corporate dataset to the public internet without raising internal application alarms.
- **Remediation:** Remove the development-mode fallback. The API must "Fail-Secure". If `API_SECRET_KEY` is not set, `verify_token()` must unconditionally raise a `401 Unauthorized` or crash on startup.

---

### VULN-021: "Fail-Open" ML Artifact Integrity Bypass
- **Severity:** High
- **Threat Vector:** The `_verify_artifact_integrity()` function protects against arbitrary code execution (Pickle/Joblib RCE). However, if the `artifact_checksums.json` file is deleted, or a specific key is removed from the JSON, the function returns `True` out of convenience.
- **PoC:** An attacker with filesystem access or a compromised upstream dependency simply deletes `artifact_checksums.json`, replaces `xgb_clv_model.joblib` with an arbitrary code execution payload (via `__reduce__`), and triggers a pod restart. The API will load the payload.
- **Impact:** Complete bypass of the cryptographic model signing guardrails, resulting in Remote Code Execution on the FastAPI inference worker.
- **Remediation:** Enforce a hard constraint: `if not checksums_path.exists(): return False`. 

---

### VULN-022: Unvalidated MLflow Model Deserialization (Supply Chain)
- **Severity:** High
- **Threat Vector:** When `DATABRICKS_HOST` is present, the API uses `mlflow.pyfunc.load_model("models:/amex-gbt-clv/Production")`. MLflow natively relies on Pickle and does not independently cryptographically sign artifact payloads by default. The local checksum logic covers local joblibs, but the MLflow path is entirely unverified.
- **PoC:** An attacker compromises the MLflow tracking server (or initiates a Man-in-the-Middle attack) and pushes a malicious `pyfunc` wrapper. When the API pod scales up, it fetches and implicitly deserializes the poisoned bundle.
- **Impact:** Remote Code Execution (RCE) and full pod takeover originating from a compromised internal Model Registry.
- **Remediation:** Transition to MLflow Model Signatures, force artifact checksum verification *after* downloading the MLFlow artifact but *before* calling `mlflow.pyfunc.load_model()`, or utilize ONNX formats for inference which do not allow arbitrary python execution.

---

### VULN-023: Training Data Poisoning via Unbounded Feature Generation
- **Severity:** Medium
- **Threat Vector:** `features/feature_engineering.py` aggressively aggregates historical financial data. It calculates `total_spend_90d` and `booking_volume_trend` directly derived from `bookings.amount` with zero mathematical bounds checking, clipping, or outlier removal.
- **PoC:** An attacker inserts a single booking into the raw datastore with `amount = 99,999,999,999.00`.
- **Impact:** 
  1. The anomaly destroys the statistical meaning of downstream normalized features.
  2. The extreme outlier vastly shifts XGBoost node boundaries during retraining, "poisoning" the distribution and rendering CLV predictions for that entire cluster highly inaccurate.
- **Remediation:** Implement defensive engineering (feature clipping/winsorizing) at a logical percentile (e.g., 99th percentile) or integrate an unsupervised anomaly detection layer (like Isolation Forest) to discard corrupted records prior to MLFlow registry pushes.

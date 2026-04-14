"""
FastAPI Inference Service
==========================
REST API for CLV prediction, churn risk, and cross-sell recommendations.
Simulates Amex GBT's EKS-based model serving architecture.

Endpoints:
    GET  /health               → Service health + model versions
    POST /predict/clv           → 12-month CLV prediction
    POST /predict/churn         → Churn risk & survival probability
    POST /predict/cross-sell    → Ranked product recommendations
    GET  /accounts/{id}         → Full account profile (CLV + churn + segment + recs)
    GET  /segments/summary      → Aggregate segment statistics
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import joblib
import mlflow
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    from prometheus_fastapi_instrumentator import Instrumentator
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# App Configuration
# =============================================================================

app = FastAPI(
    title="CLV & Cross-Sell Predictor API",
    description="Amex GBT Corporate Travel — Customer Lifetime Value & Cross-Sell Intelligence",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if HAS_PROMETHEUS:
    Instrumentator().instrument(app).expose(app)

# =============================================================================
# Model & Data Loading
# =============================================================================

ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "models/artifacts"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))

# Global model store
models = {}
data_store = {}


def load_models():
    """Load all model artifacts at startup."""
    global models, data_store

    # Toggle between MLFlow Model Registry and Local Joblib based on Environment Variables
    use_mlflow = bool(os.environ.get("DATABRICKS_HOST"))

    if use_mlflow:
        logger.info("DATABRICKS_HOST detected. Pulling models from MLFlow Model Registry...")
        mlflow.set_tracking_uri("databricks")
        
        try:
            models["clv"] = mlflow.pyfunc.load_model("models:/amex-gbt-clv/Production")
            models["survival"] = mlflow.pyfunc.load_model("models:/amex-gbt-survival/Production")
            models["cross_sell"] = mlflow.xgboost.load_model("models:/amex-gbt-cross-sell/Production")
            
            # Note: For MLflow pyfunc, prediction methods differ slightly from direct scikit-learn models,
            # but standard wrapper compatibility is maintained. Feature names should ideally be embedded
            # in the MLFlow artifact's MLModel signature. For simplicity in transition, we will still fallback
            # to loading local config schemas if they exist, or rely on Pandas DataFrame schemas.
            
            logger.info("Successfully loaded all 3 models from MLFlow Registry.")
        except Exception as e:
            logger.error("Failed to load models from MLFlow: %s", str(e))
            use_mlflow = False # Fall back to local if MLFlow fails during migration testing

    if not use_mlflow:
        # CLV model
        clv_path = ARTIFACTS_DIR / "clv" / "xgb_clv_model.joblib"
        if clv_path.exists():
            models["clv"] = joblib.load(clv_path)
            logger.info("Loaded CLV model locally")

            features_path = ARTIFACTS_DIR / "clv" / "feature_columns.json"
            if features_path.exists():
                with open(features_path) as f:
                    models["clv_features"] = json.load(f)

        # Survival model
        survival_path = ARTIFACTS_DIR / "survival" / "cox_ph_model.joblib"
        if survival_path.exists():
            models["survival"] = joblib.load(survival_path)
            models["survival_scaler"] = joblib.load(ARTIFACTS_DIR / "survival" / "survival_scaler.joblib")
            logger.info("Loaded survival model locally")

        # Cross-sell model
        xs_path = ARTIFACTS_DIR / "cross_sell" / "cross_sell_model.joblib"
        if xs_path.exists():
            models["cross_sell"] = joblib.load(xs_path)
            with open(ARTIFACTS_DIR / "cross_sell" / "feature_columns.json") as f:
                models["cross_sell_features"] = json.load(f)
            logger.info("Loaded cross-sell model locally")

    # Segmentation
    seg_path = ARTIFACTS_DIR / "segmentation" / "account_segments.parquet"
    if seg_path.exists():
        data_store["segments"] = pd.read_parquet(seg_path)
        logger.info("Loaded segment assignments")

    # Feature matrix
    features_path = DATA_DIR / "features" / "account_features.parquet"
    if features_path.exists():
        data_store["features"] = pd.read_parquet(features_path)
        logger.info("Loaded feature matrix")

    # Churn risk predictions
    churn_path = ARTIFACTS_DIR / "survival" / "churn_risk_predictions.parquet"
    if churn_path.exists():
        data_store["churn_risk"] = pd.read_parquet(churn_path)
        logger.info("Loaded churn risk predictions")

    # Cross-sell recommendations
    recs_path = ARTIFACTS_DIR / "cross_sell" / "account_recommendations.parquet"
    if recs_path.exists():
        data_store["recommendations"] = pd.read_parquet(recs_path)
        logger.info("Loaded cross-sell recommendations")

    # Cross-sell probabilities
    proba_path = ARTIFACTS_DIR / "cross_sell" / "cross_sell_probabilities.parquet"
    if proba_path.exists():
        data_store["cross_sell_proba"] = pd.read_parquet(proba_path)
        logger.info("Loaded cross-sell probabilities")

    logger.info("Model loading complete — %d models, %d data stores", len(models), len(data_store))


@app.on_event("startup")
async def startup():
    load_models()


# =============================================================================
# Request / Response Schemas
# =============================================================================

class HealthResponse(BaseModel):
    status: str
    models_loaded: list[str]
    data_stores_loaded: list[str]
    accounts_available: int


class CLVRequest(BaseModel):
    account_id: str


class CLVResponse(BaseModel):
    account_id: str
    clv_12m_predicted: float
    clv_percentile: float
    tier: str
    model: str = "XGBoost"


class ChurnRequest(BaseModel):
    account_id: str


class ChurnResponse(BaseModel):
    account_id: str
    churn_risk_score: float
    survival_prob_30d: float
    survival_prob_90d: float
    survival_prob_180d: float
    survival_prob_365d: float
    risk_level: str


class CrossSellRequest(BaseModel):
    account_id: str
    top_n: int = 3


class CrossSellResponse(BaseModel):
    account_id: str
    recommendations: list[dict]
    current_products: list[str]


class AccountProfile(BaseModel):
    account_id: str
    tier: str
    industry: str
    region: str
    clv_12m: float
    churn_risk_score: float
    risk_level: str
    segment: str
    top_recommendations: list[dict]
    key_metrics: dict


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="healthy",
        models_loaded=list(models.keys()),
        data_stores_loaded=list(data_store.keys()),
        accounts_available=len(data_store.get("features", [])),
    )


@app.post("/predict/clv", response_model=CLVResponse)
async def predict_clv(request: CLVRequest):
    if "features" not in data_store or "clv" not in models:
        raise HTTPException(status_code=503, detail="CLV model not loaded")

    features = data_store["features"]
    account = features[features["account_id"] == request.account_id]

    if len(account) == 0:
        raise HTTPException(status_code=404, detail=f"Account {request.account_id} not found")

    feature_cols = models.get("clv_features", [c for c in features.columns if c not in [
        "account_id", "tier", "region", "industry", "is_churned", "clv_12m", "feature_timestamp",
    ]])

    X = account[feature_cols].fillna(0)
    pred_log = models["clv"].predict(X)[0]
    pred = max(0, float(np.expm1(pred_log)))

    # Percentile rank
    all_clv = features["clv_12m"]
    percentile = float((all_clv < pred).mean() * 100)

    return CLVResponse(
        account_id=request.account_id,
        clv_12m_predicted=round(pred, 2),
        clv_percentile=round(percentile, 1),
        tier=account["tier"].iloc[0],
    )


@app.post("/predict/churn", response_model=ChurnResponse)
async def predict_churn(request: ChurnRequest):
    if "churn_risk" not in data_store:
        raise HTTPException(status_code=503, detail="Churn model not loaded")

    risk = data_store["churn_risk"]
    account = risk[risk["account_id"] == request.account_id]

    if len(account) == 0:
        raise HTTPException(status_code=404, detail=f"Account {request.account_id} not found")

    row = account.iloc[0]
    score = float(row["churn_risk_score"])
    risk_level = "High" if score > 0.5 else ("Medium" if score > 0.25 else "Low")

    return ChurnResponse(
        account_id=request.account_id,
        churn_risk_score=round(score, 4),
        survival_prob_30d=round(float(row.get("survival_prob_30d", 0)), 4),
        survival_prob_90d=round(float(row.get("survival_prob_90d", 0)), 4),
        survival_prob_180d=round(float(row.get("survival_prob_180d", 0)), 4),
        survival_prob_365d=round(float(row.get("survival_prob_365d", 0)), 4),
        risk_level=risk_level,
    )


@app.post("/predict/cross-sell", response_model=CrossSellResponse)
async def predict_cross_sell(request: CrossSellRequest):
    if "recommendations" not in data_store or "cross_sell_proba" not in data_store:
        raise HTTPException(status_code=503, detail="Cross-sell model not loaded")

    recs = data_store["recommendations"]
    proba = data_store["cross_sell_proba"]

    account_recs = recs[recs["account_id"] == request.account_id]
    account_proba = proba[proba["account_id"] == request.account_id]

    if len(account_recs) == 0:
        raise HTTPException(status_code=404, detail=f"Account {request.account_id} not found")

    row = account_recs.iloc[0]
    proba_row = account_proba.iloc[0]

    product_names = ["Neo", "Egencia Analytics Studio", "Meetings & Events", "Travel Consulting"]

    # Build recommendations list
    recommendations = []
    current_products = []

    for name in product_names:
        score = float(proba_row[f"{name}_score"])
        is_current = int(proba_row.get(f"{name}_current", 0))

        if is_current:
            current_products.append(name)
        else:
            recommendations.append({"product": name, "propensity_score": round(score, 4)})

    # Sort by score, take top_n
    recommendations.sort(key=lambda x: x["propensity_score"], reverse=True)
    recommendations = recommendations[:request.top_n]

    return CrossSellResponse(
        account_id=request.account_id,
        recommendations=recommendations,
        current_products=current_products,
    )


@app.get("/accounts/{account_id}", response_model=AccountProfile)
async def get_account_profile(account_id: str):
    """Full account profile — CLV + churn + segment + recommendations."""
    if "features" not in data_store:
        raise HTTPException(status_code=503, detail="Features not loaded")

    features = data_store["features"]
    account = features[features["account_id"] == account_id]

    if len(account) == 0:
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found")

    row = account.iloc[0]

    # CLV
    clv = float(row.get("clv_12m", 0))

    # Churn
    churn_score = 0.0
    risk_level = "Unknown"
    if "churn_risk" in data_store:
        churn_row = data_store["churn_risk"][data_store["churn_risk"]["account_id"] == account_id]
        if len(churn_row) > 0:
            churn_score = float(churn_row.iloc[0]["churn_risk_score"])
            risk_level = "High" if churn_score > 0.5 else ("Medium" if churn_score > 0.25 else "Low")

    # Segment
    segment = "Unknown"
    if "segments" in data_store:
        seg_row = data_store["segments"][data_store["segments"]["account_id"] == account_id]
        if len(seg_row) > 0:
            segment = seg_row.iloc[0]["segment"]

    # Recommendations
    top_recs = []
    if "recommendations" in data_store and "cross_sell_proba" in data_store:
        proba = data_store["cross_sell_proba"]
        proba_row = proba[proba["account_id"] == account_id]
        if len(proba_row) > 0:
            pr = proba_row.iloc[0]
            for name in ["Neo", "Egencia Analytics Studio", "Meetings & Events", "Travel Consulting"]:
                if int(pr.get(f"{name}_current", 0)) == 0:
                    top_recs.append({"product": name, "score": round(float(pr[f"{name}_score"]), 4)})
            top_recs.sort(key=lambda x: x["score"], reverse=True)
            top_recs = top_recs[:3]

    return AccountProfile(
        account_id=account_id,
        tier=str(row.get("tier", "Unknown")),
        industry=str(row.get("industry", "Unknown")),
        region=str(row.get("region", "Unknown")),
        clv_12m=round(clv, 2),
        churn_risk_score=round(churn_score, 4),
        risk_level=risk_level,
        segment=segment,
        top_recommendations=top_recs,
        key_metrics={
            "booking_count_90d": int(row.get("booking_count_90d", 0)),
            "total_spend_90d": round(float(row.get("total_spend_90d", 0)), 2),
            "num_active_products": int(row.get("num_active_products", 0)),
            "ticket_rate_per_month": round(float(row.get("ticket_rate_per_month", 0)), 2),
            "cancellation_rate_90d": round(float(row.get("cancellation_rate_90d", 0)), 4),
            "tenure_days": int(row.get("tenure_days", 0)),
        },
    )


@app.get("/segments/summary")
async def segment_summary():
    """Aggregate segment statistics."""
    if "segments" not in data_store or "features" not in data_store:
        raise HTTPException(status_code=503, detail="Segment data not loaded")

    features = data_store["features"].copy()
    segments = data_store["segments"][["account_id", "segment"]].copy()

    merged = features.merge(segments, on="account_id", how="left")

    summary = merged.groupby("segment").agg(
        count=("account_id", "count"),
        avg_clv=("clv_12m", "mean"),
        total_clv=("clv_12m", "sum"),
        avg_spend_90d=("total_spend_90d", "mean"),
        avg_bookings_90d=("booking_count_90d", "mean"),
        churn_rate=("is_churned", "mean"),
        avg_products=("num_active_products", "mean"),
    ).round(2).reset_index()

    return summary.to_dict(orient="records")


@app.get("/accounts")
async def list_accounts(
    tier: Optional[str] = None,
    segment: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """List accounts with optional filtering."""
    if "features" not in data_store:
        raise HTTPException(status_code=503, detail="Features not loaded")

    df = data_store["features"].copy()

    # Join segments if available
    if "segments" in data_store:
        df = df.merge(data_store["segments"][["account_id", "segment"]], on="account_id", how="left")

    if tier:
        df = df[df["tier"] == tier]
    if segment and "segment" in df.columns:
        df = df[df["segment"] == segment]

    total = len(df)
    df = df.iloc[offset:offset + limit]

    cols = ["account_id", "tier", "region", "industry", "clv_12m", "is_churned",
            "booking_count_90d", "total_spend_90d", "num_active_products"]
    if "segment" in df.columns:
        cols.append("segment")

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "accounts": df[cols].round(2).to_dict(orient="records"),
    }

"""
API Unit Tests
===============
Tests for the FastAPI inference service endpoints.
"""

# Patch environment before importing app
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    """
    Provide a TestClient configured to run the FastAPI app's lifespan with required environment variables set.
    
    Sets `ARTIFACTS_DIR` and `DATA_DIR` environment variables, imports the FastAPI `app`, and yields a TestClient created inside a context manager so the application's startup/lifespan handlers run before tests execute.
    
    Returns:
        TestClient: A test client for the FastAPI `app` with startup/lifespan executed.
    """
    os.environ["ARTIFACTS_DIR"] = "models/artifacts"
    os.environ["DATA_DIR"] = "data"

    from api.main import app

    # Enter the context manager so the FastAPI lifespan handler runs and loads
    # models/data. Returning a bare TestClient (as before) skips startup, which
    # left every data-dependent endpoint at 503 and silently un-tested.
    with TestClient(app) as test_client:
        yield test_client


def _first_account_id(client):
    """
    Retrieve the first loaded account identifier or skip the test if artifacts or accounts are unavailable.
    
    Returns:
        account_id (str): The `account_id` of the first loaded account.
    
    Notes:
        If the endpoint returns HTTP 503, the test is skipped via `pytest.skip("Features not loaded — model artifacts not available")`.
        If no accounts are returned, the test is skipped via `pytest.skip("No accounts loaded")`.
    """
    response = client.get("/accounts?limit=1")
    if response.status_code == 503:
        pytest.skip("Features not loaded — model artifacts not available")
    accounts = response.json()["accounts"]
    if not accounts:
        pytest.skip("No accounts loaded")
    return accounts[0]["account_id"]


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_response_shape(self, client):
        response = client.get("/health")
        data = response.json()
        assert "status" in data
        assert "models_loaded" in data
        assert "data_stores_loaded" in data
        assert data["status"] == "healthy"


class TestAccountsEndpoint:
    def test_list_accounts_returns_200(self, client):
        response = client.get("/accounts?limit=5")
        if response.status_code == 503:
            pytest.skip("Features not loaded — model artifacts not available")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "accounts" in data

    def test_list_accounts_with_tier_filter(self, client):
        response = client.get("/accounts?tier=Platinum&limit=5")
        if response.status_code == 503:
            pytest.skip("Features not loaded")
        assert response.status_code == 200

    def test_account_profile_not_found(self, client):
        response = client.get("/accounts/NONEXISTENT-ID")
        assert response.status_code in [404, 503]


class TestPredictionEndpoints:
    def test_clv_prediction_not_found(self, client):
        response = client.post("/predict/clv", json={"account_id": "NONEXISTENT"})
        assert response.status_code in [404, 503]

    def test_churn_prediction_not_found(self, client):
        response = client.post("/predict/churn", json={"account_id": "NONEXISTENT"})
        assert response.status_code in [404, 503]

    def test_cross_sell_not_found(self, client):
        response = client.post("/predict/cross-sell", json={"account_id": "NONEXISTENT", "top_n": 3})
        assert response.status_code in [404, 503]


class TestSegmentEndpoint:
    def test_segment_summary(self, client):
        response = client.get("/segments/summary")
        # May return 503 if data not loaded
        assert response.status_code in [200, 503]


class TestHappyPath:
    """Exercise real prediction paths when model/data artifacts are present.

    Each test skips cleanly if artifacts aren't loaded (e.g. on a fresh CI
    checkout), so the suite stays green there while genuinely covering the
    happy paths locally / wherever artifacts exist.
    """

    def test_clv_prediction_returns_score(self, client):
        account_id = _first_account_id(client)
        response = client.post("/predict/clv", json={"account_id": account_id})
        assert response.status_code == 200
        data = response.json()
        assert data["account_id"] == account_id
        assert data["clv_12m_predicted"] >= 0
        assert 0 <= data["clv_percentile"] <= 100

    def test_account_profile_current_products(self, client):
        """Regression test for the outreach current-products bug (M1):
        current_products must reflect adopted products and never overlap
        the (non-current) recommendations."""
        account_id = _first_account_id(client)
        response = client.get(f"/accounts/{account_id}")
        assert response.status_code == 200
        profile = response.json()
        assert isinstance(profile["current_products"], list)
        recommended = {r["product"] for r in profile["top_recommendations"]}
        assert set(profile["current_products"]).isdisjoint(recommended)

    def test_cross_sell_recommendations_ranked(self, client):
        account_id = _first_account_id(client)
        response = client.post("/predict/cross-sell", json={"account_id": account_id, "top_n": 3})
        assert response.status_code == 200
        recs = response.json()["recommendations"]
        assert len(recs) <= 3
        scores = [r["propensity_score"] for r in recs]
        assert scores == sorted(scores, reverse=True)

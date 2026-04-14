"""
API Unit Tests
===============
Tests for the FastAPI inference service endpoints.
"""

# Patch environment before importing app
import os

import pytest
from fastapi.testclient import TestClient

os.environ["ARTIFACTS_DIR"] = "models/artifacts"
os.environ["DATA_DIR"] = "data"

from api.main import app

client = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_response_shape(self):
        response = client.get("/health")
        data = response.json()
        assert "status" in data
        assert "models_loaded" in data
        assert "data_stores_loaded" in data
        assert data["status"] == "healthy"


class TestAccountsEndpoint:
    def test_list_accounts_returns_200(self):
        response = client.get("/accounts?limit=5")
        if response.status_code == 503:
            pytest.skip("Features not loaded — model artifacts not available")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "accounts" in data

    def test_list_accounts_with_tier_filter(self):
        response = client.get("/accounts?tier=Platinum&limit=5")
        if response.status_code == 503:
            pytest.skip("Features not loaded")
        assert response.status_code == 200

    def test_account_profile_not_found(self):
        response = client.get("/accounts/NONEXISTENT-ID")
        assert response.status_code in [404, 503]


class TestPredictionEndpoints:
    def test_clv_prediction_not_found(self):
        response = client.post("/predict/clv", json={"account_id": "NONEXISTENT"})
        assert response.status_code in [404, 503]

    def test_churn_prediction_not_found(self):
        response = client.post("/predict/churn", json={"account_id": "NONEXISTENT"})
        assert response.status_code in [404, 503]

    def test_cross_sell_not_found(self):
        response = client.post("/predict/cross-sell", json={"account_id": "NONEXISTENT", "top_n": 3})
        assert response.status_code in [404, 503]


class TestSegmentEndpoint:
    def test_segment_summary(self):
        response = client.get("/segments/summary")
        # May return 503 if data not loaded
        assert response.status_code in [200, 503]

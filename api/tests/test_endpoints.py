from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock

import pytest


class CursorMock:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("RDS_HOST", "localhost")
    monkeypatch.setenv("RDS_PORT", "5432")
    monkeypatch.setenv("RDS_DB", "shopstream_dwh")
    monkeypatch.setenv("RDS_USER", "shopstream_admin")
    monkeypatch.setenv("RDS_PASSWORD", "secret")

    if "api.app" in sys.modules:
        module = importlib.reload(sys.modules["api.app"])
    else:
        module = importlib.import_module("api.app")

    rows = [
        {
            "page_url": "/",
            "avg_time_seconds": 42.5,
            "session_count": 10,
            "rank": 1,
            "device_type": "mobile",
            "country": "Colombia",
            "session_id": "session-1",
            "user_id": "user-1",
            "page_type": "home",
            "z_score": 2.7,
            "anomaly_type": "z_score",
        }
    ]
    conn = MagicMock()
    conn.cursor.return_value = CursorMock(rows)
    pool = MagicMock()
    pool.getconn.return_value = conn
    module._pool = pool
    module.app.config.update(TESTING=True)
    return module.app.test_client()


def test_pages_top_valid(client):
    response = client.get("/pages/top?metric=time_on_page&date=2025-06-01&limit=5")
    assert response.status_code == 200


def test_pages_top_invalid_metric(client):
    response = client.get("/pages/top?metric=invalid")
    assert response.status_code == 400


def test_sessions_summary_valid(client):
    response = client.get("/sessions/summary?date=2025-06-01")
    assert response.status_code == 200


def test_sessions_no_date(client):
    response = client.get("/sessions/summary")
    assert response.status_code == 400


def test_anomalies_valid(client):
    response = client.get("/anomalies?date=2025-06-01")
    assert response.status_code == 200


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.get_json()["status"] == "ok"

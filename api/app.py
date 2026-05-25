from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from psycopg2 import DatabaseError
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


load_dotenv()

app = Flask(__name__)
_pool: SimpleConnectionPool | None = None
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def rows_to_json(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: json_value(value) for key, value in row.items()} for row in rows]


def error_response(message: str, status_code: int):
    return jsonify({"error": message}), status_code


def validate_date(date_value: str | None) -> bool:
    if not date_value or not DATE_RE.match(date_value):
        return False
    try:
        datetime.strptime(date_value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_pool() -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.environ["RDS_HOST"],
            port=os.environ.get("RDS_PORT", "5432"),
            dbname=os.environ["RDS_DB"],
            user=os.environ["RDS_USER"],
            password=os.environ["RDS_PASSWORD"],
            cursor_factory=RealDictCursor,
        )
    return _pool


def fetch_all(query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())
    finally:
        pool.putconn(conn)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})


@app.route("/pages/top")
def pages_top():
    metric = request.args.get("metric")
    date = request.args.get("date")
    limit_raw = request.args.get("limit", "10")

    if metric not in {"bounce_rate", "time_on_page"}:
        return error_response("metric must be bounce_rate or time_on_page", 400)
    if not validate_date(date):
        return error_response("date must use YYYY-MM-DD format", 400)
    try:
        limit = int(limit_raw)
        if limit < 1 or limit > 100:
            raise ValueError
    except ValueError:
        return error_response("limit must be an integer between 1 and 100", 400)

    if metric == "time_on_page":
        query = """
            SELECT
                page_url,
                avg_time_seconds,
                session_count,
                rank
            FROM shopstream_dwh.fact_top_pages
            WHERE date = %s
            ORDER BY rank
            LIMIT %s
        """
    else:
        query = """
            SELECT
                page_type,
                bounce_rate,
                total_sessions,
                bounced_sessions
            FROM shopstream_dwh.fact_bounce_rate
            WHERE date = %s
            ORDER BY bounce_rate DESC
            LIMIT %s
        """

    try:
        data = rows_to_json(fetch_all(query, (date, limit)))
    except (DatabaseError, KeyError) as exc:
        return error_response(f"database error: {exc}", 500)
    if not data:
        return error_response("no data found", 404)
    return jsonify({"date": date, "metric": metric, "limit": limit, "data": data})


@app.route("/sessions/summary")
def sessions_summary():
    date = request.args.get("date")
    country = request.args.get("country")
    device = request.args.get("device")
    if not validate_date(date):
        return error_response("date must use YYYY-MM-DD format", 400)

    filters = []
    params: list[Any] = [date]
    if country:
        filters.append("country = %s")
        params.append(country)
    if device:
        filters.append("device_type = %s")
        params.append(device)

    where_clause = " AND ".join(["date = %s", *filters])
    query = f"""
        SELECT
            device_type,
            country,
            avg_time_seconds,
            session_count
        FROM shopstream_dwh.fact_device_country_time
        WHERE {where_clause}
        ORDER BY session_count DESC
    """

    try:
        results = rows_to_json(fetch_all(query, tuple(params)))
    except (DatabaseError, KeyError) as exc:
        return error_response(f"database error: {exc}", 500)
    if not results:
        return error_response("no data found", 404)
    return jsonify({"date": date, "filters": {"country": country, "device": device}, "results": results})


@app.route("/anomalies")
def anomalies():
    date = request.args.get("date")
    if not validate_date(date):
        return error_response("date must use YYYY-MM-DD format", 400)

    query = """
        SELECT
            session_id,
            user_id,
            page_type,
            z_score,
            anomaly_type
        FROM shopstream_dwh.fact_anomalies
        WHERE date = %s
        ORDER BY ABS(z_score) DESC
    """
    try:
        anomalies_data = rows_to_json(fetch_all(query, (date,)))
    except (DatabaseError, KeyError) as exc:
        return error_response(f"database error: {exc}", 500)
    if not anomalies_data:
        return error_response("no data found", 404)
    return jsonify({"date": date, "total": len(anomalies_data), "anomalies": anomalies_data})


if __name__ == "__main__":
    app.run(debug=True)

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROCESS_DATE = "2025-06-01"
SCHEMA = "shopstream_dwh"
S3_BUCKET = "shopstream-processed-401466721010"

TABLE_MAPPING = {
    "top_pages": "fact_top_pages",
    "bounce_rate": "fact_bounce_rate",
    "conversion_funnel": "fact_conversion_funnel",
    "high_view_low_cart": "fact_high_view_low_cart",
    "navigation_paths": "fact_navigation_paths",
    "device_country_time": "fact_device_country_time",
    "anomalies": "fact_anomalies",
}


def database_url() -> str:
    password = quote_plus(os.environ["RDS_PASSWORD"])
    return (
        f"postgresql+psycopg2://{os.environ['RDS_USER']}:{password}"
        f"@{os.environ['RDS_HOST']}:{os.environ.get('RDS_PORT', '5432')}/{os.environ['RDS_DB']}"
    )


def parquet_path(metric_name: str) -> str:
    return f"s3://{S3_BUCKET}/{metric_name}/date={PROCESS_DATE}/"


def add_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = PROCESS_DATE
    return df


def prepare_top_pages(df: pd.DataFrame) -> pd.DataFrame:
    df = add_date(df)
    df = df.rename(
        columns={
            "avg_time_on_page_seconds": "avg_time_seconds",
            "event_count": "session_count",
        }
    )
    df = df.sort_values("avg_time_seconds", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df[["date", "page_url", "avg_time_seconds", "session_count", "rank"]]


def prepare_bounce_rate(df: pd.DataFrame) -> pd.DataFrame:
    df = add_date(df)
    return df[["date", "page_type", "bounce_rate", "total_sessions", "bounced_sessions"]]


def prepare_conversion_funnel(df: pd.DataFrame) -> pd.DataFrame:
    row = df.iloc[0].to_dict()
    stages = [
        ("page_view", row.get("stage1_page_view_users"), None),
        ("product_view", row.get("stage2_product_view_users"), row.get("stage2_over_stage1_pct")),
        ("cart_add", row.get("stage3_cart_add_users"), row.get("stage3_over_stage2_pct")),
        ("checkout", row.get("stage4_checkout_users"), row.get("stage4_over_stage3_pct")),
    ]
    return pd.DataFrame(
        [
            {
                "date": PROCESS_DATE,
                "stage": stage,
                "user_count": int(user_count or 0),
                "conversion_rate": None if conversion_rate is None else float(conversion_rate) / 100,
            }
            for stage, user_count, conversion_rate in stages
        ]
    )


def prepare_high_view_low_cart(df: pd.DataFrame) -> pd.DataFrame:
    df = add_date(df)
    return df[["date", "product_id", "category", "avg_price", "view_count", "cart_add_count", "conversion_rate"]]


def prepare_navigation_paths(df: pd.DataFrame) -> pd.DataFrame:
    df = add_date(df)
    df = df.sort_values("session_count", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df[["date", "path", "session_count", "rank"]]


def prepare_device_country_time(df: pd.DataFrame) -> pd.DataFrame:
    df = add_date(df)
    df = df.rename(
        columns={
            "avg_time_on_page_seconds": "avg_time_seconds",
            "distinct_sessions": "session_count",
        }
    )
    return df[["date", "device_type", "country", "avg_time_seconds", "session_count"]]


def prepare_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    df = add_date(df)
    df = df.rename(columns={"time_on_page_seconds": "time_on_page"})
    return df[
        [
            "date",
            "session_id",
            "user_id",
            "page_type",
            "time_on_page",
            "z_score",
            "is_iqr_outlier",
            "anomaly_type",
        ]
    ]


PREPARERS = {
    "top_pages": prepare_top_pages,
    "bounce_rate": prepare_bounce_rate,
    "conversion_funnel": prepare_conversion_funnel,
    "high_view_low_cart": prepare_high_view_low_cart,
    "navigation_paths": prepare_navigation_paths,
    "device_country_time": prepare_device_country_time,
    "anomalies": prepare_anomalies,
}


def load_table(engine, metric_name: str, table_name: str) -> int:
    df = pd.read_parquet(parquet_path(metric_name))
    prepared = PREPARERS[metric_name](df)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{table_name}"))
    prepared.to_sql(table_name, engine, schema=SCHEMA, if_exists="append", index=False, method="multi", chunksize=1000)
    return len(prepared)


def main() -> None:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    engine = create_engine(database_url())
    total_rows = 0
    for metric_name, table_name in TABLE_MAPPING.items():
        inserted = load_table(engine, metric_name, table_name)
        total_rows += inserted
        print(f"{table_name}: {inserted} filas insertadas")
    print(f"total_filas_insertadas: {total_rows}")


if __name__ == "__main__":
    main()

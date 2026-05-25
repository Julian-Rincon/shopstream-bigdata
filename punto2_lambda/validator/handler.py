from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote_plus
from uuid import UUID

import boto3
import pandas as pd


EVENT_TYPES = ("page_view", "click", "search", "product_view", "cart_event")
PAGE_TYPES = {"home", "category", "product", "cart", "checkout"}
CART_ACTIONS = {"add", "remove"}

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")


def infer_event_type(key: str) -> str:
    file_name = key.rsplit("/", 1)[-1]
    for event_type in EVENT_TYPES:
        if file_name.startswith(event_type):
            return event_type
    raise ValueError(f"Cannot infer event type from key: {key}")


def is_valid_uuid(value: Any) -> bool:
    try:
        UUID(str(value))
        return True
    except (TypeError, ValueError):
        return False


def get_s3_object(bucket: str, key: str) -> tuple[pd.DataFrame, int]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pd.read_csv(io.BytesIO(body)), len(body)


def put_metric(name: str, value: float, unit: str = "Count") -> None:
    cloudwatch.put_metric_data(
        Namespace=os.getenv("CLOUDWATCH_NAMESPACE", "ShopStream/Ingesta"),
        MetricData=[{"MetricName": name, "Value": value, "Unit": unit}],
    )


def failed_rows(df: pd.DataFrame, mask: pd.Series) -> list[dict[str, Any]]:
    if df.empty:
        return []
    records = df.loc[mask].head(3).where(pd.notnull(df), None).to_dict(orient="records")
    return records


def validate_required_columns(df: pd.DataFrame, columns: set[str]) -> tuple[list[str], pd.Series]:
    missing = sorted(columns - set(df.columns))
    if missing:
        return [f"missing_column:{column}" for column in missing], pd.Series(True, index=df.index)
    return [], pd.Series(False, index=df.index)


def validate_common(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    errors, invalid = validate_required_columns(df, {"user_id", "session_id", "timestamp"})
    if errors:
        return errors, invalid

    failed_fields: list[str] = []
    user_invalid = ~df["user_id"].map(is_valid_uuid)
    session_invalid = df["session_id"].isna() | (df["session_id"].astype(str).str.strip() == "")
    timestamp_invalid = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).isna()

    if user_invalid.any():
        failed_fields.append("user_id")
    if session_invalid.any():
        failed_fields.append("session_id")
    if timestamp_invalid.any():
        failed_fields.append("timestamp")

    return failed_fields, user_invalid | session_invalid | timestamp_invalid


def validate_by_type(df: pd.DataFrame, event_type: str) -> tuple[list[str], pd.Series]:
    validators = {
        "page_view": validate_page_view,
        "product_view": validate_product_view,
        "cart_event": validate_cart_event,
        "click": validate_click,
        "search": validate_search,
    }
    return validators[event_type](df)


def validate_page_view(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    errors, invalid = validate_required_columns(df, {"page_type", "time_on_page_seconds"})
    if errors:
        return errors, invalid

    failed_fields: list[str] = []
    page_type_invalid = ~df["page_type"].isin(PAGE_TYPES)
    time_values = pd.to_numeric(df["time_on_page_seconds"], errors="coerce")
    time_invalid = time_values.isna() | ~time_values.between(1, 3600)

    if page_type_invalid.any():
        failed_fields.append("page_type")
    if time_invalid.any():
        failed_fields.append("time_on_page_seconds")

    return failed_fields, page_type_invalid | time_invalid


def validate_product_view(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    errors, invalid = validate_required_columns(df, {"price", "product_id"})
    if errors:
        return errors, invalid

    failed_fields: list[str] = []
    prices = pd.to_numeric(df["price"], errors="coerce")
    price_invalid = prices.isna() | (prices <= 0)
    product_invalid = df["product_id"].isna() | (df["product_id"].astype(str).str.strip() == "")

    if price_invalid.any():
        failed_fields.append("price")
    if product_invalid.any():
        failed_fields.append("product_id")

    return failed_fields, price_invalid | product_invalid


def validate_cart_event(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    errors, invalid = validate_required_columns(df, {"action"})
    if errors:
        return errors, invalid

    action_invalid = ~df["action"].isin(CART_ACTIONS)
    return (["action"] if action_invalid.any() else []), action_invalid


def validate_click(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    errors, invalid = validate_required_columns(df, {"x_position", "y_position"})
    if errors:
        return errors, invalid

    failed_fields: list[str] = []
    x_values = pd.to_numeric(df["x_position"], errors="coerce")
    y_values = pd.to_numeric(df["y_position"], errors="coerce")
    x_invalid = x_values.isna() | (x_values < 0) | (x_values % 1 != 0)
    y_invalid = y_values.isna() | (y_values < 0) | (y_values % 1 != 0)

    if x_invalid.any():
        failed_fields.append("x_position")
    if y_invalid.any():
        failed_fields.append("y_position")

    return failed_fields, x_invalid | y_invalid


def validate_search(df: pd.DataFrame) -> tuple[list[str], pd.Series]:
    return [], pd.Series(False, index=df.index)


def write_error_report(
    bucket: str,
    key: str,
    tipo_error: str,
    campos_fallidos: list[str],
    rows: list[dict[str, Any]],
) -> None:
    error_key = f"quarantine/{key}.error.json"
    payload = {
        "timestamp_proceso": datetime.now(timezone.utc).isoformat(),
        "tipo_error": tipo_error,
        "campos_fallidos": campos_fallidos,
        "primeras_3_filas_problematicas": rows,
    }
    s3.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def quarantine_file(bucket: str, key: str) -> None:
    quarantine_key = f"quarantine/{key}"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=quarantine_key)
    s3.delete_object(Bucket=bucket, Key=key)


def validate_file(bucket: str, key: str) -> dict[str, Any]:
    event_type = infer_event_type(key)
    df, size_bytes = get_s3_object(bucket, key)
    mb = size_bytes / (1024 * 1024)

    common_fields, common_invalid = validate_common(df)
    type_fields, type_invalid = validate_by_type(df, event_type)
    invalid_mask = common_invalid | type_invalid
    failed_fields = sorted(set(common_fields + type_fields))

    if invalid_mask.any() or failed_fields:
        bad_rows = failed_rows(df, invalid_mask)
        write_error_report(bucket, key, "validation_error", failed_fields, bad_rows)
        quarantine_file(bucket, key)
        put_metric("archivos_invalidos", 1)
        print(
            json.dumps(
                {
                    "event": "file_invalid",
                    "key": key,
                    "event_type": event_type,
                    "failed_fields": failed_fields,
                    "invalid_rows": int(invalid_mask.sum()),
                }
            )
        )
        return {"status": "invalid", "key": key, "failed_fields": failed_fields}

    rows = len(df)
    put_metric("archivos_procesados", 1)
    put_metric("registros_validos", rows)
    put_metric("tamanio_mb", mb, unit="Megabytes")
    print(json.dumps({"event": "file_validated", "key": key, "rows": rows, "mb": round(mb, 4)}))
    return {"status": "valid", "key": key, "rows": rows, "mb": mb}


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        if key.startswith("quarantine/"):
            continue
        results.append(validate_file(bucket, key))
    return {"processed": len(results), "results": results}

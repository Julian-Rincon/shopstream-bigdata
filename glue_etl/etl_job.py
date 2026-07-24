import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


PROCESSED_BUCKET = "s3://shopstream-processed-401466721010"
RDS_CONNECTION = "shopstream-rds-connection"
RDS_DATABASE = "shopstream_dwh"
RDS_SCHEMA = "shopstream_dwh"
PROCESS_DATE = "2025-06-01"


TABLE_CONFIG = {
    "top_pages": {
        "table": "fact_top_pages",
        "mapping": [
            ("date", "string", "date", "date"),
            ("page_url", "string", "page_url", "string"),
            ("avg_time_on_page_seconds", "double", "avg_time_seconds", "decimal"),
            ("event_count", "long", "session_count", "int"),
            ("rank", "int", "rank", "int"),
        ],
    },
    "bounce_rate": {
        "table": "fact_bounce_rate",
        "mapping": [
            ("date", "string", "date", "date"),
            ("page_type", "string", "page_type", "string"),
            ("bounce_rate", "double", "bounce_rate", "decimal"),
            ("total_sessions", "long", "total_sessions", "int"),
            ("bounced_sessions", "long", "bounced_sessions", "int"),
        ],
    },
    "conversion_funnel": {
        "table": "fact_conversion_funnel",
        "mapping": [
            ("date", "string", "date", "date"),
            ("stage", "string", "stage", "string"),
            ("user_count", "long", "user_count", "int"),
            ("conversion_rate", "double", "conversion_rate", "decimal"),
        ],
    },
    "high_view_low_cart": {
        "table": "fact_high_view_low_cart",
        "mapping": [
            ("date", "string", "date", "date"),
            ("product_id", "string", "product_id", "string"),
            ("category", "string", "category", "string"),
            ("avg_price", "double", "avg_price", "decimal"),
            ("view_count", "long", "view_count", "int"),
            ("cart_add_count", "long", "cart_add_count", "int"),
            ("conversion_rate", "double", "conversion_rate", "decimal"),
        ],
    },
    "navigation_paths": {
        "table": "fact_navigation_paths",
        "mapping": [
            ("date", "string", "date", "date"),
            ("path", "string", "path", "string"),
            ("session_count", "long", "session_count", "int"),
            ("rank", "int", "rank", "int"),
        ],
    },
    "device_country_time": {
        "table": "fact_device_country_time",
        "mapping": [
            ("date", "string", "date", "date"),
            ("device_type", "string", "device_type", "string"),
            ("country", "string", "country", "string"),
            ("avg_time_on_page_seconds", "double", "avg_time_seconds", "decimal"),
            ("distinct_sessions", "long", "session_count", "int"),
        ],
    },
    "anomalies": {
        "table": "fact_anomalies",
        "mapping": [
            ("date", "string", "date", "date"),
            ("session_id", "string", "session_id", "string"),
            ("user_id", "string", "user_id", "string"),
            ("page_type", "string", "page_type", "string"),
            ("time_on_page_seconds", "double", "time_on_page", "decimal"),
            ("z_score", "double", "z_score", "decimal"),
            ("is_iqr_outlier", "boolean", "is_iqr_outlier", "boolean"),
            ("anomaly_type", "string", "anomaly_type", "string"),
        ],
    },
}


def add_date_and_rank(metric_name, dataframe):
    frame = dataframe.withColumn("date", F.lit(PROCESS_DATE))
    if metric_name in {"top_pages", "navigation_paths"}:
        frame = frame.withColumn("rank", F.monotonically_increasing_id().cast("int") + F.lit(1))
    if metric_name == "conversion_funnel":
        row = frame.first()
        rows = [
            ("page_view", row["stage1_page_view_users"], None),
            ("product_view", row["stage2_product_view_users"], row["stage2_over_stage1_pct"]),
            ("cart_add", row["stage3_cart_add_users"], row["stage3_over_stage2_pct"]),
            ("checkout", row["stage4_checkout_users"], row["stage4_over_stage3_pct"]),
        ]
        frame = dataframe.sparkSession.createDataFrame(rows, ["stage", "user_count", "conversion_rate"])
        frame = frame.withColumn("date", F.lit(PROCESS_DATE))
        frame = frame.withColumn("conversion_rate", F.col("conversion_rate") / F.lit(100.0))
    return frame


def process_metric(glue_context, metric_name, config):
    # Glue Studio visual node: S3 source
    source_path = f"{PROCESSED_BUCKET}/{metric_name}/date={PROCESS_DATE}/"
    source = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_path], "recurse": True},
        format="parquet",
        transformation_ctx=f"{metric_name}_s3_source",
    )

    # Glue Studio visual node: derived fields for date/rank/funnel normalization
    source_df = source.toDF()
    normalized_df = add_date_and_rank(metric_name, source_df)
    normalized = DynamicFrame.fromDF(normalized_df, glue_context, f"{metric_name}_normalized")

    # Glue Studio visual node: ApplyMapping
    mapped = ApplyMapping.apply(
        frame=normalized,
        mappings=config["mapping"],
        transformation_ctx=f"{metric_name}_apply_mapping",
    )

    # Glue Studio visual node: Filter(validos)
    valid_records = Filter.apply(
        frame=mapped,
        f=lambda row: row["date"] is not None,
        transformation_ctx=f"{metric_name}_filter_validos",
    )

    # Glue Studio visual node: DataQuality placeholder
    # In Glue Studio, attach Evaluate Data Quality with rules such as:
    # ColumnExists "date", ColumnValues "date" IS NOT NULL, and time fields BETWEEN 0 AND 3600.

    # Glue Studio visual node: RDS target
    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=valid_records,
        catalog_connection=RDS_CONNECTION,
        connection_options={
            "database": RDS_DATABASE,
            "dbtable": f"{RDS_SCHEMA}.{config['table']}",
        },
        transformation_ctx=f"{metric_name}_rds_target",
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    for metric_name, config in TABLE_CONFIG.items():
        process_metric(glue_context, metric_name, config)

    job.commit()


if __name__ == "__main__":
    main()

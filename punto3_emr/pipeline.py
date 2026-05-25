from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


EVENT_TYPES = ["page_view", "click", "search", "product_view", "cart_event"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ShopStream PySpark analytics pipeline.")
    parser.add_argument("--input-path", required=True, help="S3 path with raw event CSV files.")
    parser.add_argument("--output-path", required=True, help="S3 path for processed Parquet outputs.")
    parser.add_argument("--date", required=True, help="Processing date in YYYY-MM-DD format.")
    return parser.parse_args()


def output_path(base_path: str, metric_name: str, process_date: str) -> str:
    return f"{base_path.rstrip('/')}/{metric_name}/date={process_date}/"


def write_parquet(df: DataFrame, base_path: str, metric_name: str, process_date: str) -> None:
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path(base_path, metric_name, process_date))
    )


def read_events(spark: SparkSession, input_path: str) -> DataFrame:
    base_path = input_path.rstrip("/")
    frames: list[DataFrame] = []

    for event_type in EVENT_TYPES:
        path = f"{base_path}/year=*/month=*/day=*/*{event_type}*.csv"
        frame = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
            .withColumn("event_type", F.lit(event_type))
            .withColumn("source_file", F.input_file_name())
        )
        frames.append(frame)

    events = frames[0]
    for frame in frames[1:]:
        events = events.unionByName(frame, allowMissingColumns=True)

    return events


def clean_events(events: DataFrame) -> DataFrame:
    cleaned = events.dropDuplicates(["user_id", "session_id", "timestamp", "event_type"])

    if "timestamp" in cleaned.columns:
        cleaned = cleaned.withColumn("timestamp", F.to_utc_timestamp(F.to_timestamp("timestamp"), "UTC"))

    if "time_on_page_seconds" in cleaned.columns:
        page_medians = (
            cleaned.filter(F.col("event_type") == "page_view")
            .groupBy("page_type")
            .agg(F.expr("percentile_approx(time_on_page_seconds, 0.5)").alias("median_time_on_page"))
        )
        cleaned = (
            cleaned.join(page_medians, on="page_type", how="left")
            .withColumn(
                "time_on_page_seconds",
                F.coalesce(F.col("time_on_page_seconds").cast("double"), F.col("median_time_on_page")),
            )
            .drop("median_time_on_page")
        )

    if "price" in cleaned.columns:
        price_medians = (
            cleaned.filter(F.col("event_type") == "product_view")
            .groupBy("category")
            .agg(F.expr("percentile_approx(price, 0.5)").alias("median_price"))
        )
        cleaned = (
            cleaned.join(price_medians, on="category", how="left")
            .withColumn("price", F.coalesce(F.col("price").cast("double"), F.col("median_price")))
            .drop("median_price")
        )

    return cleaned


def metric_top_pages(events: DataFrame) -> DataFrame:
    return (
        events.filter((F.col("event_type") == "page_view") & F.col("page_url").isNotNull())
        .groupBy("page_url")
        .agg(
            F.avg("time_on_page_seconds").alias("avg_time_on_page_seconds"),
            F.count("*").alias("event_count"),
        )
        .orderBy(F.desc("avg_time_on_page_seconds"))
        .limit(20)
    )


def metric_bounce_rate(events: DataFrame) -> DataFrame:
    page_views = events.filter(F.col("event_type") == "page_view")
    session_counts = (
        page_views.groupBy("session_id")
        .agg(
            F.count("*").alias("page_view_count"),
            F.first("page_type", ignorenulls=True).alias("page_type"),
        )
        .withColumn("is_bounced", F.when(F.col("page_view_count") == 1, F.lit(1)).otherwise(F.lit(0)))
    )
    return (
        session_counts.groupBy("page_type")
        .agg(
            F.sum("is_bounced").alias("bounced_sessions"),
            F.countDistinct("session_id").alias("total_sessions"),
        )
        .withColumn("bounce_rate", (F.col("bounced_sessions") / F.col("total_sessions")) * F.lit(100.0))
        .orderBy(F.desc("bounce_rate"))
    )


def metric_conversion_funnel(events: DataFrame) -> DataFrame:
    stage1 = events.filter(F.col("event_type") == "page_view").select("user_id").distinct()
    stage2 = events.filter(F.col("event_type") == "product_view").select("user_id").distinct()
    stage3 = (
        events.filter((F.col("event_type") == "cart_event") & (F.col("action") == "add"))
        .select("user_id")
        .distinct()
    )
    stage4 = (
        events.filter((F.col("event_type") == "page_view") & (F.col("page_type") == "checkout"))
        .select("user_id")
        .distinct()
    )

    counts = events.sql_ctx.sparkSession.createDataFrame(
        [
            (
                stage1.count(),
                stage2.count(),
                stage3.count(),
                stage4.count(),
            )
        ],
        ["stage1_page_view_users", "stage2_product_view_users", "stage3_cart_add_users", "stage4_checkout_users"],
    )
    return (
        counts.withColumn(
            "stage2_over_stage1_pct",
            F.when(F.col("stage1_page_view_users") > 0, F.col("stage2_product_view_users") / F.col("stage1_page_view_users") * 100),
        )
        .withColumn(
            "stage3_over_stage2_pct",
            F.when(F.col("stage2_product_view_users") > 0, F.col("stage3_cart_add_users") / F.col("stage2_product_view_users") * 100),
        )
        .withColumn(
            "stage4_over_stage3_pct",
            F.when(F.col("stage3_cart_add_users") > 0, F.col("stage4_checkout_users") / F.col("stage3_cart_add_users") * 100),
        )
    )


def metric_high_view_low_cart(events: DataFrame) -> DataFrame:
    product_views = events.filter(F.col("event_type") == "product_view")
    cart_adds = events.filter((F.col("event_type") == "cart_event") & (F.col("action") == "add"))

    views = (
        product_views.groupBy("product_id")
        .agg(
            F.first("category", ignorenulls=True).alias("category"),
            F.avg("price").alias("avg_price"),
            F.count("*").alias("view_count"),
        )
        .filter(F.col("product_id").isNotNull())
    )
    carts = cart_adds.groupBy("product_id").agg(F.count("*").alias("cart_add_count"))

    product_metrics = (
        views.join(carts, on="product_id", how="left")
        .fillna({"cart_add_count": 0})
        .withColumn("conversion_rate", F.col("cart_add_count") / F.col("view_count"))
    )

    quantiles = product_metrics.approxQuantile(["view_count", "cart_add_count"], [0.25, 0.75], 0.01)
    cart_p25 = quantiles[1][0] if quantiles and len(quantiles) > 1 and quantiles[1] else 0
    views_p75 = quantiles[0][1] if quantiles and quantiles[0] and len(quantiles[0]) > 1 else 0

    return (
        product_metrics.filter((F.col("view_count") > F.lit(views_p75)) & (F.col("cart_add_count") < F.lit(cart_p25)))
        .select("product_id", "category", "avg_price", "view_count", "cart_add_count", "conversion_rate")
        .orderBy(F.desc("view_count"), F.asc("cart_add_count"))
    )


def metric_navigation_paths(events: DataFrame) -> DataFrame:
    page_views = events.filter(F.col("event_type") == "page_view")
    ordered = page_views.withColumn(
        "page_event",
        F.struct(F.col("timestamp").alias("event_ts"), F.col("page_type").alias("page_type")),
    )
    paths = (
        ordered.groupBy("session_id")
        .agg(F.sort_array(F.collect_list("page_event")).alias("page_events"))
        .withColumn("path", F.concat_ws("→", F.transform("page_events", lambda item: item["page_type"])))
    )
    return paths.groupBy("path").agg(F.count("*").alias("session_count")).orderBy(F.desc("session_count")).limit(10)


def metric_device_country_time(events: DataFrame) -> DataFrame:
    return (
        events.filter(F.col("event_type") == "page_view")
        .groupBy("device_type", "country")
        .agg(
            F.avg("time_on_page_seconds").alias("avg_time_on_page_seconds"),
            F.countDistinct("session_id").alias("distinct_sessions"),
        )
        .orderBy("device_type", "country")
    )


def metric_anomalies(events: DataFrame) -> DataFrame:
    page_views = events.filter(F.col("event_type") == "page_view")
    stats_window = Window.partitionBy("page_type")
    with_stats = (
        page_views.withColumn("mean_time", F.avg("time_on_page_seconds").over(stats_window))
        .withColumn("std_time", F.stddev("time_on_page_seconds").over(stats_window))
        .withColumn(
            "z_score",
            F.when(F.col("std_time") > 0, (F.col("time_on_page_seconds") - F.col("mean_time")) / F.col("std_time")).otherwise(0.0),
        )
    )

    iqr_stats = (
        page_views.groupBy("page_type")
        .agg(
            F.expr("percentile_approx(time_on_page_seconds, 0.25)").alias("q1"),
            F.expr("percentile_approx(time_on_page_seconds, 0.75)").alias("q3"),
        )
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn("lower_bound", F.col("q1") - F.lit(1.5) * F.col("iqr"))
        .withColumn("upper_bound", F.col("q3") + F.lit(1.5) * F.col("iqr"))
    )

    anomalies = (
        with_stats.join(iqr_stats, on="page_type", how="left")
        .withColumn(
            "is_iqr_outlier",
            (F.col("time_on_page_seconds") < F.col("lower_bound"))
            | (F.col("time_on_page_seconds") > F.col("upper_bound")),
        )
        .withColumn(
            "anomaly_type",
            F.when((F.abs(F.col("z_score")) > 2) & F.col("is_iqr_outlier"), F.lit("z_score_and_iqr"))
            .when(F.abs(F.col("z_score")) > 2, F.lit("z_score"))
            .when(F.col("is_iqr_outlier"), F.lit("iqr")),
        )
        .filter((F.abs(F.col("z_score")) > 2) | (F.col("is_iqr_outlier") == F.lit(True)))
    )
    return anomalies.select(
        "session_id",
        "user_id",
        "page_type",
        "time_on_page_seconds",
        "z_score",
        "is_iqr_outlier",
        "anomaly_type",
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("ShopStreamPipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = clean_events(read_events(spark, args.input_path)).cache()
    events.count()

    metrics = {
        "top_pages": metric_top_pages(events),
        "bounce_rate": metric_bounce_rate(events),
        "conversion_funnel": metric_conversion_funnel(events),
        "high_view_low_cart": metric_high_view_low_cart(events),
        "navigation_paths": metric_navigation_paths(events),
        "device_country_time": metric_device_country_time(events),
        "anomalies": metric_anomalies(events),
    }

    for metric_name, frame in metrics.items():
        write_parquet(frame, args.output_path, metric_name, args.date)

    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

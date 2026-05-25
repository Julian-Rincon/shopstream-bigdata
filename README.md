# ShopStream Big Data Pipeline

![Python](https://img.shields.io/badge/python-3.11-blue)
![AWS](https://img.shields.io/badge/AWS-EMR%20%7C%20Lambda%20%7C%20RDS-orange)
![PySpark](https://img.shields.io/badge/PySpark-3.4-red)
![Build](https://github.com/Julian-Rincon/shopstream-bigdata/actions/workflows/ci.yml/badge.svg)

ShopStream is an end-to-end AWS big data pipeline for a fictional e-commerce platform. It generates synthetic behavioral data, validates ingestion, computes analytics with Spark, stores warehouse tables in PostgreSQL, and exposes business metrics through a Flask API deployed with Zappa.

## Architecture

The pipeline starts with synthetic ShopStream event generation in Python. Five event types are created locally as partitioned CSV files: `page_view`, `click`, `search`, `product_view`, and `cart_event`. The files are uploaded to the raw S3 bucket under `events/year=YYYY/month=MM/day=DD/`.

Each new S3 object triggers the `shopstream-validator` Lambda. The Lambda reads the CSV from S3, infers the event type from the file name, validates common and event-specific rules, emits CloudWatch metrics, and quarantines invalid files with an error report. Valid raw data remains in the raw zone.

An EMR Spark job reads the raw S3 events, cleans and deduplicates records, imputes missing values, and writes curated Parquet datasets to the processed S3 bucket. The analytics layer includes top pages, bounce rate, conversion funnel, high-view low-cart products, navigation paths, device-country session time, and anomaly detection.

The RDS PostgreSQL warehouse stores dimensional and fact tables for downstream analytics. Glue Studio is documented as the managed ETL path for mapping processed Parquet outputs into the warehouse, applying data quality checks, scheduling daily jobs, and sending failure alerts through SNS.

The API layer is a Flask application deployed on AWS Lambda/API Gateway with Zappa. It reads RDS credentials from environment variables, uses a PostgreSQL connection pool, and exposes analytics endpoints for dashboards or portfolio demos.

## Components

- `punto1_datagen/`: synthetic data generator and S3 upload script.
- `punto2_lambda/`: S3-triggered Lambda validator with CloudWatch metrics and quarantine handling.
- `punto3_emr/`: PySpark EMR pipeline that produces analytics Parquet datasets.
- `punto4_glue/`: RDS DWH schema and Glue Studio workflow documentation.
- `api/`: Flask API, Zappa configuration, and endpoint tests.
- `.github/workflows/ci.yml`: lint, test, and deploy pipeline for GitHub Actions.

## Local Setup

```bash
python -m pip install -r requirements.txt
python -m pip install -r api/requirements.txt
```

Create a local `.env` file with:

```text
AWS_REGION=us-east-1
S3_BUCKET_RAW=shopstream-raw-401466721010
S3_BUCKET_PROCESSED=shopstream-processed-401466721010
S3_BUCKET_SCRIPTS=shopstream-scripts-401466721010
RDS_HOST=<rds-endpoint>
RDS_PORT=5432
RDS_DB=shopstream_dwh
RDS_USER=shopstream_admin
RDS_PASSWORD=<password>
```

Run API tests:

```bash
pytest api/tests/ --cov=api --cov-report=term
```

Run the Flask API locally:

```bash
cd api
flask --app app run
```

## AWS Deployment

Generate and upload raw data:

```bash
python punto1_datagen/generate_data.py
python punto1_datagen/upload_to_s3.py
```

Deploy the Lambda validator by packaging `punto2_lambda/validator` and uploading the deployment zip to the scripts bucket. Configure the S3 notification in `punto2_lambda/s3_trigger.json`.

Run the EMR Spark pipeline:

```bash
aws emr create-cluster \
  --name "shopstream-pipeline" \
  --release-label emr-6.15.0 \
  --applications Name=Spark Name=Hadoop \
  --steps file://tmp_emr_steps.json \
  --auto-terminate \
  --region us-east-1
```

Create the RDS schema:

```bash
psql "host=$RDS_HOST port=5432 dbname=shopstream_dwh user=shopstream_admin" \
  -f punto4_glue/rds_schema.sql
```

Deploy the API:

```bash
cd api
zappa deploy production
```

## API Endpoints

Health check:

```bash
curl "$API_URL/health"
```

Top pages by time on page:

```bash
curl "$API_URL/pages/top?metric=time_on_page&date=2025-06-01&limit=5"
```

Top page types by bounce rate:

```bash
curl "$API_URL/pages/top?metric=bounce_rate&date=2025-06-01&limit=5"
```

Session summary by country and device:

```bash
curl "$API_URL/sessions/summary?country=Colombia&device=mobile&date=2025-06-01"
```

Anomalies:

```bash
curl "$API_URL/anomalies?date=2025-06-01"
```

## Tech Stack

- Python 3.11
- Pandas, NumPy, Faker
- AWS S3, Lambda, CloudWatch, EMR, Glue, RDS PostgreSQL, API Gateway
- PySpark
- Flask, psycopg2, Zappa
- GitHub Actions, pytest, flake8

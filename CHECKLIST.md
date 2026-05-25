# ShopStream — Checklist Final de Entrega

## Infraestructura AWS

| Componente | Detalle | Estado |
|---|---|---|
| S3 raw | 25 archivos CSV, 315.37 MB | ✅ |
| S3 processed | 7 métricas en Parquet, 19 objetos S3 | ✅ |
| Lambda validator | python3.11, 256MB, trigger S3, State=Active | ✅ |
| EMR cluster | j-1D7UG34TYVYLW, TERMINATED, ALL_STEPS_COMPLETED | ✅ |
| RDS PostgreSQL | db.t3.micro, postgres 15.18, available, 97,410 filas fact | ✅ |
| Glue ETL job | shopstream-etl-s3-to-rds, último run SUCCEEDED | ✅ |
| Glue visual ETL | Screenshot en punto4_glue/glue_studio_screenshot.png, grafo visual con Data Quality node | ✅ |
| Glue workflow | shopstream-workflow, schedule 2AM UTC + trigger condicional ACTIVATED | ✅ |
| API Gateway | /health + 3 endpoints analíticos, Lambda api-production en VPC | ✅ |
| CI/CD | GitHub Actions, run #9 green al verificar | ✅ |

## Endpoints API

Base URL: `https://cxuvlpa6jg.execute-api.us-east-1.amazonaws.com/production`

| Endpoint | Status | Datos |
|---|---|---|
| GET /health | 200 | `{"status":"ok","timestamp":"2026-05-25T08:15:20.999303+00:00"}` |
| GET /pages/top | 200 | 5 filas retornadas para `metric=time_on_page`, top page `/product/PROD-0368` |
| GET /sessions/summary | 200 | 14 filas retornadas para `device=mobile`; Colombia lidera con 157,791 sesiones |
| GET /anomalies | 200 | 97,228 anomalías totales; 100 más severas retornadas |
| GET /pages/top?metric=invalid | 400 | `{"error":"metric must be bounce_rate or time_on_page"}` |

## Tests

| Métrica | Valor |
|---|---|
| Tests pasados | 6/6 |
| Coverage | 86% |
| Flake8 | OK |

## Verificaciones de Seguridad

| Control | Resultado | Estado |
|---|---|---|
| `.env` fuera de Git | `git ls-files .env` no retorna archivos | ✅ |
| `.env.example` versionado | `git ls-files .env.example` retorna `.env.example` | ✅ |
| RDS SG 5432 | Sin `0.0.0.0/0`; demo IP `/32` + Lambda en VPC con SG interno | ✅ |

## Conteos RDS

| Tabla | Filas |
|---|---:|
| fact_top_pages | 40 |
| fact_bounce_rate | 10 |
| fact_conversion_funnel | 8 |
| fact_high_view_low_cart | 62 |
| fact_navigation_paths | 20 |
| fact_device_country_time | 42 |
| fact_anomalies | 97,228 |

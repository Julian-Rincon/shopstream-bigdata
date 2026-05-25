# ShopStream Big Data AWS

Proyecto académico de Big Data en AWS para un e-commerce ficticio llamado ShopStream.

## Punto 1: generación de datos sintéticos

El script `punto1_datagen/generate_data.py` genera eventos sintéticos de navegación y compra:

- `page_view`: 40%
- `product_view`: 25%
- `click`: 20%
- `search`: 10%
- `cart_event`: 5%

Por defecto genera 5 días, del 2025-06-01 al 2025-06-05, con 500,000 registros por día.

```bash
python punto1_datagen/generate_data.py
```

Salida esperada:

```text
data/year=2025/month=06/day=01/page_view.csv
data/year=2025/month=06/day=01/product_view.csv
data/year=2025/month=06/day=01/click.csv
data/year=2025/month=06/day=01/search.csv
data/year=2025/month=06/day=01/cart_event.csv
```

Para una prueba rápida:

```bash
python punto1_datagen/generate_data.py --records-per-day 1000 --days 1 --output-dir data_sample
```

## Estructura

```text
shopstream-bigdata/
├── .github/workflows/ci.yml
├── punto1_datagen/
│   ├── generate_data.py
│   └── upload_to_s3.py
├── punto2_lambda/validator/
├── punto3_emr/
├── punto4_glue/
├── api/tests/
├── requirements.txt
└── README.md
```

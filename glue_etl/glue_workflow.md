# ShopStream Glue Studio 4.0 Workflow

## 1. Crear Glue Database

1. Abrir AWS Console.
2. Ir a **AWS Glue**.
3. En el menú izquierdo, seleccionar **Data Catalog** > **Databases**.
4. Hacer clic en **Add database**.
5. En **Name**, escribir `shopstream_catalog`.
6. Dejar las demás opciones por defecto.
7. Hacer clic en **Create database**.

## 2. Crear conexión JDBC hacia RDS PostgreSQL

1. En **AWS Glue**, abrir **Data Catalog** > **Connections**.
2. Hacer clic en **Create connection**.
3. Seleccionar **JDBC** y hacer clic en **Next**.
4. En **JDBC URL**, usar:

   `jdbc:postgresql://<RDS_HOST>:5432/shopstream_dwh`

5. En **Username**, escribir `shopstream_admin`.
6. En **Password**, escribir la contraseña configurada en `.env`.
7. Seleccionar la VPC, subnet y security group donde está `shopstream-dwh`.
8. Hacer clic en **Create connection**.
9. Usar **Test connection** para validar conectividad.

## 3. Crear job visual ETL

1. En **AWS Glue**, abrir **ETL jobs**.
2. Hacer clic en **Visual ETL**.
3. Seleccionar **Visual with a source and target**.
4. En **Job name**, escribir `shopstream-etl`.
5. En **Glue version**, seleccionar **Glue 4.0**.
6. En **IAM Role**, seleccionar `LabRole`.
7. En el canvas, configurar los nodos:

### Nodo S3 source

1. Seleccionar el nodo **Amazon S3**.
2. En **S3 source type**, elegir **S3 location**.
3. En **S3 URL**, usar:

   `s3://shopstream-processed-401466721010/`

4. En **Data format**, seleccionar **Parquet**.
5. En **Recursive**, activar **Read files recursively**.

### Nodo ApplyMapping

1. Hacer clic en **+** y seleccionar **Transform** > **ApplyMapping**.
2. Conectar **Amazon S3** hacia **ApplyMapping**.
3. Mapear los campos según la tabla destino.
4. Usar tipos compatibles:
   - `date` -> `date`
   - métricas de conteo -> `int`
   - métricas decimales -> `decimal`
   - texto largo de rutas -> `string`

### Nodo Filter(válidos)

1. Hacer clic en **+** y seleccionar **Transform** > **Filter**.
2. Nombrar el nodo `Filter(validos)`.
3. Conectar **ApplyMapping** hacia **Filter(validos)**.
4. Agregar condición para conservar solo registros válidos:
   - `date IS NOT NULL`
   - cuando existan, `session_id IS NOT NULL`
   - cuando existan, `user_id IS NOT NULL`
   - campos de tiempo entre `0` y `3600`

### Nodo DataQuality

1. Hacer clic en **+** y seleccionar **Transform** > **Evaluate Data Quality**.
2. Conectar **Filter(validos)** hacia **Evaluate Data Quality**.
3. En **Ruleset**, usar reglas como:

```text
Rules = [
  ColumnExists "date",
  ColumnValues "date" IS NOT NULL,
  ColumnValues "session_id" IS NOT NULL,
  ColumnValues "user_id" IS NOT NULL,
  ColumnValues "time_on_page" BETWEEN 0 AND 3600,
  ColumnValues "avg_time_seconds" BETWEEN 0 AND 3600
]
```

4. Para tablas agregadas que no tienen `session_id`, `user_id` o `time_on_page`, crear un job por salida o desactivar esas reglas en esa rama.

### Nodo RDS target

1. Hacer clic en **+** y seleccionar **Target** > **JDBC**.
2. Conectar **Evaluate Data Quality** hacia **JDBC**.
3. En **Connection**, seleccionar la conexión JDBC creada para `shopstream-dwh`.
4. En **Database**, escribir `shopstream_dwh`.
5. En **Table**, escribir la tabla destino, por ejemplo `shopstream_dwh.fact_top_pages`.
6. En **Save mode**, seleccionar **Append**.
7. Guardar el job con **Save**.

## 4. Job load-to-rds

1. Crear un segundo job visual o script job llamado `load-to-rds`.
2. Usar las mismas conexiones S3 processed y RDS.
3. Configurar como destino las tablas `fact_*` y dimensiones `dim_*`.
4. Guardar el job.

## 5. Trigger condicional

1. Ir a **ETL jobs** > **Triggers**.
2. Hacer clic en **Create trigger**.
3. En **Name**, escribir `load-to-rds-after-etl`.
4. En **Trigger type**, seleccionar **Event**.
5. En **Trigger logic**, seleccionar **Start after ANY watched event**.
6. En **Watched events**, seleccionar el job `shopstream-etl`.
7. En **Condition**, seleccionar **SUCCEEDED**.
8. En **Actions**, seleccionar el job `load-to-rds`.
9. Hacer clic en **Create trigger**.

## 6. Trigger schedule diario

1. Ir a **ETL jobs** > **Triggers**.
2. Hacer clic en **Create trigger**.
3. En **Name**, escribir `shopstream-etl-daily-2am-utc`.
4. En **Trigger type**, seleccionar **Schedule**.
5. En **Frequency**, seleccionar **Custom cron**.
6. En **Cron expression**, escribir:

   `0 2 * * ? *`

7. En **Actions**, seleccionar el job `shopstream-etl`.
8. Hacer clic en **Create trigger**.

## 7. SNS alert si falla

1. Ir a **Amazon SNS**.
2. Abrir **Topics**.
3. Hacer clic en **Create topic**.
4. En **Type**, seleccionar **Standard**.
5. En **Name**, escribir `shopstream-alerts`.
6. Hacer clic en **Create topic**.
7. En el topic, hacer clic en **Create subscription**.
8. En **Protocol**, seleccionar **Email**.
9. En **Endpoint**, escribir el correo de notificación.
10. Confirmar la suscripción desde el correo recibido.
11. En **Amazon EventBridge**, crear una regla con patrón de evento para Glue Job State Change donde:
    - `detail.jobName` sea `shopstream-etl` o `load-to-rds`
    - `detail.state` sea `FAILED`, `TIMEOUT` o `STOPPED`
12. En **Target**, seleccionar **SNS topic** y elegir `shopstream-alerts`.

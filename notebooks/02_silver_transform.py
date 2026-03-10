# Databricks notebook source

# Imports

from pyspark.sql.functions import col, trim, to_date, expr, when, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

# Ler a Bronze V2
df_bronze_v2 = spark.table("workspace.drs_bronze.sales_v2")

# COMMAND ----------

# Padronizando os nomes das colunas
df_v2 = df_bronze_v2 \
.withColumnRenamed("id_da_fila", "row_id") \
.withColumnRenamed("id_do_pedido", "order_id") \
.withColumnRenamed("data_do_pedido", "order_date") \
.withColumnRenamed("data_de_envio", "ship_date") \
.withColumnRenamed("modo_de_envio", "ship_mode") \
.withColumnRenamed("id_do_cliente", "customer_id") \
.withColumnRenamed("nome_do_cliente", "customer_name") \
.withColumnRenamed("segmento", "segment") \
.withColumnRenamed("cidade", "city") \
.withColumnRenamed("estadoprovincia", "state") \
.withColumnRenamed("paisregiao", "country") \
.withColumnRenamed("regiao", "region") \
.withColumnRenamed("id_do_produto", "product_id") \
.withColumnRenamed("categoria", "category") \
.withColumnRenamed("subcategoria", "sub_category") \
.withColumnRenamed("nome_do_produto", "product_name") \
.withColumnRenamed("vendas", "sales") \
.withColumnRenamed("quantidade", "quantity") \
.withColumnRenamed("desconto", "discount") \
.withColumnRenamed("lucro", "profit") \
.withColumnRenamed("cep", "postal_code")

display(df_v2)


# COMMAND ----------

# Transformar/Convercet colunas para o padarão para depois salvar a Silver V2

from pyspark.sql.functions import col, trim, to_date, expr, when

df_silver_v2 = (
    df_v2
    .withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
    .withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))
    .withColumn("sales", expr("try_cast(sales as double)"))
    .withColumn("quantity", expr("try_cast(quantity as int)"))
    .withColumn("discount", expr("try_cast(discount as double)"))
    .withColumn("profit", expr("try_cast(profit as double)"))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("city", trim(col("city")))
    .withColumn("product_name", trim(col("product_name")))
)

# COMMAND ----------

# Separando registros inválidos para quarentena

df_invalid_v2 = df_silver_v2.filter("""
sales < 0
OR quantity <= 0
OR discount < 0
OR discount > 1
OR order_date IS NULL
OR ship_date IS NULL
OR order_date > ship_date
""")

# COMMAND ----------

# Salvando quarentena da Silver V2

df_invalid_v2.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_silver.sales_v2_quarantine")

# COMMAND ----------

# Filtrando valores válidos
df_silver_v2 = df_silver_v2.filter("""
sales IS NOT NULL
AND quantity IS NOT NULL
AND discount IS NOT NULL
AND profit IS NOT NULL
AND sales >= 0
AND quantity > 0
AND discount >= 0
AND discount <= 1
AND order_date IS NOT NULL
AND ship_date IS NOT NULL
AND order_date <= ship_date
""")

# COMMAND ----------

# Criando o campo profit_margem na v2

df_silver_v2 = df_silver_v2.withColumn(
    "profit_margin",
    when(col("sales") != 0, col("profit") / col("sales")).otherwise(None)
)

# COMMAND ----------

# Criando o campo silver_processed_timestamp em df_silver_v2
from pyspark.sql.functions import current_timestamp

df_silver_v2 = df_silver_v2 \
.withColumn("silver_processed_timestamp", current_timestamp()) \
.withColumn("created_at", current_timestamp()) \
.withColumn("updated_at", current_timestamp())

# COMMAND ----------

# Grantindo a deduplicação por chave de negócio

window_spec = Window.partitionBy(
    "order_id",
    "product_id"
).orderBy(
    col("ingestion_timestamp").desc()
)

df_silver_v2 = (
    df_silver_v2
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# Validando shema da dataframe v2
df_silver_v2.printSchema()

# COMMAND ----------

# Salvando a tabela Silver V2 em uma staging table

df_silver_v2.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("workspace.drs_silver.sales_v2_staging")

# COMMAND ----------


# Craindo tabela final se não existir
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.drs_silver.sales_v2
AS
SELECT * FROM workspace.drs_silver.sales_v2_staging WHERE 1 = 0
""")

# COMMAND ----------

# Validar schema da staging

spark.table("workspace.drs_silver.sales_v2_staging").printSchema()

# COMMAND ----------

# DBTITLE 1,Fix unsupported 'merge into' with correct mode
# Salvando tabela drs_silver_v2 com MERGE

spark.sql("""
MERGE INTO workspace.drs_silver.sales_v2 AS target
USING workspace.drs_silver.sales_v2_staging AS source

ON target.order_id = source.order_id
AND target.product_id = source.product_id

WHEN MATCHED THEN
UPDATE SET
    target.row_id = source.row_id,
    target.order_date = source.order_date,
    target.ship_date = source.ship_date,
    target.ship_mode = source.ship_mode,
    target.customer_id = source.customer_id,
    target.customer_name = source.customer_name,
    target.segment = source.segment,
    target.city = source.city,
    target.state = source.state,
    target.country = source.country,
    target.region = source.region,
    target.category = source.category,
    target.sub_category = source.sub_category,
    target.product_name = source.product_name,
    target.sales = source.sales,
    target.quantity = source.quantity,
    target.discount = source.discount,
    target.profit = source.profit,
    target.postal_code = source.postal_code,
    target.ingestion_timestamp = source.ingestion_timestamp,
    target.source_file = source.source_file,
    target.profit_margin = source.profit_margin,
    target.silver_processed_timestamp = source.silver_processed_timestamp,
    target.updated_at = current_timestamp()

WHEN NOT MATCHED THEN
INSERT (
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    city,
    state,
    country,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    postal_code,
    ingestion_timestamp,
    source_file,
    profit_margin,
    silver_processed_timestamp,
    created_at,
    updated_at
)
VALUES (
    source.row_id,
    source.order_id,
    source.order_date,
    source.ship_date,
    source.ship_mode,
    source.customer_id,
    source.customer_name,
    source.segment,
    source.city,
    source.state,
    source.country,
    source.region,
    source.product_id,
    source.category,
    source.sub_category,
    source.product_name,
    source.sales,
    source.quantity,
    source.discount,
    source.profit,
    source.postal_code,
    source.ingestion_timestamp,
    source.source_file,
    source.profit_margin,
    source.silver_processed_timestamp,
    current_timestamp(),
    current_timestamp()
)
""")

# COMMAND ----------

# Validando a tabela drs_silver_sales_v2

spark.sql("""
SELECT COUNT(*) AS total_rows
FROM workspace.drs_silver.sales_v2
""").display()

# COMMAND ----------

# Verificar duplicidade pela chave do merge

spark.sql("""
SELECT
    order_id,
    product_id,
    COUNT(*) AS qtd
FROM workspace.drs_silver.sales_v2
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
ORDER BY qtd DESC
""").display()

# COMMAND ----------

# Data Quality checks da Silver V2
dq = spark.sql("""
SELECT
    SUM(CASE WHEN sales < 0 THEN 1 ELSE 0 END) AS sales_negative,
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS quantity_invalid,
    SUM(CASE WHEN discount < 0 OR discount > 1 THEN 1 ELSE 0 END) AS discount_invalid,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS order_date_null,
    SUM(CASE WHEN ship_date IS NULL THEN 1 ELSE 0 END) AS ship_date_null,
    SUM(CASE WHEN order_date > ship_date THEN 1 ELSE 0 END) AS invalid_date_sequence
FROM workspace.drs_silver.sales_v2
""")

display(dq)

# COMMAND ----------

# validação quarentena
spark.sql("""
SELECT COUNT(*) AS total_invalid
FROM workspace.drs_silver.sales_v2_quarantine
""").display()

# COMMAND ----------

# Falhar notebook se houver erro crítico de qualidade
dq_row = dq.collect()[0]

if (
    dq_row["sales_negative"] > 0 or
    dq_row["quantity_invalid"] > 0 or
    dq_row["discount_invalid"] > 0 or
    dq_row["order_date_null"] > 0 or
    dq_row["ship_date_null"] > 0 or
    dq_row["invalid_date_sequence"] > 0
):
    raise Exception("Data Quality check failed in workspace.drs_silver.sales_v2")

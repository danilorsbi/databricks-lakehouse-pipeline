# Databricks notebook source
# Imports funções
from pyspark.sql.functions import sum, round, countDistinct

# COMMAND ----------

# Lendo a tabela silver que foi gravada
df = spark.table("workspace.drs_silver.sales_v2")
display(df)

# COMMAND ----------

# Validação silver_sales_v2

spark.sql("""
SELECT COUNT(*) AS total_rows
FROM workspace.drs_silver.sales_v2
""").display()

# COMMAND ----------

# lendo a estrutura da tabela drs-silver sales
 
df.printSchema() 

# COMMAND ----------

# Criando a tabela gold por região

df_region = (
    df
    .groupBy("region", "order_id")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers")
    )
)

display(df_region)

# COMMAND ----------

# Salvando a tabela gold by region
df_region.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.sales_by_region")

# COMMAND ----------

# Criando a tabela gold por categoria

df_category = (
    df
    .groupBy("category", "order_id")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers")
    )
)

display(df_category)

# COMMAND ----------

# Salvando a tabela gold by Category
df_category.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.sales_by_category")

# COMMAND ----------

# Criando a tabela gold por top produtos

df_products = (
    df
    .groupBy("product_name", "order_id", "category", "sub_category")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy("total_sales", ascending=False)
)

display(df_products)

# COMMAND ----------

# Salvando a tabela gold by Category
df_products.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.sales_by_top_products")

# COMMAND ----------

# lendo os dados da tabela sales-silver_by_products com join com tabelas sales_by_category e sales_by_region
spark.sql("""
SELECT *
FROM workspace.drs_gold.sales_by_top_products as p
INNER JOIN workspace.drs_gold.sales_by_category as c
ON c.order_id = p.order_id
inner JOIN workspace.drs_gold.sales_by_region as r
ON r.order_id = p.order_id
""").display()

# COMMAND ----------

#validando as tabelas gold
spark.sql("SHOW TABLES IN workspace.drs_gold").display()


# COMMAND ----------

### Criando Gold V2 com base na Silver V2
df_v2 = spark.table("workspace.drs_silver.sales_v2")
display(df_v2)

# COMMAND ----------

# Criando a tabela gold_sales_by_region_v2
df_region_v2 = (
    df_v2
    .groupBy("region")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers")
    )

)

# COMMAND ----------

# Salvando a tabela drs_gold.sales_by_region_v2
df_region_v2.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.sales_by_region_v2")

# COMMAND ----------

# Criando a tabela gold_sales_by_region_v2
df_category_v2 = (
    df_v2
    .groupBy("category")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers")
    )

)

# COMMAND ----------

# Salvando a tabela drs_gold.sales_by_region_v2
df_category_v2.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.sales_by_category_v2")

# COMMAND ----------

# Criando a tabela gold_sales_by_products_v2
df_products_v2 = (
    df_v2
    .groupBy("product_name", "category", "sub_category")
    .agg(
        round(sum("sales"), 2).alias("total_sales"),
        round(sum("profit"), 2).alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy("total_sales", ascending=False)
)

# COMMAND ----------

# Criando a tabela gold_sales_by_products_v2
df_products_v2.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.top_products_v2")

# COMMAND ----------

# comando para renomear tabelas
# %python
# spark.sql("""
# ALTER TABLE workspace.drs_gold.sales_by_top_products
# RENAME TO workspace.drs_gold.sales_by_top_products
# """)
# renomeando a tabela sales_by_products

# COMMAND ----------

# DBTITLE 1,Fix: wrap SQL statements for drop table
# Deletando as tabelas criadas quando necessário

#spark.sql("DROP TABLE IF EXISTS workspace.drs_gold.sales_by_top_products")
#spark.sql("DROP TABLE IF EXISTS workspace.drs_gold.sales_by_region")
#spark.sql("DROP TABLE IF EXISTS workspace.drs_gold.sales_by_category")

#spark.sql("DROP TABLE IF EXISTS workspace.drs_silver.sales_v2")
#spark.sql("DROP TABLE IF EXISTS workspace.drs_silver.sales_v2_staging")

#spark.sql("DROP TABLE IF EXISTS workspace.drs_bronze.sales")
#spark.sql("DROP TABLE IF EXISTS workspace.drs_silver.sales")

# COMMAND ----------

#deletando os schemas

#spark.sql("DROP SCHEMA IF EXISTS workspace.drs_bronze")
#spark.sql("DROP SCHEMA IF EXISTS workspace.drs_silver")
#spark.sql("DROP SCHEMA IF EXISTS workspace.drs_gold")
#spark.sql("DROP SCHEMA IF EXISTS workspace.dev")

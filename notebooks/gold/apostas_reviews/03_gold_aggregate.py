# Databricks notebook source
# Imports funções
from pyspark.sql.functions import sum, round, countDistinct, avg, count, when, col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# Lendo a tabela silver que foi gravada

df = spark.read.table("workspace.drs_silver.reviews_apps")
display(df)

# COMMAND ----------

# Validação silver_reviews_apps

spark.sql("""
SELECT COUNT(*) AS total_rows
FROM workspace.drs_silver.reviews_apps
""").display()

# COMMAND ----------

# lendo a estrutura da tabela drs_silver.reviews_apps

df.printSchema()

# COMMAND ----------

# Criando a tabela gold de KPIs por app e platform

df_kpi = (
    df
    .groupBy("app_name", "platform")
    .agg(
        count("*").alias("total_reviews"),
        round(avg("rating"), 2).alias("rating_media"),
        sum(when(col("rating_category") == "positiva", 1).otherwise(0)).alias("qtd_positivas"),
        sum(when(col("rating_category") == "neutra", 1).otherwise(0)).alias("qtd_neutras"),
        sum(when(col("rating_category") == "negativa", 1).otherwise(0)).alias("qtd_negativas"),
        round(sum(when(col("rating_category") == "positiva", 1).otherwise(0)) * 100.0 / count("*"), 2).alias("pct_positivas"),
        sum("likes").alias("total_likes")
    )
)

display(df_kpi)

# COMMAND ----------

# Salvando a tabela gold kpi_app_platform
df_kpi.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.reviews_kpi_app_platform")

# COMMAND ----------

# Criando a tabela gold de evolução mensal

df_mensal = (
    df
    .groupBy("app_name", "platform", "year_month")
    .agg(
        count("*").alias("qtd_reviews"),
        round(avg("rating"), 2).alias("rating_avg_month"),
        round(sum(when(col("rating_category") == "negativa", 1).otherwise(0)) * 100.0 / count("*"), 2).alias("pct_negativas")
    )
    .orderBy("app_name", "platform", "year_month")
)

display(df_mensal)

# COMMAND ----------

# Salvando a tabela gold reviews_evolucao_mensal
df_mensal.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.reviews_evolucao_mensal")

# COMMAND ----------

# Criando a tabela gold de temas por app

df_temas = (
    df
    .groupBy("app_name", "platform")
    .agg(
        sum(when(col("mentions_withdrawal"), 1).otherwise(0)).alias("mencoes_saque"),
        sum(when(col("mentions_deposit"), 1).otherwise(0)).alias("mencoes_deposito"),
        sum(when(col("mentions_bonus"), 1).otherwise(0)).alias("mencoes_bonus"),
        sum(when(col("mentions_support"), 1).otherwise(0)).alias("mencoes_suporte"),
        sum(when(col("mentions_bug"), 1).otherwise(0)).alias("mencoes_bug"),
        round(avg(when(col("mentions_withdrawal"), col("rating"))), 2).alias("rating_media_saque"),
        round(avg(when(col("mentions_deposit"), col("rating"))), 2).alias("rating_media_deposito"),
        round(avg(when(col("mentions_bonus"), col("rating"))), 2).alias("rating_media_bonus"),
        round(avg(when(col("mentions_support"), col("rating"))), 2).alias("rating_media_suporte"),
        round(avg(when(col("mentions_bug"), col("rating"))), 2).alias("rating_media_bug")
    )
)
display(df_temas)

# COMMAND ----------

# Salvando a tabela gold reviews_temas_por_app
df_temas.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.reviews_temas_por_app")

# COMMAND ----------

# Criando a tabela gold de top reviews mais curtidos (Voz do cliente)

window_top = Window.partitionBy("app_name").orderBy(col("likes").desc())

df_top_reviews = (
    df
    .filter((col("platform") == "Android") & (col("likes") > 0))
    .select(
        "app_name",
        "platform",
        "user",
        "rating",
        "rating_category",
        "review_date",
        "likes",
        "comment"
    )
    .withColumn("rank_likes", row_number().over(window_top))
    .filter(col("rank_likes") <= 50)
    .drop("rank_likes")
    .orderBy("app_name", col("likes").desc())
)

display(df_top_reviews)

# COMMAND ----------

# Salvando a tabela gold reviews_top_curtidos
df_top_reviews.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.reviews_top_curtidos")

# COMMAND ----------

# lendo os dados das tabelas gold com join entre KPI e Temas
spark.sql("""
SELECT
    k.app_name,
    k.platform,
    k.total_reviews,
    k.rating_media,
    k.pct_positivas,
    t.mencoes_saque,
    t.mencoes_deposito,
    t.mencoes_bonus,
    t.mencoes_suporte,
    t.mencoes_bug
FROM workspace.drs_gold.reviews_kpi_app_platform AS k
INNER JOIN workspace.drs_gold.reviews_temas_por_app AS t
ON k.app_name = t.app_name
AND k.platform = t.platform
ORDER BY k.app_name, k.platform
""").display()

# COMMAND ----------

#validando as tabelas gold
spark.sql("SHOW TABLES IN workspace.drs_gold").display()

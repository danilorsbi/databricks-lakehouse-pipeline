# Databricks notebook source
# Imports

from pyspark.sql.functions import col, trim, to_timestamp, expr, when, current_timestamp, lit, lower, regexp_replace, length, date_format, to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col

# COMMAND ----------

# Ler a Bronze
df_bronze = spark.table("workspace.drs_bronze.reviews_apps")
display(df_bronze)

# COMMAND ----------

# Padronizando os nomes das colunas (já vêm em snake_case do Autoloader, mas garantimos consistência)
df = df_bronze \
    .withColumnRenamed("app_nome", "app_name") \
    .withColumnRenamed("app_id", "app_id") \
    .withColumnRenamed("plataforma", "platform") \
    .withColumnRenamed("review_id", "review_id") \
    .withColumnRenamed("usuario", "user") \
    .withColumnRenamed("nota", "rating") \
    .withColumnRenamed("data_review", "review_date") \
    .withColumnRenamed("comentario", "comment") \
    .withColumnRenamed("curtidas", "likes") \
    .withColumnRenamed("versao_app", "app_version") \
    .withColumnRenamed("resposta_dev", "dev_response")

display(df)


# COMMAND ----------

# Transformar/Converter colunas para o padrão para depois salvar a Silver

df_silver = (
    df
    .withColumn("review_date", to_timestamp(col("review_date")))
    .withColumn("rating", expr("try_cast(rating as int)"))
    .withColumn("likes", expr("try_cast(likes as int)"))
    .withColumn("user", trim(col("user")))
    .withColumn("comment", trim(col("comment")))
    .withColumn("app_name", trim(col("app_name")))
    .withColumn("platform", trim(col("platform")))
)


# COMMAND ----------

# criando o campo source_file (mantém o do bronze que veio do Autoloader)
# já existe como source_file vindo da Bronze, mantemos

# COMMAND ----------

# Separando registros inválidos para quarentena

df_invalid = df.filter("""
rating IS NULL
OR rating < 1
OR rating > 5
OR comment IS NULL
OR length(trim(comment)) = 0
OR review_date IS NULL
OR app_id IS NULL
OR platform IS NULL
""")

# COMMAND ----------

# Salvando quarentena da Silver

df_invalid.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("workspace.drs_silver.reviews_apps_quarantine")

# COMMAND ----------

# criando o campo ingestion_timestamp (mantém o vindo da Bronze)
# já existe como ingestion_timestamp na Bronze, mantemos

# COMMAND ----------

# Filtrando valores válidos

df_silver = (
    df_silver.dropDuplicates()
      .filter(
          (col("rating").isNotNull()) &
          (col("rating") >= 1) &
          (col("rating") <= 5) &
          (col("comment").isNotNull()) &
          (length(trim(col("comment"))) > 0) &
          (col("review_date").isNotNull()) &
          (col("app_id").isNotNull()) &
          (col("platform").isNotNull())
      )
)

display(df_silver)


# COMMAND ----------

# Criando o campo rating_category

df_silver = df_silver.withColumn(
    "rating_category",
    when(col("rating") >= 4, "positiva")
    .when(col("rating") == 3, "neutra")
    .otherwise("negativa")
)

# COMMAND ----------

# Criando campos de enriquecimento por temas (regex flags)

df_silver = (
    df_silver
    .withColumn("clean_comment", lower(regexp_replace(col("comment"), r"\s+", " ")))
    .withColumn("comment_length", length(col("comment")))
    .withColumn("mentions_withdrawal",
                col("clean_comment").rlike(r"saque|sacar|retirada|retirar|pix"))
    .withColumn("mentions_deposit",
                col("clean_comment").rlike(r"deposit|recarga"))
    .withColumn("mentions_bonus",
                col("clean_comment").rlike(r"b[ôo]nus|bonus|promo"))
    .withColumn("mentions_support",
                col("clean_comment").rlike(r"suporte|atendimento|chat|sac"))
    .withColumn("mentions_bug",
                col("clean_comment").rlike(r"bug|trava|travando|fecha sozinho|crash|erro"))
)


# COMMAND ----------

# Criando campos de tempo para análise

df_silver = (
    df_silver
    .withColumn("dt_review", to_date(col("review_date")))
    .withColumn("year_month", date_format(col("review_date"), "yyyy-MM"))
)

# COMMAND ----------

# Criando o campo silver_processed_timestamp em df_silver

df_silver = df_silver \
.withColumn("silver_processed_timestamp", current_timestamp()) \
.withColumn("created_at", current_timestamp()) \
.withColumn("updated_at", current_timestamp())

# COMMAND ----------

# Garantindo a deduplicação por chave de negócio
# Para Android usamos review_id; para iOS usamos hash de campos

from pyspark.sql.functions import sha2, concat_ws, coalesce

df_silver = df_silver.withColumn(
    "dedup_key",
    when(col("review_id").isNotNull(), col("review_id"))
    .otherwise(sha2(concat_ws("||",
        col("platform"),
        col("app_id"),
        coalesce(col("user"), lit("")),
        coalesce(col("review_date").cast("string"), lit("")),
        coalesce(col("comment"), lit(""))
    ), 256))
)

window_spec = Window.partitionBy(
    "platform",
    "app_id",
    "dedup_key"
).orderBy(
    col("ingestion_timestamp").desc()
)

df_silver = (
    df_silver
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)


# COMMAND ----------

# Validando schema da dataframe
df_silver.printSchema()

# COMMAND ----------

# Salvando a tabela Silver em uma staging table

df_silver.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("workspace.drs_silver.reviews_apps_staging")

# COMMAND ----------

# Criando tabela final se não existir

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.drs_silver.reviews_apps
AS
SELECT * FROM workspace.drs_silver.reviews_apps_staging WHERE 1 = 0
""")

# COMMAND ----------

# Validar schema da staging

spark.table("workspace.drs_silver.reviews_apps_staging").printSchema()

# COMMAND ----------

# DBTITLE 1,Salvando tabela drs_silver.reviews_apps com MERGE
# MAGIC
# MAGIC %sql
# MAGIC -- Salvando tabela drs_silver.reviews_apps com MERGE
# MAGIC MERGE INTO workspace.drs_silver.reviews_apps AS target
# MAGIC USING workspace.drs_silver.reviews_apps_staging AS source
# MAGIC
# MAGIC ON target.platform = source.platform
# MAGIC AND target.app_id = source.app_id
# MAGIC AND target.dedup_key = source.dedup_key
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.app_name = source.app_name,
# MAGIC     target.review_id = source.review_id,
# MAGIC     target.user = source.user,
# MAGIC     target.rating = source.rating,
# MAGIC     target.review_date = source.review_date,
# MAGIC     target.comment = source.comment,
# MAGIC     target.likes = source.likes,
# MAGIC     target.app_version = source.app_version,
# MAGIC     target.dev_response = source.dev_response,
# MAGIC     target.rating_category = source.rating_category, -- Target(PT) <- Source(EN)
# MAGIC     target.clean_comment = source.clean_comment,
# MAGIC     target.comment_length = source.comment_length,
# MAGIC     target.mentions_withdrawal = source.mentions_withdrawal,
# MAGIC     target.mentions_deposit = source.mentions_deposit,
# MAGIC     target.mentions_bonus = source.mentions_bonus,
# MAGIC     target.mentions_support = source.mentions_support,
# MAGIC     target.mentions_bug = source.mentions_bug,
# MAGIC     target.dt_review = source.dt_review,           -- Ambos estão como dt_review
# MAGIC     target.year_month = source.year_month,               -- Ambos estão como year_month
# MAGIC     target.ingestion_timestamp = source.ingestion_timestamp,
# MAGIC     target.source_file = source.source_file,
# MAGIC     target.silver_processed_timestamp = source.silver_processed_timestamp,
# MAGIC     target.updated_at = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     app_name, app_id, platform, review_id, user, rating, review_date, 
# MAGIC     comment, likes, app_version, dev_response, rating_category, 
# MAGIC     clean_comment, comment_length, mentions_withdrawal, mentions_deposit, 
# MAGIC     mentions_bonus, mentions_support, mentions_bug, dt_review, year_month, 
# MAGIC     dedup_key, ingestion_timestamp, source_file, silver_processed_timestamp, 
# MAGIC     created_at, updated_at
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.app_name, source.app_id, source.platform, source.review_id, source.user, source.rating, source.review_date, 
# MAGIC     source.comment, source.likes, source.app_version, source.dev_response, source.rating_category, 
# MAGIC     source.clean_comment, source.comment_length, source.mentions_withdrawal, source.mentions_deposit, 
# MAGIC     source.mentions_bonus, source.mentions_support, source.mentions_bug, source.dt_review, source.year_month, 
# MAGIC     source.dedup_key, source.ingestion_timestamp, source.source_file, source.silver_processed_timestamp, 
# MAGIC     current_timestamp(), current_timestamp()
# MAGIC   )
# MAGIC
# MAGIC

# COMMAND ----------

df = spark.sql("""
SELECT * 
FROM workspace.drs_silver.reviews_apps
""")

display(df)

# COMMAND ----------

# Validando a tabela drs_silver.reviews_apps

spark.sql("""
SELECT COUNT(*) AS total_rows
FROM workspace.drs_silver.reviews_apps
""").display()

# COMMAND ----------

# Verificar duplicidade pela chave do merge

spark.sql("""
SELECT
    platform,
    app_id,
    dedup_key,
    COUNT(*) AS qtd
FROM workspace.drs_silver.reviews_apps
GROUP BY platform, app_id, dedup_key
HAVING COUNT(*) > 1
ORDER BY qtd DESC
""").display()

# COMMAND ----------

# Data Quality checks da Silver
dq = spark.sql("""
SELECT
    SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS rating_null,
    SUM(CASE WHEN rating < 1 OR rating > 5 THEN 1 ELSE 0 END) AS nota_invalid,
    SUM(CASE WHEN comment IS NULL OR length(trim(comment)) = 0 THEN 1 ELSE 0 END) AS comment_invalid,
    SUM(CASE WHEN review_date IS NULL THEN 1 ELSE 0 END) AS review_date_null,
    SUM(CASE WHEN app_id IS NULL THEN 1 ELSE 0 END) AS app_id_null,
    SUM(CASE WHEN platform IS NULL THEN 1 ELSE 0 END) AS platform_null
FROM workspace.drs_silver.reviews_apps
""")

display(dq)

# COMMAND ----------

# validação quarentena
spark.sql("""
SELECT COUNT(*) AS total_invalid
FROM workspace.drs_silver.reviews_apps_quarantine
""").display()

# COMMAND ----------

# Falhar notebook se houver erro crítico de qualidade
dq_row = dq.collect()[0]

if (
    dq_row["rating_null"] > 0 or
    dq_row["nota_invalid"] > 0 or
    dq_row["comment_invalid"] > 0 or
    dq_row["review_date_null"] > 0 or
    dq_row["app_id_null"] > 0 or
    dq_row["platform_null"] > 0
):
    raise Exception("Data Quality check failed in workspace.drs_silver.reviews_apps")

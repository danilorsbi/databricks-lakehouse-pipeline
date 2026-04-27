# Databricks notebook source
# DBTITLE 1,Atualiza criação dos schemas para drs_bronze, drs_silver, drs_gold
# Cria schemas se ainda não existirem
# Criando schemas no workspace
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.drs_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.drs_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.drs_gold")

# COMMAND ----------

# importar bibliotecas
import re
import unicodedata
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

# Cria as pastas necessárias dentro do volume

dbutils.fs.mkdirs("/Volumes/workspace/pipeline_estudo/raw_files/input/reviews_apps/")
dbutils.fs.mkdirs("/Volumes/workspace/pipeline_estudo/raw_files/checkpoints/bronze_reviews_apps/")

# COMMAND ----------

#👉 Função para padronizar nomes de colunas

def clean_column_name(col_name):
    col_name = unicodedata.normalize('NFKD', col_name)
    col_name = col_name.encode('ascii', 'ignore').decode('utf-8')
    col_name = col_name.lower()
    col_name = col_name.replace(" ", "_")
    col_name = re.sub(r'[^a-z0-9_]', '', col_name)
    return col_name

# COMMAND ----------

# 👉 Lê os arquivos JSON da pasta input com Autoloader

source_path = "/Volumes/workspace/pipeline_estudo/raw_files/input/reviews_apps"
 
schema = (
    spark.read
    .option("multiLine", False)
    .json(source_path)
    .schema
)

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .option("multiLine", False)
    .load(source_path)
)

df = df.toDF(*[clean_column_name(c) for c in df.columns])

# COMMAND ----------

# Adiciona colunas técnicas da Bronze

df_bronze = (
    df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# COMMAND ----------

# limpa a tabela Bronze para recarga controlada
spark.sql("DROP TABLE IF EXISTS workspace.drs_bronze.reviews_apps")

# COMMAND ----------

# apaga o checkpoint antigo para reiniciar o controle do stream
dbutils.fs.rm("/Volumes/workspace/pipeline_estudo/raw_files/checkpoints/bronze_reviews_apps/", True)
dbutils.fs.mkdirs("/Volumes/workspace/pipeline_estudo/raw_files/checkpoints/bronze_reviews_apps/")

# COMMAND ----------

# Grava o stream na tabela Bronze final
(df_bronze.writeStream
 .format("delta")
 .option("checkpointLocation", "/Volumes/workspace/pipeline_estudo/raw_files/checkpoints/bronze_reviews_apps/")
 .trigger(availableNow=True)
 .toTable("workspace.drs_bronze.reviews_apps"))

# COMMAND ----------

# Valida a tabela Bronze final
spark.sql("""
SELECT *
FROM workspace.drs_bronze.reviews_apps
LIMIT 10
""").display()

# COMMAND ----------

# Conta os registros da Bronze
spark.sql("""
SELECT COUNT(*) AS total_bronze
FROM workspace.drs_bronze.reviews_apps
""").display()

# COMMAND ----------

# Valida duplicidade por chave de negócio

spark.sql("""
SELECT
    plataforma,
    app_id,
    review_id,
    usuario,
    data_review,
    COUNT(*) AS qtd
FROM workspace.drs_bronze.reviews_apps
GROUP BY plataforma, app_id, review_id, usuario, data_review
HAVING COUNT(*) > 1
ORDER BY qtd DESC
""").display()

# Databricks notebook source
# DBTITLE 1,Atualiza criação dos schemas para drs_bronze, drs_silver, drs_gold
#criando os chemas dentro do workspace
#spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.drs_bronze")
#spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.drs_silver")
#spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.drs_gold")

# COMMAND ----------

# Definindo o caminho do nosso arquivo e criando o dataframe
file_path = "/Volumes/workspace/pipeline_estudo/raw_files/sales.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(file_path)
)

display(df)

# COMMAND ----------

# importar bibliotecas
import re
import unicodedata

# COMMAND ----------

# Criar função de limpeza
def clean_column_name(col_name):

    # Remover acentos
    col_name = unicodedata.normalize('NFKD', col_name)
    col_name = col_name.encode('ascii', 'ignore').decode('utf-8')

    # Minúsculo
    col_name = col_name.lower()

    # Substituir espaços por _
    col_name = col_name.replace(" ", "_")

    # Remover caracteres especiais
    col_name = re.sub(r'[^a-z0-9_]', '', col_name)

    return col_name

# COMMAND ----------

# DBTITLE 1,Fix SyntaxError in function application
# Aplicar no dataframe
df = df.toDF(*[clean_column_name(c) for c in df.columns])


# COMMAND ----------

print(df.columns)

# COMMAND ----------

# Salvar tabela bronze
df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_bronze.sales")


# COMMAND ----------

# Verificando a estrututra da tabela
df = spark.sql("""
          select * from workspace.drs_bronze.sales
          """)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## De agora em diante vamos transformar nossa pipeline em PRODUCTION-GRADE PIPELINE
# MAGIC
# MAGIC Com:
# MAGIC
# MAGIC ✔ Ingestão incremental
# MAGIC ✔ Delta MERGE
# MAGIC ✔ idempotência
# MAGIC ✔ schema evolution
# MAGIC ✔ data quality checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estratégia
# MAGIC
# MAGIC Vamos criar uma v2 com estes pilares:
# MAGIC
# MAGIC - ambiente paralelo, sem sobrescrever o que já funciona
# MAGIC - carga incremental, em vez de overwrite cego
# MAGIC - MERGE/UPSERT na Silver
# MAGIC - controles de auditoria, como data de ingestão
# MAGIC - qualidade de dados
# MAGIC - job separado para testes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como ficará a nova arquitetura
# MAGIC
# MAGIC Vamos manter a atual:
# MAGIC
# MAGIC - workspace.drs_bronze.sales
# MAGIC - workspace.drs_silver.sales
# MAGIC - workspace.drs_gold.*
# MAGIC
# MAGIC E criar uma nova trilha:
# MAGIC
# MAGIC - workspace.drs_bronze.sales_v2
# MAGIC - workspace.drs_silver.sales_v2
# MAGIC - workspace.drs_gold.sales_by_region_v2
# MAGIC - workspace.drs_gold.sales_by_category_v2
# MAGIC - workspace.drs_gold.top_products_v2

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que muda da pipeline atual para a production-grade
# MAGIC
# MAGIC Hoje você faz, em essência:
# MAGIC
# MAGIC read csv
# MAGIC - overwrite bronze
# MAGIC - overwrite silver
# MAGIC - overwrite gold
# MAGIC
# MAGIC Na v2 vamos caminhar para:
# MAGIC
# MAGIC read csv
# MAGIC - append/ingest bronze com metadados
# MAGIC - merge silver por chave de negócio
# MAGIC - rebuild ou merge gold

# COMMAND ----------

# Primeiro, vamos adicionar metadados de ingestão.
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# Criando um novo dataframe a partir do df já padronizado

df_bronze_v2 = (
    df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("sales.csv"))
)

# COMMAND ----------

# Salvando a tabela sales drs_bronze.sales_v2

df_bronze_v2.write \
.format("delta") \
.mode("append") \
.saveAsTable("workspace.drs_bronze.sales_v2")


# COMMAND ----------

# Validando a tabela drs_bronze.sales_v2
spark.sql("""
SELECT *
FROM workspace.drs_bronze.sales_v2
LIMIT 10
""").display()

# COMMAND ----------

# Contando registros da tabela sales_v2 na bronze
spark.sql("""
SELECT COUNT(*) AS total_bronze_v2
FROM workspace.drs_bronze.sales_v2
""").display()


# COMMAND ----------

# Verificar lotes por arquivo e ingestão
spark.sql("""
SELECT
    source_file,
    date_trunc('minute', ingestion_timestamp) AS ingestion_minute,
    COUNT(*) AS total_rows
FROM workspace.drs_bronze.sales_v2
GROUP BY source_file, date_trunc('minute', ingestion_timestamp)
ORDER BY ingestion_minute DESC
""").display()

# COMMAND ----------

# Validar duplicidade por chave de negócio
spark.sql("""
SELECT
    id_do_pedido,
    id_do_produto,
    COUNT(*) AS qtd
FROM workspace.drs_bronze.sales_v2
GROUP BY id_do_pedido, id_do_produto
HAVING COUNT(*) > 1
ORDER BY qtd DESC
""").display()
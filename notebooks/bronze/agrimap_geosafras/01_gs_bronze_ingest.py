# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# =============================================================
#  GEO SAFRAS · 01_gs_bronze_ingest.py
#  Ingestão Raw → Bronze
#  Fonte: /Volumes/workspace/pipeline_estudo/raw_files/
#         fazenda_santa_lucia_padre_bernardo_go/
#  Tabelas: gs_bronze.pedidos
#            gs_bronze.calendario_safras
#            gs_bronze.saude_financeira
# =============================================================

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

RAW_PATH   = "/Volumes/workspace/pipeline_estudo/raw_files/geo_safras/"
CATALOG    = "workspace"
SCHEMA_B   = "gs_bronze"
SOURCE     = "pipeline_estudo_fazenda_santa_lucia"
NOW        = datetime.now().isoformat()

print(f"[Bronze] Início: {NOW}")
print(f"[Bronze] Fonte : {RAW_PATH}")

# COMMAND ----------

# ─── HELPER: ler CSV ─────────────────────────────────────────
def ler_csv(nome_arquivo: str):
    path = f"{RAW_PATH}/{nome_arquivo}"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("encoding", "utf-8")
          .option("multiLine", "true")
          .option("dateFormat", "yyyy-MM-dd")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
          .csv(path))
    df = (df
          .withColumn("ingestion_timestamp", F.lit(NOW).cast("timestamp"))
          .withColumn("source_file", F.lit(nome_arquivo)))
    # Remover BOM do nome da primeira coluna se existir
    primeiro_col = df.columns[0]
    if primeiro_col.startswith('﻿') or primeiro_col.startswith('ï»¿'):
        df = df.withColumnRenamed(primeiro_col, primeiro_col.lstrip('﻿').lstrip('ï»¿'))

    print(f"  ✓ {nome_arquivo}: {df.count()} linhas, {len(df.columns)} colunas")
    return df

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
#  1. PEDIDOS
# ═══════════════════════════════════════════════════════════════

# COMMAND ----------

df_pedidos = ler_csv("pedidos.csv")

# Renomear colunas para snake_case limpo
df_pedidos = (df_pedidos
    .withColumnRenamed("ID do pedido",      "id_pedido")
    .withColumnRenamed("Data do pedido",    "data_pedido")
    .withColumnRenamed("Data de envio",     "data_envio")
    .withColumnRenamed("cultura",           "cultura")
    .withColumnRenamed("Cidade",            "cidade")
    .withColumnRenamed("localidade",        "localidade")
    .withColumnRenamed("Estado/Província",  "estado")
    .withColumnRenamed("País/Região",       "pais")
    .withColumnRenamed("Região",            "regiao")
    .withColumnRenamed("subcultura 2",      "subcultura2")
    .withColumnRenamed("subcultura",        "subcultura")
    .withColumnRenamed("Vendas",            "vendas")
    .withColumnRenamed("Quantidade",        "quantidade")
    .withColumnRenamed("Desconto",          "desconto")
    .withColumnRenamed("Lucro",             "lucro")
    .withColumnRenamed("formato",           "formato")
    .withColumnRenamed("custo_total",       "custo_total")
    .withColumnRenamed("margem_liquida_pct","margem_liquida_pct")
    .withColumnRenamed("preco_por_unidade", "preco_por_unidade")
    .withColumnRenamed("custo_por_unidade", "custo_por_unidade")
    .withColumnRenamed("lucro_por_unidade", "lucro_por_unidade")
    .withColumnRenamed("ano",               "ano")
    .withColumnRenamed("mes",               "mes")
    .withColumnRenamed("trimestre",         "trimestre")
    .withColumnRenamed("safra",             "safra")
    .withColumnRenamed("categoria_margem",  "categoria_margem")
    .withColumnRenamed("impacto_desconto",  "impacto_desconto")
    .withColumnRenamed("custo_insumos_est",        "custo_insumos_est")
    .withColumnRenamed("custo_fertilizantes_est",  "custo_fertilizantes_est")
    .withColumnRenamed("custo_defensivos_est",      "custo_defensivos_est")
    .withColumnRenamed("custo_outros_est",          "custo_outros_est")
    .withColumnRenamed("cepea_referencia_sc",       "cepea_referencia_sc")
    .withColumnRenamed("gap_preco_cepea",           "gap_preco_cepea")
    .withColumnRenamed("perc_vs_cepea",             "perc_vs_cepea")
    .withColumnRenamed("receita_perdida_cepea",     "receita_perdida_cepea")
    .withColumnRenamed("lucro_justo_cepea",         "lucro_justo_cepea")
)

# Tratar lucro_justo_cepea — célula AJ4 contém token Databricks acidentalmente
# Forçar cast para double; valor inválido vira NULL
df_pedidos = df_pedidos.withColumn(
    "lucro_justo_cepea",
    F.col("lucro_justo_cepea").cast("double")
)

# Filtrar linhas sem id_pedido
invalidos_pedidos = df_pedidos.filter(F.col("id_pedido").isNull()).count()
if invalidos_pedidos > 0:
    print(f"  ⚠️  {invalidos_pedidos} linhas sem id_pedido descartadas")
df_pedidos = df_pedidos.filter(F.col("id_pedido").isNotNull())

# Deduplicar por id_pedido — o MERGE exige no máximo 1 linha de origem
# por linha de destino. Se o mesmo id_pedido aparecer mais de uma vez
# no CSV, mantemos só a última ocorrência (pela ordem do arquivo).
from pyspark.sql import Window
window_dedup = Window.partitionBy("id_pedido").orderBy(F.monotonically_increasing_id().desc())

duplicados = df_pedidos.count() - df_pedidos.dropDuplicates(["id_pedido"]).count()
if duplicados > 0:
    print(f"  ⚠️  {duplicados} linhas duplicadas por id_pedido removidas")

df_pedidos = (df_pedidos
    .withColumn("_rn", F.row_number().over(window_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn"))

# Coalesce: o CSV de origem é um arquivo único, dado de uma única
# fazenda (volume pequeno). Sem isso, a tabela Delta pode acabar
# fragmentada em vários arquivos pequenos à toa. Reduzimos para 1
# arquivo, sem pagar custo de shuffle completo (coalesce só combina
# partições vizinhas, não redistribui tudo pela rede como o repartition).
df_pedidos = df_pedidos.coalesce(1)

# MERGE Bronze — chave: id_pedido
if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA_B}.pedidos"):
    dt = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA_B}.pedidos")
    (dt.alias("tgt")
       .merge(df_pedidos.alias("src"), "tgt.id_pedido = src.id_pedido")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())
    print("  ✓ pedidos: MERGE concluído")
else:
    df_pedidos.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_B}.pedidos")
    print("  ✓ pedidos: criada e carregada")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
#  2. CALENDARIO_SAFRAS
# ═══════════════════════════════════════════════════════════════

# COMMAND ----------

df_cal = ler_csv("calendario_safras.csv")

# Tratar datas
for col_data in ["mes_data_inicio", "mes_data_fim", "data_inicio_estagio", "data_fim_estagio"]:
    df_cal = df_cal.withColumn(col_data, F.col(col_data).cast("date"))

# Coalesce: tabela de calendário/referência, cardinalidade baixa
# (cultura x talhão x ano x safra x mês). Volume pequeno, 1 arquivo
# é suficiente e evita fragmentação desnecessária.
df_cal = df_cal.coalesce(1)

# MERGE Bronze — chave composta
chave_cal = """
    tgt.cultura      = src.cultura      AND
    tgt.talhao       = src.talhao       AND
    tgt.ano          = src.ano          AND
    tgt.safra_tipo   = src.safra_tipo   AND
    tgt.mes_num      = src.mes_num
"""

if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA_B}.calendario_safras"):
    dt = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA_B}.calendario_safras")
    (dt.alias("tgt")
       .merge(df_cal.alias("src"), chave_cal)
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())
    print("  ✓ calendario_safras: MERGE concluído")
else:
    df_cal.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_B}.calendario_safras")
    print("  ✓ calendario_safras: criada e carregada")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
#  3. SAUDE_FINANCEIRA
# ═══════════════════════════════════════════════════════════════

# COMMAND ----------

df_sf = ler_csv("saude_financeira_simulada.csv")

# Coalesce: granularidade (fazenda, ano) — poucas linhas, mesmo
# raciocínio das duas tabelas acima: 1 arquivo é suficiente.
df_sf = df_sf.coalesce(1)

# MERGE Bronze — chave: (fazenda, ano)
chave_sf = "tgt.fazenda = src.fazenda AND tgt.ano = src.ano"

if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA_B}.saude_financeira"):
    dt = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA_B}.saude_financeira")
    (dt.alias("tgt")
       .merge(df_sf.alias("src"), chave_sf)
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())
    print("  ✓ saude_financeira: MERGE concluído")
else:
    df_sf.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_B}.saude_financeira")
    print("  ✓ saude_financeira: criada e carregada")

# COMMAND ----------

print(f"\n[Bronze] ✅ Ingestão concluída em {datetime.now().isoformat()}")
print(f"  Tabelas atualizadas:")
print(f"  • {CATALOG}.{SCHEMA_B}.pedidos")
print(f"  • {CATALOG}.{SCHEMA_B}.calendario_safras")
print(f"  • {CATALOG}.{SCHEMA_B}.saude_financeira")

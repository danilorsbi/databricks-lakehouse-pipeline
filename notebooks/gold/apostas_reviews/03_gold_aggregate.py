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
    .groupBy("app_name", "platform", "review_date")
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
  .option("mergeSchema", "true") \
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

# COMMAND ----------

# criando a tabela frequencia x severidade

from pyspark.sql.functions import col, count, avg, round, expr, lit, sum as spark_sum, when

# 1) Cada review entra em N linhas (uma por tema mencionado) - usando UNION ALL
df_renomeado = df.selectExpr(
    "app_name as app_nome",
    "platform as plataforma",
    "rating as nota",
    "mentions_withdrawal as menciona_saque",
    "mentions_deposit as menciona_deposito",
    "mentions_bonus as menciona_bonus",
    "mentions_support as menciona_suporte",
    "mentions_bug as menciona_bug"
)

temas_long = (
    df_renomeado.filter(col("menciona_saque") == True).withColumn("tema", lit("saque"))
    .select("app_nome", "plataforma", "nota", "tema")
    .unionByName(
        df_renomeado.filter(col("menciona_deposito") == True).withColumn("tema", lit("deposito"))
        .select("app_nome", "plataforma", "nota", "tema")
    )
    .unionByName(
        df_renomeado.filter(col("menciona_bonus") == True).withColumn("tema", lit("bonus"))
        .select("app_nome", "plataforma", "nota", "tema")
    )
    .unionByName(
        df_renomeado.filter(col("menciona_suporte") == True).withColumn("tema", lit("suporte"))
        .select("app_nome", "plataforma", "nota", "tema")
    )
    .unionByName(
        df_renomeado.filter(col("menciona_bug") == True).withColumn("tema", lit("bug"))
        .select("app_nome", "plataforma", "nota", "tema")
    )
)

# 2) Total de reviews por app (denominador da frequencia)
totais_app = (
    df.groupBy("app_name", "platform")
    .agg(count("*").alias("total_reviews_app"))
    .selectExpr("app_name as app_nome", "platform as plataforma", "total_reviews_app")
)

# 3) Agregacao com TODAS as informacoes acionaveis
df_freq_severidade = (
    temas_long.join(totais_app, ["app_nome", "plataforma"])
    .groupBy("app_nome", "plataforma", "tema", "total_reviews_app")
    .agg(
        count("*").alias("qtd_mencoes"),
        round(avg("nota"), 2).alias("nota_media_tema"),
        # Distribuicao das notas dentro do tema
        spark_sum(when(col("nota") == 1, 1).otherwise(0)).alias("qtd_nota_1"),
        spark_sum(when(col("nota") == 2, 1).otherwise(0)).alias("qtd_nota_2"),
        spark_sum(when(col("nota") == 3, 1).otherwise(0)).alias("qtd_nota_3"),
        spark_sum(when(col("nota") == 4, 1).otherwise(0)).alias("qtd_nota_4"),
        spark_sum(when(col("nota") == 5, 1).otherwise(0)).alias("qtd_nota_5"),
    )
    # Calcula freq, severidade e priority_score
    .withColumn("freq_pct", round(col("qtd_mencoes") * 100.0 / col("total_reviews_app"), 2))
    .withColumn("severidade", round(lit(5) - col("nota_media_tema"), 2))
    .withColumn("priority_score", round(col("freq_pct") * col("severidade"), 2))
    # Quadrante
    .withColumn(
        "quadrante",
        when((col("freq_pct") >= 10) & (col("severidade") >= 2.5), "1_RESOLVER_JA")
        .when((col("freq_pct") >= 10) & (col("severidade") <  2.5), "2_BAIXA_PRIORIDADE")
        .when((col("freq_pct") <  10) & (col("severidade") >= 2.5), "3_MONITORAR")
        .otherwise("4_IGNORAR")
    )
    # Acao recomendada (texto descritivo para quem olha o painel)
    .withColumn(
        "acao_recomendada",
        when(col("quadrante") == "1_RESOLVER_JA",
             expr("concat('CRITICO: ', qtd_mencoes, ' reviews de ', app_nome, ' (', plataforma, ') sobre ', tema, ' - nota media ', cast(nota_media_tema as string))"))
        .when(col("quadrante") == "3_MONITORAR",
             expr("concat('VIGIAR: ', qtd_mencoes, ' reviews mas dor alta - ', tema, ' em ', app_nome)"))
        .when(col("quadrante") == "2_BAIXA_PRIORIDADE",
             expr("concat('AGUARDAR: ', qtd_mencoes, ' reviews mas dor baixa - ', tema, ' em ', app_nome)"))
        .otherwise(
             expr("concat('IGNORAR: ', qtd_mencoes, ' reviews irrelevantes - ', tema, ' em ', app_nome)"))
    )
    # Ranking dentro de cada quadrante
    .withColumn(
        "rank_no_quadrante",
        expr("ROW_NUMBER() OVER (PARTITION BY quadrante ORDER BY priority_score DESC)")
    )
    .orderBy(
        # Ordena por quadrante (1 primeiro) e depois por priority_score
        col("quadrante"),
        col("priority_score").desc()
    )
)

display(df_freq_severidade)

# COMMAND ----------

# salvando a tabela frequencia x severidade
df_freq_severidade.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("workspace.drs_gold.reviews_freq_severidade")

# COMMAND ----------

# top 3 reviews mais curtidas por app

df_top_10_reviews = spark.sql("""

select 
    app_name,
    user,
    rating,
    likes,
    left(comment, 80) as comment_preview, -- pega os comentários com 80 caracteres iniciais
    row_number() over (partition by app_name order by likes desc) as row_num, -- criar um rankin com valores (1,2,3,4)  
    rank()       over (partition by app_name order by likes desc) as rank, -- empata e pula (1,1,3,4)
    dense_rank() over (partition by app_name order by likes desc) as dense_rank -- empata sem pular (1,1,2,3)
from workspace.drs_silver.reviews_apps    
where 
platform = 'Android'
and 
likes > 0 
qualify row_num <= 10 -- limita o valor do top que pode ser alterado
order by app_name, row_num
""")


display(df_top_10_reviews)        

# COMMAND ----------

# Salvando a tabela gold reviews_top_curtidos
df_top_10_reviews.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("workspace.drs_gold.reviews_top_10_curtidos")

# COMMAND ----------

# criando a média movel de notas dos últimos 7 dias

df_moving_average_last_7_days = (
    spark.sql("""
   
        select 
            app_name,
            user,
            dt_review,
            rating,
            -- média móvel do último 7 dias 
            round(
                avg(rating) over(
                    partition by app_name
                    order by dt_review
                    rows between 6 PRECEDING and CURRENT ROW -- aqui defino que pega a linha a tual menos 6 linhas anteriores
                ),
                2
             ) AS media_movel_7d, 
        -- soma acumulada de likes
        sum(likes) over(
                partition by app_name
                order by dt_review
                rows unbounded preceding
            ) as like_acumulados,

        -- diferença da nota vs review anterior do mesmo app
            rating - lag(rating, 1) over( -- com lag podemos pegar o valor anterior
                partition by app_name
                order by dt_review
            ) as delta_vs_anterior
        from workspace.drs_silver.reviews_apps    
        where app_name = 'Betano'
        order by dt_review
""")
)

display(df_moving_average_last_7_days)

# COMMAND ----------

# Salvando a tabela gold reviews_top_curtidos

df_moving_average_last_7_days.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("workspace.drs_gold.moving_average_last_7_days")

# COMMAND ----------

df_words_frequency_tokenization =  spark.sql("""
SELECT
    app_name,
    dt_review,
    user,
    COUNT(*) AS qtd_reviews_no_dia,
    SUM(rating) AS soma_ratings,
    ROUND(AVG(rating), 2) AS nota_media,
    SUM(likes) AS total_likes,
    -- Concatena todos os comentarios do mesmo (app, dia, user)
    CONCAT_WS(' ||| ', COLLECT_LIST(comment)) AS comentarios_concatenados,
    -- Versao limpa (sem acentos, lowercase) para analise
    CONCAT_WS(' ||| ', COLLECT_LIST(clean_comment)) AS comentarios_limpos,
    -- Flags acumuladas (se ALGUM dos reviews mencionou)
    MAX(CASE WHEN mentions_withdrawal THEN 1 ELSE 0 END) AS mencionou_saque,
    MAX(CASE WHEN mentions_deposit THEN 1 ELSE 0 END) AS mencionou_deposito,
    MAX(CASE WHEN mentions_bonus THEN 1 ELSE 0 END) AS mencionou_bonus,
    MAX(CASE WHEN mentions_support THEN 1 ELSE 0 END) AS mencionou_suporte,
    MAX(CASE WHEN mentions_bug THEN 1 ELSE 0 END) AS mencionou_bug,
    -- Total de palavras nos comentarios (proxy de engajamento textual)
    SUM(comment_length) AS total_caracteres_escritos,
    -- Tem alguma resposta do dev?
    MAX(CASE WHEN dev_response IS NOT NULL AND LENGTH(TRIM(dev_response)) > 0 THEN 1 ELSE 0 END) AS teve_resposta_dev
FROM workspace.drs_silver.reviews_apps
WHERE comment IS NOT NULL
  AND LENGTH(TRIM(comment)) > 0
GROUP BY app_name, dt_review, user
ORDER BY soma_ratings ASC, total_likes DESC
""")


display(df_words_frequency_tokenization)

# COMMAND ----------

df_problem_consolidated = spark.sql("""
CREATE OR REPLACE TABLE workspace.drs_gold.reviews_explorer AS
SELECT
    app_name,
    platform,
    dt_review,
    year_month,
    user,

    -- Volumes
    COUNT(*) AS qtd_reviews_no_grupo,
    SUM(rating) AS soma_ratings,
    ROUND(AVG(rating), 2) AS nota_media,
    SUM(likes) AS total_likes,

    -- Categorizacao geral
    SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS qtd_negativos,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS qtd_positivos,

    -- Comentarios concatenados (para leitura humana)
    CONCAT_WS(' ||| ', COLLECT_LIST(comment)) AS comentarios_originais,
    CONCAT_WS(' ||| ', COLLECT_LIST(clean_comment)) AS comentarios_limpos,

    -- Flags conhecidas (qualquer review do grupo mencionou?)
    MAX(CASE WHEN mentions_withdrawal THEN 1 ELSE 0 END) AS tem_saque,
    MAX(CASE WHEN mentions_deposit THEN 1 ELSE 0 END) AS tem_deposito,
    MAX(CASE WHEN mentions_bonus THEN 1 ELSE 0 END) AS tem_bonus,
    MAX(CASE WHEN mentions_support THEN 1 ELSE 0 END) AS tem_suporte,
    MAX(CASE WHEN mentions_bug THEN 1 ELSE 0 END) AS tem_bug,

    -- Flag mais importante: "orfao" = nenhum tema conhecido
    CASE
        WHEN MAX(CASE WHEN mentions_withdrawal OR mentions_deposit
                       OR mentions_bonus OR mentions_support
                       OR mentions_bug THEN 1 ELSE 0 END) = 0
        THEN TRUE
        ELSE FALSE
    END AS eh_orfao,

    -- Total caracteres escritos (proxy de engajamento)
    SUM(comment_length) AS total_caracteres,

    -- Engajamento da operadora
    MAX(CASE WHEN dev_response IS NOT NULL AND LENGTH(TRIM(dev_response)) > 0 THEN 1 ELSE 0 END) AS teve_resposta_dev,

    -- Padroes interessantes que escapam das flags atuais (RLIKE direto no comentario)
    MAX(CASE WHEN clean_comment RLIKE r'\\bdocument(o|os|aca|acao)\\b|\\bverifica(r|cao)\\b|\\bidentidad' THEN 1 ELSE 0 END) AS menciona_kyc,
    MAX(CASE WHEN clean_comment RLIKE r'\\blent(o|a|idao)\\b|\\bdemora\\b|\\btravad' THEN 1 ELSE 0 END) AS menciona_lentidao,
    MAX(CASE WHEN clean_comment RLIKE r'\\bcancel(ou|ad|ar)\\b|\\bbloque(ou|ad|ar)\\b|\\bsuspen(s|d)' THEN 1 ELSE 0 END) AS menciona_cancelamento,
    MAX(CASE WHEN clean_comment RLIKE r'\\bvic(io|iad)\\b|\\baddict|\\bvic(iei|iou)\\b' THEN 1 ELSE 0 END) AS menciona_vicio,
    MAX(CASE WHEN clean_comment RLIKE r'\\bgolp(e|ist)|\\broub(o|ou|ar)\\b|\\bfraud' THEN 1 ELSE 0 END) AS menciona_fraude,
    MAX(CASE WHEN clean_comment RLIKE r'\\bperdi\\b|\\bperde(r|u)\\b|\\bdinheir' THEN 1 ELSE 0 END) AS menciona_perda

FROM workspace.drs_silver.reviews_apps
WHERE comment IS NOT NULL
  AND LENGTH(TRIM(comment)) > 0
GROUP BY app_name, platform, dt_review, year_month, user
""")


display(df_problem_consolidated)

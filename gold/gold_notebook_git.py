# Databricks notebook source
# MAGIC %md
# MAGIC #Lab Prático
# MAGIC
# MAGIC Workshop de Boas Práticas no Desenvolvimento de Software com Notebooks Databricks
# MAGIC
# MAGIC #####Notebook Gold:
# MAGIC
# MAGIC Agregações e transformações com os dados da camada silver em formatos otimizados para consumo final, como relatórios, dashboards ou modelos de machine learning, atendendo a requisitos específicos de negócios.

# COMMAND ----------

# Imports do Notebook Databricks

from pyspark.sql.functions import month, avg

# COMMAND ----------

# Leitura de dados tratados do Delta Lake

df_silver = spark.read.format("delta").load("/mnt/silver/nyc_taxi")

# COMMAND ----------

# Agregacao: Calcular receita media por mes

df_gold = (
    df_silver
    .groupBy(month("pickup_datetime").alias("month"))
    .agg(avg("total_amount").alias("avg_revenue"))
)


# COMMAND ----------

# Escrita de dados agregados para o Delta Lake

df_gold.write.format("delta").mode("overwrite").save("/mnt/gold/nyc_taxi")


# COMMAND ----------

# Exibicao dos resultados

display(df_gold)


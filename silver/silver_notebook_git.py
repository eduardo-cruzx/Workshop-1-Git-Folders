# Databricks notebook source
# MAGIC %md
# MAGIC #Lab Prático
# MAGIC
# MAGIC Workshop de Boas Práticas no Desenvolvimento de Software com Notebooks Databricks
# MAGIC
# MAGIC #####Notebook Silver:
# MAGIC
# MAGIC Processamento dos dados da camada bronze, realizando limpeza, enriquecimento e transformações intermediárias para torná-los mais estruturados e prontos para análises ou agregações específicas.

# COMMAND ----------

# Imports do Notebook Databricks

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

# Leitura de dados brutos do Delta Lake

df_bronze = spark.read.format("delta").load("/mnt/bronze/nyc_taxi")


# COMMAND ----------

# Limpeza e transformacao dos dados

df_silver = (
    df_bronze
    .filter("passenger_count > 0 AND trip_distance > 0")
    .select(
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "total_amount"
    )
    .withColumn("pickup_datetime", to_timestamp("tpep_pickup_datetime"))
    .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
)

# COMMAND ----------

# Validação: Número de passageiros deve ser maior ou igual a 1

df_silver = df_silver.filter(col("passenger_count") >= 1)


# COMMAND ----------

# Adicionando uma coluna de classificação de receita 

df_silver = df_silver.withColumn(
    "revenue_category",
    when(col("total_amount") < 10, "Small")
    .when((col("total_amount") >= 10) & (col("total_amount") < 50), "Average")
    .otherwise("High")
)

# COMMAND ----------

# Escrita de dados processados para o Delta Lake

df_silver.write.format("delta").mode("overwrite").save("/mnt/silver/nyc_taxi_updated")


# COMMAND ----------

# Exibicao dos resultados

display(df_silver)


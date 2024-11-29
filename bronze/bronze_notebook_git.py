# Databricks notebook source
# MAGIC %md
# MAGIC #Lab Prático
# MAGIC
# MAGIC Workshop de Boas Práticas no Desenvolvimento de Software com Notebooks Databricks
# MAGIC
# MAGIC #####Notebook Bronze:
# MAGIC
# MAGIC Ingestão de dados brutos de fontes diversas, aplicando validações mínimas e armazenando-os em formato bruto ou levemente processado no Delta Lake. Serve como a camada inicial do pipeline.

# COMMAND ----------

# Imports do Notebook Databricks

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType
from pyspark.sql.functions import col


# COMMAND ----------

# Definicao do Schema

schema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', LongType(), True), 
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', LongType(), True), 
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True), 
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', LongType(), True), 
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True), 
    StructField('mta_tax', DoubleType(), True), 
    StructField('tip_amount', DoubleType(), True), 
    StructField('tolls_amount', DoubleType(), True), 
    StructField('improvement_surcharge', DoubleType(), True), 
    StructField('total_amount', DoubleType(), True), 
    StructField('congestion_surcharge', DoubleType(), True), 
    StructField('Airport_fee', DoubleType(), True)
])

# COMMAND ----------

# Leitura de Dados Brutos em Parquet

file_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

dbutils.fs.cp(file_url, "/mnt/data/nyc_taxi.parquet")

df_raw = spark.read.schema(schema).parquet("/mnt/data/nyc_taxi.parquet")

display(df_raw)


# COMMAND ----------

# Validação: Detectar registros invalidos

df_invalid = df_raw.filter(
    (col("passenger_count").isNull()) |
    (col("trip_distance").isNull()) |
    (col("total_amount").isNull())
)

# COMMAND ----------

# Contar e mostrar registros inválidos

invalid_count = df_invalid.count()
print(f"Número de registros inválidos: {invalid_count}")

# COMMAND ----------

# Salvar registros inválidos para auditoria

if invalid_count > 0:
    df_invalid.write.format("delta").mode("overwrite").save("/mnt/bronze/invalid_nyc_taxi")


# COMMAND ----------

# Filtrar registros válidos

df_bronze = df_raw.filter(
    (col("passenger_count").isNotNull()) &
    (col("trip_distance").isNotNull()) &
    (col("total_amount").isNotNull())
)

# COMMAND ----------

# Escrita de dados brutos no Delta Lake

df_bronze.write.format("delta").mode("overwrite").save("/mnt/bronze/nyc_taxi")


# COMMAND ----------

# Exibicao dos resultados

display(spark.read.format("delta").load("/mnt/bronze/nyc_taxi"))


import datetime
import os
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/classes')
import DataIngestion
from google.cloud import bigquery
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging as log
import uuid

PROJECT_ID = "serious-glyph-328219"
BUCKET = "etl-use-case-gcp"
LAYER = "bronze_layer"
DATA_SOURCE_TABLE = "public.customers"
BRONZE_LAYER_TABLE = "olist_customers"

spark = SparkSession \
    .builder \
    .appName("DataIngestionJob") \
    .config("spark.jars", 
        "/home/ayres/Documents/projects/use-case-gcp-etl/configs/postgresql-42.7.3.jar, \
        https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .getOrCreate()

client = bigquery.Client()

def get_batch_id():

    batch_id = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-" + str(uuid.uuid4())

    #tratamento para extrair o ingestion time (TIMESTAMP)
    ingestion_time = '-'.join(batch_id.split('-')[:6])
    ingestion_time = datetime.datetime.strptime(ingestion_time, "%Y-%m-%d-%H-%M-%S")
    ingestion_time = ingestion_time.isoformat()

    return batch_id, ingestion_time       

batch_id, ingestion_time = get_batch_id()

PATH = f"gs://{BUCKET}/{LAYER}/olist/{DATA_SOURCE_TABLE}/{batch_id}"

consulta_sql = f"""
SELECT batch_id
FROM {PROJECT_ID}.{LAYER}.{BRONZE_LAYER_TABLE}
"""

df_destino = client.query(consulta_sql).to_dataframe()

df_origem = spark.read.parquet(PATH)

df_origem_filtrado = df_origem.select("batch_id").distinct()
df_destino_filtrado = df_destino.select("batch_id").distinct()

df_duplicatas = df_origem_filtrado.join(df_destino_filtrado, on="batch_id", how="inner")

if df_duplicatas.count() == 0:
    print("dados inseridos")

   

import datetime
import os
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/classes')
import DataIngestion
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging as log
import uuid

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ayres/.config/gcloud/application_default_credentials.json"

spark = SparkSession \
    .builder \
    .appName("DataIngestionJob") \
    .config("spark.jars", 
        "/home/ayres/Documents/projects/use-case-gcp-etl/configs/postgresql-42.7.3.jar, \
        https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .getOrCreate()

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

data_ingestor = DataIngestion.DataIngestor("sql", "olist")

db_properties = data_ingestor.read_data()

def job_ingestion_name():
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-" + str(uuid.uuid4())

BUCKET = "etl-use-case-gcp"
LAYER = "bronze_layer"
TABLE = "public.customers"
jobname = job_ingestion_name()
PATH = f"gs://{BUCKET}/{LAYER}/{db_properties['name']}/{TABLE}/{jobname}"


try:
    df = spark.read \
        .format("jdbc") \
        .option("url", f"{db_properties['url']}") \
        .option("dbtable", f"{TABLE}") \
        .option("user", f"{db_properties['user']}") \
        .option("password", f"{db_properties['password']}") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    #tabela nao esta apta ao uso de ingestao incremental (sem campos timestamp), portanto sera feito ingestao full. 
    #Em ambiente Big Data, algumas abordagens poderiam ser realizadas para atenuar o problema como criacao de indices (caso nao exista), bem como,
    #a insercao de colunas de controle tipo timestamp como: inserted_at, updated_at ou last_update

    #df.write.partitionBy("<col_date>").format("parquet").save("gs://<caminho_storage>")

    df.write.format("parquet").save(PATH)

except Exception as e:
    log.error("Ocorreu um erro ao salvar os dados no Google Cloud Storage: ",e)



#spark.stop()
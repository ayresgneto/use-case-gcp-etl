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

def get_batch_id():
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-" + str(uuid.uuid4())

batch_id = get_batch_id()

#extraindo data do batch_id e formatando string em timestamp
ingestion_time = '-'.join(batch_id.split('-')[:6])
ingestion_time = datetime.datetime.strptime(ingestion_time, "%Y-%m-%d-%H-%M-%S")
ingestion_time = ingestion_time.isoformat()


def write_metadata_batches(bucket,layer, data_source, table, batch_id, ingestion_time):
    
    

    try:
        client = bigquery.Client()

        table_ref = client.dataset("metadata").table("batches")
        metadata_table = client.get_table(table_ref)

        data = [{'bucket': bucket, 'layer': layer, 'data_source': data_source, 'table': table, 'batch_id': batch_id, 'ingestion_time': ingestion_time}]

        errors = client.insert_rows(metadata_table, data)

        if errors == []:
            print("job details inserted in metadata.batches!")
        else:
            print("error: ", errors)
    except Exception as e:
        log.error("Error: ",e)

BUCKET = "etl-use-case-gcp"
LAYER = "bronze_layer"
TABLE = "public.customers"

PATH = f"gs://{BUCKET}/{LAYER}/{db_properties['name']}/{TABLE}/{batch_id}"

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
    log.info(f"ingestion job sucess!")
    write_metadata_batches(BUCKET,LAYER,"olist",TABLE, batch_id, ingestion_time)
    spark.stop()

except Exception as e:
    log.error("Error: ",e)

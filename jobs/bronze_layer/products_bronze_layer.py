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

BUCKET = "etl-use-case-gcp"
LAYER = "bronze_layer"
TABLE = "public.products"

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

def validating_and_ingest(bucket,layer, data_source, table, batch_id, ingestion_time):

    client = bigquery.Client()
    
    table_ref = client.dataset("metadata").table("batches")
    metadata_table = client.get_table(table_ref)

    def data_ingestion():

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

            df.write.format("parquet").save(PATH)
            log.info(f"ingestion job sucess!")
            data = [{'bucket': bucket, 'layer': layer, 'data_source': data_source, 'table': table, 'batch_id': batch_id, 'ingestion_time': ingestion_time}]

            errors = client.insert_rows(metadata_table, data)

            if errors == []:
                print("job details inserted in metadata.batches!")
            else:
                print("error: ", errors)
            
            spark.stop()


        except Exception as e:
                log.error("Error: ",e)

    def check_batch_ingested(batch_id):

        # Consulta SQL para verificar se todos os registros no batch estão ingeridos
        query = f"""
        SELECT COUNT(*) AS total
        FROM 'metadata.batches'
        WHERE batch_id = '{batch_id}' AND NOT ingested
        """

        result = client.query(query).result()
        for row in result:
            if row.total == 0:
                return True
        return False
    
    def set_ingested_status(batch_id):
        # Função para marcar o campo 'ingested' como True após a ingestão dos dados
        query = f"""
        UPDATE 'metadata.batches' 
        SET ingested = TRUE
        WHERE batch_id = '{batch_id}'
        """

        client.query(query).result()
                            
    def check_ingest_new_data(batch_id):

        # Verifique se o batch está ingerido antes de ingerir novos dados
        if check_batch_ingested(batch_id):
            print("batch ingested. Ignoring new data....")
        else:
            print("Ingesting new data...")
            data_ingestion()
    








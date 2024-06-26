
import os
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/classes')
import DataIngestion
from google.cloud import bigquery
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging as log


PROJECT_ID = "serious-glyph-328219"
BUCKET = "etl-use-case-gcp"
LAYER = "bronze_layer"
DATASOURCE = "olist"
#DATA_SOURCE_TABLE = "public.customers"
BRONZE_LAYER_TABLE = "olist_customers"

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


def bronze_data_ingestion(batch_id, ingestion_time):
    
    PATH = f"gs://{BUCKET}/{LAYER}/{DATASOURCE}/{BRONZE_LAYER_TABLE}/{batch_id}"       

    client = bigquery.Client()
 

    table_ref = client.dataset("metadata").table("batches")
    metadata_table = client.get_table(table_ref)
    try:

        
        df = df.withColumn("batch_id", F.lit(batch_id))
        df.write.format("parquet").save(PATH)
        log.info(f"ingestion job sucess!")
        
        data = [{'bucket': BUCKET, 'layer': LAYER, 'data_source': DATASOURCE, 'table': BRONZE_LAYER_TABLE, 'batch_id': batch_id, 'ingestion_time': ingestion_time}]
        errors = client.insert_rows(metadata_table, data)
        if errors == []:
            print("job details inserted in metadata.batches!")
        else:
            print("error: ", errors)
        spark.stop()
    except Exception as e:
        log.error("Error: ",e)
     
    # Consulta SQL para verificar se todos os registros no batch estão ingeridos
    # def check_ingested_data():
        
    #     query_ingerido = f"""
    #     SELECT COUNT(batch_id) AS total
    #     FROM {PROJECT_ID}.{LAYER}.{BRONZE_LAYER_TABLE}
    #     WHERE batch_id = '{batch_id}'
    #     """

    #     query_new_data = f"""
    #     SELECT COUNT(batch_id) as batch_id_total
    #     FROM {PROJECT_ID}.metadata.batches
    #     WHERE batch_id = '{batch_id}'
    #     """

    #     result_ingerido = list(client.query(query_ingerido).result())
    #     result_new_data = list(client.query(query_new_data).result())

    #     #verifica se a tabela esta vazia
    #     if not result_ingerido:
    #         return True
        
    #     #verifica se os dados ja foram ingeridos
    #     for row in result_ingerido:
    #         if row.total != 0: 
    #             return False
        
    #     #verifica se os dados sao novos
    #     for row in result_new_data:
    #        if row.batch_id_total == 0: 
    #             return True
           
    #     return False

    # def check_ingestion_test():

    #     consulta_sql = f"""
    #     SELECT batch_id
    #     FROM {PROJECT_ID}.{LAYER}.{BRONZE_LAYER_TABLE}
    #     """
    #     df_destino = client.query(consulta_sql).to_dataframe()

    #     df_origem = spark.read.parquet(PATH)

    #     df_origem_filtrado = df_origem.select("batch_id").distinct()
    #     df_destino_filtrado = df_destino.select("batch_id").distinct()

    #     df_duplicatas = df_origem_filtrado.join(df_destino_filtrado, on="batch_id", how="inner")

    #     if df_duplicatas.count() == 0:
    #         return True
    #     return False
        
        
    # # Verifique se o batch está ingerido antes de ingerir novos dados                           
    # if check_ingestion_test():
    #     print("Ingesting new data...")
    #     #--customer tem 99441 rows
    #     data_ingestion(BUCKET, LAYER, data_source_properties["name"], DATA_SOURCE_TABLE)

    #     #set_ingested_status(batch_id)
    #     print("data load complete!!")
            
    # else:
    #     print("nothing to ingest...")  









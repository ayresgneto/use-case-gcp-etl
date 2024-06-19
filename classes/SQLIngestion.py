import datetime
import sys
sys.path.append('/home/ayres/airflow/configs')
from DataIngestion import DataIngestion
from databases import databases_list
import logging as log
from google.cloud import bigquery
import pyspark.sql.functions as F


class SQLIngestion(DataIngestion):
    def __init__(self, spark_session):
        super().__init__(spark_session)

    def bronze_ingestion(self, bucket: str, datasource: str, table: str, batch_id: str, ingestion_time: datetime.datetime):
        """
        Realiza ingestão de dados de uma tabela para o storage (bronze_layer) e
        adiciona os dados de ingestão na tabela de metadados

        Args:
            self: herda da classe DataIngestion
            bucket (str): nome no bucket da ingestão (padrão: etl-use-case-gcp)
            datasource (str): nome do banco de dados
            table (str): nome da tabela
            batch_id (str): valor do batch_id
            ingestion_time (timestamp): data da ingestão

        Returns:
            df (spark data frame): dataframe com dados da tabela
        """

        if not isinstance(ingestion_time, datetime.datetime):
            raise TypeError("ingestion_time must be timestamp type: %Y-%m-%d-%H-%M-%S")
        
        path_ingestion = f"gs://{bucket}/bronze_layer/{datasource}/{table}/{batch_id}" 
        database_properties = next((db for db in databases_list if db["name"] == datasource), None)

        if database_properties:

            #instanciando tabela de metadados
            client = bigquery.Client()
            table_ref = client.dataset("metadata").table("batches")
            metadata_table = client.get_table(table_ref)
            
            try:
                url = database_properties["url"]
                user = database_properties["user"]
                password = database_properties["password"]
                driver = database_properties["driver"]


                df = self.load_data("jdbc") \
                    .option("url", url) \
                    .option("dbtable", table) \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", driver) \
                    .load()
                df = df.withColumn("batch_id", F.lit(batch_id))
                df.write.format("parquet").save(path_ingestion)
                log.info(f"ingestion job sucess on: {path_ingestion}!")

                #adicionando detalhes do job de ingestão para tabela de metadados
                data = [{'bucket': bucket, 'layer': 'bronze_layer', 'data_source': datasource, 'table': table, 'batch_id': batch_id, 'ingestion_time': ingestion_time}]
                errors = client.insert_rows(metadata_table, data)
                if errors == []:
                    print("job details inserted in metadata.batches!")
                else:
                    print("error: ", errors)
                return df
            except Exception as e:
                log.error(f"Error: connection failure, database '{datasource}.", e)      
        else:
            log.error(f"database '{datasource}' not found in databases.")
    


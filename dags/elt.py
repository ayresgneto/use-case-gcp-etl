import logging
import os
import sys
sys.path.append('/home/ayres/airflow/classes')
from SQLIngestion import SQLIngestion
from pyspark.sql import SparkSession
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import uuid

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ayres/.config/gcloud/application_default_credentials.json"

def create_cluster():
    spark = SparkSession \
        .builder \
        .appName("DataIngestionJob") \
        .config("spark.jars", 
            "/home/ayres/Documents/projects/use-case-gcp-etl/configs/postgresql-42.7.3.jar, \
            https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.instances", "4") \
        .getOrCreate()

    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    return spark

default_args = {
    'owner': 'ayres',
    'description': 'GCP ELT',
    'start_date': dt.datetime(2024,6,20),
    'schedule_interval': None,
    #'max_active_runs': 1,
    #'email_on_failure': False,
    #'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),

    }

dag = DAG('Data_Replication_Workflow',
        default_args=default_args,
        catchup=False)

def logger():
    logger =  logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.FileHandler(f'./logs/dag.log')
    logger_formatter = logging.Formatter('%(asctime)s -%(name)s %(levelname)s - %(message)s \n')
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)
    logger.info('Logs is instatiated')

def get_batch_id():

    batch_id = dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-" + str(uuid.uuid4())

    #tratamento para extrair o ingestion time (TIMESTAMP)
    ingestion_time = '-'.join(batch_id.split('-')[:6])
    ingestion_time = dt.datetime.strptime(ingestion_time, "%Y-%m-%d-%H-%M-%S")
    ingestion_time = ingestion_time.isoformat()

    return batch_id, ingestion_time
        
batch_id, ingestion_time = get_batch_id()
spark = create_cluster()
sql_ingestion = SQLIngestion(spark)

log = PythonOperator(task_id='dag_log',
                    python_callable=logger,
                    dag=dag)

creating_cluster = PythonOperator(task_id='create_cluster',
                    python_callable=create_cluster,
                    dag=dag)

generate_batch_id = PythonOperator(task_id='get_batch_id',
                    python_callable=get_batch_id,
                    dag=dag)

olist_customer_ingestion_params = {'bucket': 'etl-use-case-gcp', 'datasource': 'olist', 'table': 'public.customers', 
                                   'batch_id': batch_id, 'ingestion_time': ingestion_time}
ingest_olist_customers = PythonOperator(task_id='ingest_olist_customers',
                            python_callable=sql_ingestion.bronze_ingestion,
                            op_kwargs=olist_customer_ingestion_params,
                            #execution_timeout=timedelta(minutes=1),
                            provide_context=True,
                            dag=dag)

log >> creating_cluster >> generate_batch_id >> ingest_olist_customers

       
        

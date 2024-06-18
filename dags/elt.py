import logging
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/jobs')
from jobs.customers_bronze_layer_full import bronze_data_ingestion
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import uuid

default_args = {
    'owner': 'ayres',
    'description': 'GCP ELT',
    'start_date': days_ago(1),
    'depends_on_past': False
    #'retries': 1
    #'retry_delay': timedelta(minutes=2)
    }

def logger():
    logger =  logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.FileHandler(f'./logs/dag.log')
    logger_formatter = logging.Formatter('%(asctime)s -%(name)s %(levelname)s - %(message)s \n')
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)
    logger.info('Logs is instatiated')

def get_batch_id():

    batch_id = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-" + str(uuid.uuid4())

    #tratamento para extrair o ingestion time (TIMESTAMP)
    ingestion_time = '-'.join(batch_id.split('-')[:6])
    ingestion_time = datetime.datetime.strptime(ingestion_time, "%Y-%m-%d-%H-%M-%S")
    ingestion_time = ingestion_time.isoformat()

    return batch_id, ingestion_time

dag = DAG('Data_Replication_Workflow',
        default_args=default_args,
        catchup=False,
        schedule_interval=None)
        
batch_id, ingestion_time = get_batch_id()

log = PythonOperator(task_id='dag_log',
                    python_callable=logger,
                    dag=dag)

generate_batch_id = PythonOperator(task_id='get_batch_id',
                    python_callable=get_batch_id,
                    dag=dag)

ingest_olist_customers = PythonOperator(task_id='ingest_olist_customers',
                            python_callable=bronze_data_ingestion,
                            op_kwargs={'batch_id': batch_id, 'ingestion_time': ingestion_time},
                            provide_context=True,
                            dag=dag)

       
        

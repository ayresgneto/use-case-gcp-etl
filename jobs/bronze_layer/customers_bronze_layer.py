import datetime
import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/classes')
import DataIngestion
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import uuid

spark = SparkSession \
    .builder \
    .appName("DataIngestionJob") \
    .config("spark.jars", "/home/ayres/Documents/projects/use-case-gcp-etl/configs/postgresql-42.7.3.jar") \
    .getOrCreate()

data_ingestor = DataIngestion.DataIngestor("sql", "olist")

LAYER = "bronze_layer"
TABLE = "public.customers"
#PATH = f"gs://{LAYER}/{data_ingestor["dbname"]}/{TABLE}"

#def job_ingestion_name():
job= datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-" + str(uuid.uuid4())
db_properties = data_ingestor.read_data()

# df = spark.read \
#     .format("jdbc") \
#     .option("url", f"{db_properties["url"]}") \
#     .option("dbtable", f"{TABLE}") \
#     .option("user", f"{db_properties["user"]}") \
#     .option("password", f"{db_properties["password"]}") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

print(df.head)

#spark.stop()
from venv import logger
from pyspark.sql import SparkSession
from configs import databases

spark = SparkSession \
    .builder \
    .appName("elt") \
    .config("spark.jars", "/configs/postgresql-42.7.3.jar") \
    .getOrCreate()

def ingestion(dbname, table):

    for db_properties in databases.databases_list:
        if db_properties["name"] == dbname:
            try:
                df = spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:{db_properties["kind"]}://{db_properties["url"]}:{db_properties["port"]}/{db_properties["name"]}") \
                .option("dbtable", f"{table}") \
                .option("user", f"{db_properties["user"]}") \
                .option("password", f"{db_properties["password"]}") \
                .option("driver", f"org.{db_properties["kind"]}.Driver") \
                .load()
            except: 
                logger.error(f'Error extracting from {db_properties["name"]}.{table}', exc_info=True)
        else:
            logger.error("database not found")       
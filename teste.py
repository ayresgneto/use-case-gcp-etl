import sys
sys.path.append('/home/ayres/Documents/projects/use-case-gcp-etl/classes')
from classes.SQLIngestion import SQLIngestion
from pyspark.sql import SparkSession

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

customers = SQLIngestion(spark)
df_db = customers.extract_data("olist","customers")
df_db.show(10)
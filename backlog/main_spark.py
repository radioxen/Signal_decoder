from constants import *
import findspark
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

findspark.init()

load_dotenv("../.env")

spark_cluster_uri = os.environ["SPARK_URI"]
csv_path = os.environ["CSV_PATH"]
# os.environ['PYARROW_IGNORE_TIMEZONE'] = "1"

spark = (
    SparkSession.Builder().master(spark_cluster_uri).appName("Beta_test").getOrCreate()
)

df = spark.read.option("header", True).csv(default_input_file_path)
df.dropna(how="all")
df.fillna(df["ID (hex)"].isin(target_signals))
df.printSchema()

# common/spark.py
from pyspark.sql import SparkSession
import os

POSTGRES_JAR = os.path.abspath("jars/postgresql-42.7.8.jar")

def get_spark(app_name="DataWarehouseSpark"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", POSTGRES_JAR)
        .config("spark.driver.extraClassPath", POSTGRES_JAR)
        .config("spark.executor.extraClassPath", POSTGRES_JAR)
        .getOrCreate()
    )


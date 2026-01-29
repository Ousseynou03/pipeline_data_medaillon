from pyspark.sql import SparkSession

def get_spark(app_name="DataWarehouseSpark"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

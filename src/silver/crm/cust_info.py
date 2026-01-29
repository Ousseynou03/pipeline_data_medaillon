# src/silver/cust_info.py
from pyspark.sql.functions import (
    row_number, col, trim, when, to_date, current_timestamp
)
from pyspark.sql.window import Window
import os

bronze_path = "data/bronze/cust_info.parquet"
silver_path = "data/silver/cust_info.csv"
os.makedirs("data/silver", exist_ok=True)

def transform_cust_info(spark, write_output=True):
    print("\n[INFO] Démarrage de la transformation Silver pour cust_info...")

    print(f"[INFO] Lecture du fichier Bronze : {bronze_path}")
    df = spark.read.parquet(bronze_path) \
        .withColumn("cst_id", col("cst_id").cast("string"))

    print(f"[INFO] Nombre de lignes Bronze : {df.count()}")

    df = df.withColumn("cst_id", trim(col("cst_id"))) \
           .filter((col("cst_id").isNotNull()) & (col("cst_id") != ""))

    print(f"[INFO] Après filtrage cst_id : {df.count()}")

    window_spec = Window.partitionBy("cst_id").orderBy(col("_ingestion_date").desc())
    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    print(f"[INFO] Après déduplication : {df.count()}")

    df = (
        df.withColumn("cst_firstname", trim(col("cst_firstname")))
          .withColumn("cst_lastname", trim(col("cst_lastname")))
          .withColumn(
              "cst_marital_status",
              when(col("cst_marital_status") == "M", "Married")
              .when(col("cst_marital_status") == "S", "Single")
              .otherwise(col("cst_marital_status"))
          )
          .withColumn(
              "cst_gndr",
              when(col("cst_gndr") == "M", "Male")
              .when(col("cst_gndr") == "F", "Female")
              .otherwise(col("cst_gndr"))
          )
          .withColumn("cst_create_date", to_date(col("cst_create_date"), "yyyyMMdd"))
          .withColumn("_processed_timestamp", current_timestamp())
    )

    if write_output:
        df.write.mode("overwrite").csv(silver_path, header=True)
        print(f"[INFO] Silver écrit dans {silver_path}")

    print("[INFO] Transformation Silver terminée\n")
    return df

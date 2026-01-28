# silver/cust_info.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, trim, when, to_date, current_timestamp
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.appName("SilverCustInfo").getOrCreate()

bronze_path = "data/bronze/cust_info.parquet"
silver_path = "data/silver/cust_info"
os.makedirs("data/silver", exist_ok=True)

def transform_cust_info(write_output=True):
    print("\n[INFO] Démarrage de la transformation Silver pour cust_info...")

    # Lire le bronze et forcer cst_id en string
    print(f"[INFO] Lecture du fichier Bronze depuis : {bronze_path}")
    df = spark.read.parquet(bronze_path).withColumn("cst_id", col("cst_id").cast("string"))
    print(f"[INFO] {df.count()} enregistrements lus depuis Bronze")
    df.show(5, truncate=False)

    # Filtrer les cst_id non null et non vides (après trim)
    df = df.withColumn("cst_id", trim(col("cst_id"))) \
           .filter((col("cst_id").isNotNull()) & (col("cst_id") != ""))
    print(f"[INFO] Après filtrage des cst_id nuls ou vides : {df.count()} enregistrements restants")
    df.show(5, truncate=False)

    # Déduplication : garder le plus récent
    window_spec = Window.partitionBy("cst_id").orderBy(col("_ingestion_date").desc())
    df = df.withColumn("rn", row_number().over(window_spec))
    df = df.filter(col("rn") == 1).drop("rn")
    print(f"[INFO] Après déduplication : {df.count()} enregistrements uniques")
    df.show(5, truncate=False)

    # Standardisation
    print("[INFO] Application des transformations : TRIM, standardisation marital_status et gndr, conversion date")
    df = df.withColumn("cst_firstname", trim(col("cst_firstname"))) \
           .withColumn("cst_lastname", trim(col("cst_lastname"))) \
           .withColumn("cst_marital_status", when(col("cst_marital_status") == "M", "Married")
                                               .when(col("cst_marital_status") == "S", "Single")
                                               .otherwise(col("cst_marital_status"))) \
           .withColumn("cst_gndr", when(col("cst_gndr") == "M", "Male")
                                    .when(col("cst_gndr") == "F", "Female")
                                    .otherwise(col("cst_gndr"))) \
           .withColumn("cst_create_date", to_date(col("cst_create_date"), "yyyyMMdd")) \
           .withColumn("_processed_timestamp", current_timestamp())

    if write_output:
        df.write.mode("overwrite").csv(silver_path)
        print(f"[INFO] Silver cust_info écrit dans : {silver_path}")
        df.show(5, truncate=False)

    print("[INFO] Transformation Silver terminée.\n")
    return df

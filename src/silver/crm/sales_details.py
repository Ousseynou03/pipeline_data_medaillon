# silver/sales_details.py
from pyspark.sql.functions import (
    col,
    when,
    length,
    lit,
    to_date,
    abs,
    current_timestamp
)
import os

bronze_path = "data/bronze/sales_details.parquet"
silver_path = "data/silver/sales_details"

os.makedirs("data/silver", exist_ok=True)

def transform_sales_details(spark, write_output=True):
    print("\n[INFO] ===== Démarrage de la transformation Silver : sales_details =====")

    # Lecture Bronze
    print(f"[INFO] Lecture du fichier Bronze : {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[INFO] {df.count()} lignes lues depuis Bronze")
    df.show(5, truncate=False)

    # Nettoyage & conversion des dates
    print("[INFO] Nettoyage et conversion des dates (order / ship / due)")

    df = df.withColumn(
        "sls_order_dt",
        when((col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8), None)
        .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))
    ).withColumn(
        "sls_ship_dt",
        when((col("sls_ship_dt") == 0) | (length(col("sls_ship_dt")) != 8), None)
        .otherwise(to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))
    ).withColumn(
        "sls_due_dt",
        when((col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8), None)
        .otherwise(to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))
    )

    # Recalcul du montant des ventes (sls_sales)
    print("[INFO] Recalcul sls_sales si NULL, <= 0 ou incohérent")

    df = df.withColumn(
        "sls_sales",
        when(
            (col("sls_sales").isNull()) |
            (col("sls_sales") <= 0) |
            (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
            col("sls_quantity") * abs(col("sls_price"))
        ).otherwise(col("sls_sales"))
    )

    # Recalcul du prix unitaire si invalide
    print("[INFO] Recalcul sls_price si NULL ou <= 0")

    df = df.withColumn(
        "sls_price",
        when(
            (col("sls_price").isNull()) | (col("sls_price") <= 0),
            col("sls_sales") / when(col("sls_quantity") != 0, col("sls_quantity"))
        ).otherwise(col("sls_price"))
    )

    # Ajout métadonnée Silver
    df = df.withColumn("_processed_timestamp", current_timestamp())

    print("[INFO] Aperçu final Silver sales_details")
    df.select(
        "sls_ord_num",
        "sls_prd_key",
        "sls_cust_id",
        "sls_order_dt",
        "sls_ship_dt",
        "sls_due_dt",
        "sls_sales",
        "sls_quantity",
        "sls_price"
    ).show(5, truncate=False)

    # Écriture Silver
    if write_output:
        df.write.mode("overwrite").csv(silver_path, header=True)
        print(f"[INFO] Silver sales_details écrit dans : {silver_path}")

    print("[INFO] ===== Transformation Silver sales_details terminée =====\n")
    return df

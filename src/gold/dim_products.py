# gold/dim_products.py
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from src.common.config import PG_URL, PG_PROPERTIES

def load_dim_products(spark, write_output=True):
    print("\n[INFO] Chargement GOLD.DIM_PRODUCTS")

    # Lecture Silver
    pn = spark.read.option("header", True).csv("data/silver/prd_info.csv")
    pc = spark.read.option("header", True).csv("data/silver/px_cat_g1v2.csv")

    # Renommer les colonnes pour éviter les problèmes de casse
    pc = pc.withColumnRenamed("ID", "id") \
           .withColumnRenamed("CAT", "cat") \
           .withColumnRenamed("SUBCAT", "subcat") \
           .withColumnRenamed("MAINTENANCE", "maintenance")

    # Join Silver pn avec pc
    df = pn.filter(col("prd_end_dt").isNull()) \
           .join(pc, pn.cat_id == pc.id, "left")

    # Surrogate key
    window_spec = Window.orderBy("prd_start_dt", "prd_key")
    df = df.withColumn("product_key", row_number().over(window_spec))

    # Sélection finale
    df = df.select(
        "product_key",
        col("prd_id").alias("product_id"),
        col("prd_key").alias("product_number"),
        col("prd_nm").alias("product_name"),
        "cat_id",
        col("cat").alias("category"),
        col("subcat").alias("subcategory"),
        "maintenance",
        col("prd_cost").alias("cost"),
        col("prd_line").alias("product_line"),
        col("prd_start_dt").alias("start_date")
    )

    # Écriture PostgreSQL
    if write_output:
        df.write.jdbc(
            url=PG_URL,
            table="gold.dim_products",
            mode="overwrite",
            properties=PG_PROPERTIES
        )
        print("[INFO] gold.dim_products écrite dans PostgreSQL")

    return df

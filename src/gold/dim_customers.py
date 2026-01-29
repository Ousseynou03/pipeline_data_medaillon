# gold/dim_customers.py

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from src.common.config import PG_URL, PG_PROPERTIES

def load_dim_customers(spark, write_output=True):
    print("\n[INFO] Chargement GOLD.DIM_CUSTOMERS")

    # Lire Silver
    ci = spark.read.option("header", True).csv("data/silver/cust_info.csv")
    ca = spark.read.option("header", True).csv("data/silver/cust_az12")
    la = spark.read.option("header", True).csv("data/silver/loc_a101.csv")

    # Joins
    df = (
        ci.alias("ci")
        .join(ca.alias("ca"), col("ci.cst_key") == col("ca.cid"), "left")
        .join(la.alias("la"), col("ci.cst_key") == col("la.cid"), "left")
    )

    # Gender rule
    df = df.withColumn(
        "gender",
        when(col("ci.cst_gndr") != "n/a", col("ci.cst_gndr"))
        .otherwise(coalesce(col("ca.gen"), lit("n/a")))
    )

    # Surrogate key
    window_spec = Window.orderBy("ci.cst_id")
    df = df.withColumn("customer_key", row_number().over(window_spec))

    # Sélection finale
    df = df.select(
        "customer_key",
        col("ci.cst_id").alias("customer_id"),
        col("ci.cst_key").alias("customer_number"),
        col("ci.cst_firstname").alias("first_name"),
        col("ci.cst_lastname").alias("last_name"),
        col("la.cntry").alias("country"),
        col("ci.cst_marital_status").alias("marital_status"),
        "gender",
        col("ca.bdate").alias("birthdate"),
        col("ci.cst_create_date").alias("create_date")
    )

    if write_output:
        df.write.jdbc(
            url=PG_URL,
            table="gold.dim_customers",
            mode="overwrite",
            properties=PG_PROPERTIES
        )
        print("[INFO] gold.dim_customers écrite dans PostgreSQL")

    return df

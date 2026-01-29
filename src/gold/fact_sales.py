# gold/fact_sales.py

from pyspark.sql.functions import *
from src.common.config import PG_URL, PG_PROPERTIES

def load_fact_sales(spark, write_output=True):
    print("\n[INFO] Chargement GOLD.FACT_SALES")

    sd = spark.read.option("header", True).csv("data/silver/sales_details")
    pr = spark.read.jdbc(PG_URL, "gold.dim_products", properties=PG_PROPERTIES)
    cu = spark.read.jdbc(PG_URL, "gold.dim_customers", properties=PG_PROPERTIES)

    df = (
        sd.join(pr, sd.sls_prd_key == pr.product_number, "left")
          .join(cu, sd.sls_cust_id == cu.customer_id, "left")
    )

    df = df.select(
        col("sls_ord_num").alias("order_number"),
        "product_key",
        "customer_key",
        col("sls_order_dt").alias("order_date"),
        col("sls_ship_dt").alias("shipping_date"),
        col("sls_due_dt").alias("due_date"),
        col("sls_sales").alias("sales_amount"),
        col("sls_quantity").alias("quantity"),
        col("sls_price").alias("price")
    )

    if write_output:
        df.write.jdbc(
            url=PG_URL,
            table="gold.fact_sales",
            mode="overwrite",
            properties=PG_PROPERTIES
        )
        print("[INFO] gold.fact_sales écrite dans PostgreSQL")

    return df

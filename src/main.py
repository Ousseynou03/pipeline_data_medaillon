# src/main.py
from common.spark import get_spark
from src.silver.crm.cust_info import transform_cust_info
from src.silver.crm.prd_info import transform_prd_info
from src.silver.crm.sales_details import transform_sales_details
from src.silver.erp.cust_az12 import transform_cust_az12
from src.silver.erp.loc_a101 import transform_loc_a101
from src.silver.erp.px_cat_g1v2 import transform_px_cat_g1v2
from src.bronze.bronze_ingest import bronze_ingest

from src.gold.dim_customers import load_dim_customers
from src.gold.dim_products import load_dim_products
from src.gold.fact_sales import load_fact_sales

def main():
    spark = get_spark()

    print("\n===== PIPELINE START =====")

    #--------- BRONZE -------------#
    bronze_ingest(spark)

    #--------- SILVER -----------#
    transform_cust_info(spark)
    transform_prd_info(spark)
    transform_sales_details(spark)
    transform_cust_az12(spark)
    transform_loc_a101(spark)
    transform_px_cat_g1v2(spark)

    # --------- GOLD ------------ #
    load_dim_customers(spark)
    load_dim_products(spark)
    load_fact_sales(spark)

    print("===== PIPELINE END =====\n")

    spark.stop()

if __name__ == "__main__":
    main()

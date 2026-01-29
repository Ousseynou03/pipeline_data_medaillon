# src/main.py
from common.spark import get_spark
from src.silver.crm.cust_info import transform_cust_info
from src.silver.crm.prd_info import transform_prd_info
from src.silver.crm.sales_details import transform_sales_details
from src.silver.erp.cust_az12 import transform_cust_az12
from src.silver.erp.loc_a101 import transform_loc_a101
from src.silver.erp.px_cat_g1v2 import transform_px_cat_g1v2

def main():
    spark = get_spark()

    print("\n===== PIPELINE START =====")

    #bronze_ingest(spark)
    transform_cust_info(spark)
    transform_prd_info(spark)
    transform_sales_details(spark)
    transform_cust_az12(spark)
    transform_loc_a101(spark)
    transform_px_cat_g1v2(spark)

    print("===== PIPELINE END =====\n")

    spark.stop()

if __name__ == "__main__":
    main()

# src/main.py
from common.spark import get_spark
from bronze.bronze_ingest import bronze_ingest
from silver.cust_info import transform_cust_info
from silver.prd_info import transform_prd_info
from silver.sales_details import transform_sales_details

def main():
    spark = get_spark()

    print("\n===== PIPELINE START =====")

    #bronze_ingest(spark)
    transform_cust_info(spark)
    transform_prd_info(spark)
    transform_sales_details(spark)

    print("===== PIPELINE END =====\n")

    spark.stop()

if __name__ == "__main__":
    main()

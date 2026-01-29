import os
from pyspark.sql.functions import current_timestamp

bronze_path = "data/bronze"
os.makedirs(bronze_path, exist_ok=True)

source_file = [
    "dataset/source_crm/cust_info.csv",
    "dataset/source_crm/prd_info.csv",
    "dataset/source_crm/sales_details.csv",
    "dataset/source_erp/CUST_AZ12.csv",
    "dataset/source_erp/LOC_A101.csv",
    "dataset/source_erp/PX_CAT_G1V2.csv",
]

def bronze_ingest(spark):
    for datas in source_file:
        df = spark.read.csv(datas, inferSchema=True, header=True)
        df = df.withColumn("_ingestion_date", current_timestamp())

        output_file = os.path.join(bronze_path, datas.split("/")[-1].replace(".csv", ".parquet"))
        df.write.mode("overwrite").parquet(output_file)

        print(f"Ingested {datas} → {output_file}")

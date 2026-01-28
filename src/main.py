from bronze.bronze_ingest import bronze_ingest
from silver.cust_info import transform_cust_info


bronze_ingest()
transform_cust_info()

if __name__ == '__main__':
    bronze_ingest()
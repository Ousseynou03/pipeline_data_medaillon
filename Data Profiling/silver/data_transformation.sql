-- Créer la table Silver avec déduplication et standardisation
CREATE TABLE silver.cust_info AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY _ingestion_date DESC
           ) AS rn
    FROM bronze.cust_info
    WHERE cst_id IS NOT NULL AND cst_id <> ''
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN cst_marital_status = 'M' THEN 'Married'
        WHEN cst_marital_status = 'S' THEN 'Single'
        ELSE cst_marital_status
    END AS cst_marital_status,
    CASE 
        WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'
        ELSE cst_gndr
    END AS cst_gndr,
    TO_DATE(cst_create_date, 'YYYY-MM-DD') AS cst_create_date,
    _ingestion_date
FROM ranked
WHERE rn = 1;


-- Vérifie que tout est okay 
SELECT * FROM silver.cust_info LIMIT 10;
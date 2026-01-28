-- pour la table cust_info
-- Vérifions s'il y'a des doublons
SELECT *, 
COUNT(*) FROM bronze.cust_info
GROUP BY cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date,_ingestion_date,_source_file 
HAVING COUNT(*) > 1;

-- Vérifions s'il y'a des doublons sur la clé primaire cst_id
SELECT cst_id, COUNT(*) FROM bronze.cust_info GROUP BY cst_id HAVING COUNT(*) > 1; -- 6 doublons sur cst_id



-- Vérifions s'il n'y a pas des ids nuls
SELECT * FROM bronze.cust_info WHERE cst_id IS NULL;  -- 4 lignes avec cst_id null

-- pour la table prd_info
SELECT *, COUNT(*) FROM bronze.prd_info GROUP BY prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt,_ingestion_date,_source_file  HAVING COUNT(*) > 1;

-- Vérifions s'il y'a des doublons sur la clé primaire prd_id
SELECT prd_id, COUNT(*) FROM bronze.prd_info GROUP BY prd_id HAVING COUNT(*) > 1; -- pas de doublons sur prd_id


-- Vérifions sil ny a pas des ids nuls
SELECT * FROM bronze.prd_info WHERE prd_id IS NULL;  -- pas de lignes

# silver/cust_az12.py
from pyspark.sql.functions import col, when, upper, trim, current_date, current_timestamp
import os

bronze_path = "data/bronze/cust_az12.parquet"
silver_path = "data/silver/cust_az12"

os.makedirs("data/silver", exist_ok=True)

def transform_cust_az12(spark, write_output=True):
    print("\n[INFO] ===== Démarrage de la transformation Silver : cust_az12 =====")

    # Lecture Bronze
    print(f"[INFO] Lecture du fichier Bronze : {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[INFO] {df.count()} lignes lues depuis Bronze")

    # Normalisation du cid (supprimer le préfixe NAS)
    print("[INFO] Normalisation des IDs client (cid)")
    df = df.withColumn(
        "cid",
        when(col("cid").startswith("NAS"), col("cid").substr(4, 1000))  # substr(startPos, length)
        .otherwise(col("cid"))
    )

    # Gestion des dates de naissance futures
    print("[INFO] Remplacer les dates de naissance futures par NULL")
    df = df.withColumn(
        "bdate",
        when(col("bdate") > current_date(), None)
        .otherwise(col("bdate"))
    )

    # Normalisation du genre
    print("[INFO] Normalisation des valeurs de genre")
    df = df.withColumn(
        "gen",
        when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
        .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
        .otherwise("n/a")
    )

    # Ajout métadonnée Silver
    df = df.withColumn("_processed_timestamp", current_timestamp())

    print("[INFO] Aperçu final Silver cust_az12")
    df.show(5, truncate=False)

    # Écriture Silver
    if write_output:
        df.write.mode("overwrite").csv(silver_path, header=True)
        print(f"[INFO] Silver cust_az12 écrit dans : {silver_path}")

    print("[INFO] ===== Transformation Silver cust_az12 terminée =====\n")
    return df

# silver/loc_a101.py
from pyspark.sql.functions import col, trim, when, regexp_replace, current_timestamp
import os

bronze_path = "data/bronze/LOC_A101.parquet"
silver_path = "data/silver/loc_a101.csv"

os.makedirs("data/silver", exist_ok=True)

def transform_loc_a101(spark, write_output=True):
    print("\n[INFO] ===== Démarrage de la transformation Silver : loc_a101 =====")

    # Lecture Bronze
    print(f"[INFO] Lecture du fichier Bronze : {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[INFO] {df.count()} lignes lues depuis Bronze")

    # Normalisation du cid (supprimer les tirets)
    print("[INFO] Normalisation des IDs client (cid)")
    df = df.withColumn("cid", regexp_replace(col("cid"), "-", ""))

    # Normalisation du pays
    print("[INFO] Normalisation des codes pays")
    df = df.withColumn(
        "cntry",
        when(trim(col("cntry")) == "DE", "Germany")
        .when(trim(col("cntry")).isin("US", "USA"), "United States")
        .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
        .otherwise(trim(col("cntry")))
    )

    # Ajout métadonnée Silver
    df = df.withColumn("_processed_timestamp", current_timestamp())

    print("[INFO] Aperçu final Silver loc_a101")
    df.show(5, truncate=False)

    # Écriture Silver
    if write_output:
        df.write.mode("overwrite").csv(silver_path, header=True)
        print(f"[INFO] Silver loc_a101 écrit dans : {silver_path}")

    print("[INFO] ===== Transformation Silver loc_a101 terminée =====\n")
    return df

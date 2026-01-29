# silver/px_cat_g1v2.py
from pyspark.sql.functions import col, current_timestamp
import os

bronze_path = "data/bronze/PX_CAT_G1V2.parquet"
silver_path = "data/silver/px_cat_g1v2.csv"

os.makedirs("data/silver", exist_ok=True)

def transform_px_cat_g1v2(spark, write_output=True):
    print("\n[INFO] ===== Démarrage de la transformation Silver : px_cat_g1v2 =====")

    # Lecture Bronze
    print(f"[INFO] Lecture du fichier Bronze : {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[INFO] {df.count()} lignes lues depuis Bronze")

    # Ajout métadonnée Silver
    df = df.withColumn("_processed_timestamp", current_timestamp())

    print("[INFO] Aperçu final Silver px_cat_g1v2")
    df.show(5, truncate=False)

    # Écriture Silver
    if write_output:
        df.write.mode("overwrite").csv(silver_path, header=True)
        print(f"[INFO] Silver px_cat_g1v2 écrit dans : {silver_path}")

    print("[INFO] ===== Transformation Silver px_cat_g1v2 terminée =====\n")
    return df

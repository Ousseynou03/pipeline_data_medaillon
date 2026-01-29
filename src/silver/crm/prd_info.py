from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    substring,
    regexp_replace,
    length,
    coalesce,
    lit,
    lead,
    to_date,
    date_sub,
    current_timestamp
)
from pyspark.sql.window import Window
import os

bronze_path = "data/bronze/prd_info.parquet"
silver_path = "data/silver/prd_info.csv"

os.makedirs("data/silver", exist_ok=True)

def transform_prd_info(spark, write_output=True):
    print("\n[INFO] ===== Démarrage de la transformation Silver : prd_info =====")

    # Lecture Bronze
    print(f"[INFO] Lecture du fichier Bronze : {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[INFO] {df.count()} lignes lues depuis Bronze")

    # Extraction des clés (cat_id / prd_key)
    print("[INFO] Extraction cat_id et nettoyage prd_key")
    df = df.withColumn(
            "cat_id",
            regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
        ) \
        .withColumn(
            "prd_key",
            substring(col("prd_key"), 7, length(col("prd_key")))
        )

    # Gestion des valeurs nulles
    print("[INFO] Remplacement des coûts NULL par 0")
    df = df.withColumn("prd_cost", coalesce(col("prd_cost"), lit(0)))

    # Standardisation prd_line
    print("[INFO] Standardisation des codes prd_line")
    df = df.withColumn(
        "prd_line",
        when(upper(trim(col("prd_line"))) == "M", "Mountain")
        .when(upper(trim(col("prd_line"))) == "R", "Road")
        .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
        .when(upper(trim(col("prd_line"))) == "T", "Touring")
        .otherwise("n/a")
    )

    # Gestion de l’historique (SCD)
    print("[INFO] Calcul des dates de validité (prd_start_dt / prd_end_dt)")
    df = df.withColumn("prd_start_dt", to_date(col("prd_start_dt")))

    window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")

    df = df.withColumn(
        "prd_end_dt",
        date_sub(lead("prd_start_dt").over(window_spec), 1)
    )

    # Ajout métadonnée Silver
    df = df.withColumn("_processed_timestamp", current_timestamp())

    print("[INFO] Aperçu final Silver prd_info")
    df.show(5, truncate=False)

    # Écriture Silver
    if write_output:
        df.write.mode("overwrite").csv(silver_path, header=True)
        print(f"[INFO] Silver prd_info écrit dans : {silver_path}")

    print("[INFO] ===== Transformation Silver prd_info terminée =====\n")
    return df

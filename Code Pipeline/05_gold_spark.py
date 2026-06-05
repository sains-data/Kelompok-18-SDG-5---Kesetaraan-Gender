from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("UKGPG-Gold-Aggregate") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER = "s3a://bigdata-lab/silver/"
GOLD   = "s3a://bigdata-lab/gold/"

print("\n[GOLD] Membaca Silver layer dari MinIO...")
t_start = time.time()

df_silver = spark.read.parquet(SILVER)

# Ekstrak tahun dari duedate dengan format yang benar
df_silver = df_silver.withColumn(
    "tahun",
    F.year(F.to_timestamp(F.col("duedate"), "yyyy/MM/dd HH:mm:ss"))
)

# Filter hanya baris dengan employersize yang valid
valid_sizes = [
    "Less than 250", "250 to 499", "500 to 999",
    "1000 to 4999", "5000 to 19,999", "20,000 or more", "Not Provided"
]
df_silver = df_silver.filter(F.col("employersize").isin(valid_sizes))

total = df_silver.count()
print(f"  Terbaca. {total:,} baris valid")

# --- Agregasi 1: Tren per tahun ---
print("\n[GOLD] Agregasi tren per tahun...")
df_gold_tahunan = df_silver \
    .groupBy("tahun") \
    .agg(
        F.round(F.mean("diffmeanhourlypercent"), 4).alias("avg_diff_mean_hourly"),
        F.round(F.mean("diffmedianhourlypercent"), 4).alias("avg_diff_median_hourly"),
        F.round(F.stddev("diffmeanhourlypercent"), 4).alias("std_diff_mean_hourly"),
        F.count("employername").alias("jumlah_perusahaan")
    ) \
    .orderBy("tahun")

# --- Agregasi 2: Per ukuran perusahaan ---
print("[GOLD] Agregasi per ukuran perusahaan...")
df_gold_size = df_silver \
    .groupBy("employersize") \
    .agg(
        F.round(F.mean("diffmeanhourlypercent"), 4).alias("avg_diff_mean_hourly"),
        F.count("employername").alias("jumlah_perusahaan")
    ) \
    .orderBy(F.desc("avg_diff_mean_hourly"))

# --- Simpan ke Gold layer ---
print("\n[GOLD] Menyimpan ke MinIO...")
df_gold_tahunan.write.mode("overwrite").parquet(GOLD + "tahunan/")
df_gold_size.write.mode("overwrite").parquet(GOLD + "per_ukuran/")

t_end = time.time()
print(f"\nSelesai dalam {t_end - t_start:.2f} detik")

print("\n=== TREN KESENJANGAN UPAH PER TAHUN ===")
df_gold_tahunan.show(truncate=False)

print("=== KESENJANGAN PER UKURAN PERUSAHAAN ===")
df_gold_size.show(truncate=False)

spark.stop()
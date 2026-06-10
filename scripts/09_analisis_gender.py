from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("UKGPG-Analisis-Gender") \
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

print("\n[ANALISIS GENDER] Membaca Silver layer...")
df = spark.read.parquet(SILVER)

# Ekstrak tahun
df = df.withColumn(
    "tahun",
    F.year(F.to_timestamp(F.col("duedate"), "yyyy/MM/dd HH:mm:ss"))
)

# Filter employersize valid
valid_sizes = [
    "Less than 250", "250 to 499", "500 to 999",
    "1000 to 4999", "5000 to 19,999", "20,000 or more", "Not Provided"
]
df = df.filter(F.col("employersize").isin(valid_sizes))

print(f"Total baris: {df.count():,}")

# ============================================================
# ANALISIS 1: Distribusi Quartile Laki-laki vs Perempuan
# ============================================================
print("\n=== ANALISIS 1: Rata-rata Distribusi Quartile per Jenis Kelamin ===")
df_quartile = df.agg(
    F.round(F.mean("malelowerquartile"), 2).alias("male_lower_q"),
    F.round(F.mean("femalelowerquartile"), 2).alias("female_lower_q"),
    F.round(F.mean("malelowermiddlequartile"), 2).alias("male_lower_mid_q"),
    F.round(F.mean("femalelowermiddlequartile"), 2).alias("female_lower_mid_q"),
    F.round(F.mean("maleuppermiddlequartile"), 2).alias("male_upper_mid_q"),
    F.round(F.mean("femaleuppermiddlequartile"), 2).alias("female_upper_mid_q"),
    F.round(F.mean("maletopquartile"), 2).alias("male_top_q"),
    F.round(F.mean("femaletopquartile"), 2).alias("female_top_q"),
).collect()[0]

print(f"Quartile Bawah    : Laki-laki {df_quartile['male_lower_q']}% | Perempuan {df_quartile['female_lower_q']}%")
print(f"Quartile Mid-Bawah: Laki-laki {df_quartile['male_lower_mid_q']}% | Perempuan {df_quartile['female_lower_mid_q']}%")
print(f"Quartile Mid-Atas : Laki-laki {df_quartile['male_upper_mid_q']}% | Perempuan {df_quartile['female_upper_mid_q']}%")
print(f"Quartile Atas     : Laki-laki {df_quartile['male_top_q']}% | Perempuan {df_quartile['female_top_q']}%")

# ============================================================
# ANALISIS 2: Tren Quartile Atas per Tahun
# ============================================================
print("\n=== ANALISIS 2: Tren Perempuan di Quartile Atas (Top Quartile) per Tahun ===")
df_top = df.groupBy("tahun").agg(
    F.round(F.mean("maletopquartile"), 2).alias("avg_male_top"),
    F.round(F.mean("femaletopquartile"), 2).alias("avg_female_top"),
    F.round(F.mean("diffmeanhourlypercent"), 2).alias("avg_gap"),
).orderBy("tahun")
df_top.show(truncate=False)

# ============================================================
# ANALISIS 3: Bonus Gap
# ============================================================
print("\n=== ANALISIS 3: Kesenjangan Bonus Laki-laki vs Perempuan per Tahun ===")
df_bonus = df.groupBy("tahun").agg(
    F.round(F.mean("diffmeanbonuspercent"), 2).alias("avg_diff_mean_bonus"),
    F.round(F.mean("diffmedianbonuspercent"), 2).alias("avg_diff_median_bonus"),
    F.round(F.mean("malebonuspercent"), 2).alias("pct_laki_terima_bonus"),
    F.round(F.mean("femalebonuspercent"), 2).alias("pct_perempuan_terima_bonus"),
).orderBy("tahun")
df_bonus.show(truncate=False)

# ============================================================
# ANALISIS 4: Ringkasan Statistik Keseluruhan
# ============================================================
print("\n=== ANALISIS 4: Ringkasan Statistik Kesenjangan Gender Keseluruhan ===")
df_summary = df.agg(
    F.round(F.mean("diffmeanhourlypercent"), 4).alias("rata_rata_gap_upah_mean"),
    F.round(F.mean("diffmedianbonuspercent"), 4).alias("rata_rata_gap_bonus_median"),
    F.round(F.mean("malebonuspercent"), 4).alias("pct_laki_bonus"),
    F.round(F.mean("femalebonuspercent"), 4).alias("pct_perempuan_bonus"),
    F.round(F.mean("maletopquartile"), 4).alias("laki_top_quartile"),
    F.round(F.mean("femaletopquartile"), 4).alias("perempuan_top_quartile"),
    F.count("employername").alias("total_perusahaan")
).collect()[0]

print(f"Rata-rata gap upah mean          : {df_summary['rata_rata_gap_upah_mean']}%")
print(f"Rata-rata gap bonus median        : {df_summary['rata_rata_gap_bonus_median']}%")
print(f"% Laki-laki terima bonus          : {df_summary['pct_laki_bonus']}%")
print(f"% Perempuan terima bonus          : {df_summary['pct_perempuan_bonus']}%")
print(f"% Laki-laki di top quartile       : {df_summary['laki_top_quartile']}%")
print(f"% Perempuan di top quartile       : {df_summary['perempuan_top_quartile']}%")
print(f"Total perusahaan dianalisis       : {df_summary['total_perusahaan']:,}")

# ============================================================
# ANALISIS 5: Perusahaan dengan kesenjangan tertinggi & terendah
# ============================================================
print("\n=== ANALISIS 5: 10 Perusahaan dengan Kesenjangan Upah Tertinggi ===")
df.select("employername", "diffmeanhourlypercent", "employersize", "tahun") \
    .filter(F.col("diffmeanhourlypercent") > 0) \
    .orderBy(F.desc("diffmeanhourlypercent")) \
    .limit(10).show(truncate=False)

print("\n=== ANALISIS 6: 10 Perusahaan dengan Kesenjangan Upah Terendah (Paling Setara) ===")
df.select("employername", "diffmeanhourlypercent", "employersize", "tahun") \
    .filter(F.col("diffmeanhourlypercent").isNotNull()) \
    .orderBy("diffmeanhourlypercent") \
    .limit(10).show(truncate=False)

# Simpan ke Gold
df_top.write.mode("overwrite").parquet(GOLD + "analisis_quartile/")
df_bonus.write.mode("overwrite").parquet(GOLD + "analisis_bonus/")

print("\nSemua hasil analisis tersimpan ke Gold layer.")
spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import time

spark = SparkSession.builder \
    .appName("UKGPG-Silver-Transform") \
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
print(f"Spark version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")

BRONZE = "s3a://bigdata-lab/bronze/"
SILVER = "s3a://bigdata-lab/silver/"

# ============================================================
# BRONZE: Baca CSV dari MinIO
# ============================================================
print("\n[BRONZE] Membaca CSV dari MinIO...")
t_start = time.time()

df_bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(BRONZE)

# Standardisasi nama kolom ke lowercase DULU sebelum filter apapun
new_columns = [c.lower().strip() for c in df_bronze.columns]
df_bronze = df_bronze.toDF(*new_columns)

# Hapus baris yang headernya terbaca sebagai data (setelah lowercase)
df_bronze = df_bronze.filter(F.col("employername") != "employername")

total_baris = df_bronze.count()
t_bronze = time.time()
print(f"  Selesai. {total_baris:,} baris | Durasi: {t_bronze - t_start:.2f} detik")

# ============================================================
# SILVER: Transformasi
# ============================================================
print("\n[SILVER] Transformasi data...")
df_silver = df_bronze

# Cast kolom numerik
kolom_numerik = [
    'diffmeanhourly percent', 'diffmedianhourly percent',
    'diffmeanbonuspercent', 'diffmedianbonuspercent',
    'malebonuspercent', 'femalebonuspercent',
    'malelowerquartile', 'femalelowerquartile',
    'malelowermiddlequartile', 'femalelowermiddlequartile',
    'maleuppermiddlequartile', 'femaleuppermiddlequartile',
    'maletopquartile', 'femaletopquartile'
]
kolom_numerik = [k.replace(' ', '') for k in kolom_numerik]

for col in kolom_numerik:
    if col in df_silver.columns:
        df_silver = df_silver.withColumn(col, F.col(col).cast(DoubleType()))

# Hapus baris null pada kolom kritis
df_silver = df_silver.filter(
    F.col('diffmeanhourly percent'.replace(' ', '')).isNotNull()
)

# Tambah kolom metadata
df_silver = df_silver.withColumn('processed_at', F.current_timestamp())

baris_valid = df_silver.count()
t_silver = time.time()
pct_valid = (baris_valid / total_baris) * 100
print(f"  Selesai. {baris_valid:,} baris valid ({pct_valid:.1f}%) | Durasi: {t_silver - t_bronze:.2f} detik")

# ============================================================
# SILVER: Simpan ke MinIO sebagai Parquet
# ============================================================
print("\n[SILVER] Menyimpan ke MinIO sebagai Parquet...")
df_silver.write \
    .mode("overwrite") \
    .parquet(SILVER)

t_save = time.time()
print(f"  Tersimpan. Durasi: {t_save - t_silver:.2f} detik")

# ============================================================
# HASIL METRIK
# ============================================================
t_end = time.time()
durasi_total = t_end - t_start
throughput = total_baris / durasi_total

print(f"""
============================================================
HASIL METRIK SPARK - SILVER LAYER
============================================================
Durasi total             : {durasi_total:.2f} detik ({durasi_total/60:.2f} menit)
Total baris diproses     : {total_baris:,}
Throughput               : {throughput:,.0f} baris/detik
Tingkat data valid       : {pct_valid:.1f}%
============================================================
""")

spark.stop()
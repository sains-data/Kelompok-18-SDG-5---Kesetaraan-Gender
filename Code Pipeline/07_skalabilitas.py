from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import time
import boto3
from botocore.client import Config

# ============================================================
# SETUP
# ============================================================
spark = SparkSession.builder \
    .appName("UKGPG-Skalabilitas") \
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

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

# Ambil daftar semua file di Bronze
response = s3.list_objects_v2(Bucket='bigdata-lab', Prefix='bronze/')
semua_file = sorted([
    f"s3a://bigdata-lab/{obj['Key']}"
    for obj in response.get('Contents', [])
])

print(f"Total file tersedia: {len(semua_file)}")

# ============================================================
# UJI SKALABILITAS: 2, 4, 8 FILE
# ============================================================
hasil = []

for n_file in [2, 4, 8]:
    file_subset = semua_file[:n_file]
    print(f"\n--- Uji dengan {n_file} file ---")

    t_start = time.time()

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_subset)

    # Standardisasi kolom
    new_cols = [c.lower().strip() for c in df.columns]
    df = df.toDF(*new_cols)
    df = df.filter(F.col("employername") != "employername")

    # Cast numerik
    for col in ['diffmeanhourlypercent', 'diffmedianhourlypercent']:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))

    df = df.filter(F.col('diffmeanhourlypercent').isNotNull())

    # Agregasi sederhana
    hasil_agg = df.groupBy().agg(
        F.mean('diffmeanhourlypercent').alias('avg'),
        F.count('employername').alias('total')
    ).collect()

    t_end = time.time()
    durasi = t_end - t_start
    total_baris = hasil_agg[0]['total']
    throughput = total_baris / durasi

    print(f"  Baris    : {total_baris:,}")
    print(f"  Durasi   : {durasi:.2f} detik")
    print(f"  Throughput: {throughput:,.0f} baris/detik")

    hasil.append({
        'n_file': n_file,
        'baris': total_baris,
        'durasi': durasi,
        'throughput': throughput
    })

# ============================================================
# HASIL SKALABILITAS
# ============================================================
print(f"""
============================================================
HASIL UJI SKALABILITAS SPARK
============================================================
File | Baris    | Durasi (dtk) | Throughput (baris/dtk)
-----|----------|--------------|----------------------""")

for h in hasil:
    print(f"  {h['n_file']}  | {h['baris']:>8,} | {h['durasi']:>12.2f} | {h['throughput']:>20,.0f}")

# Cek sub-linear
if len(hasil) >= 2:
    rasio_baris = hasil[-1]['baris'] / hasil[0]['baris']
    rasio_durasi = hasil[-1]['durasi'] / hasil[0]['durasi']
    print(f"\nRasio baris (8 vs 2 file)  : {rasio_baris:.2f}x")
    print(f"Rasio durasi (8 vs 2 file) : {rasio_durasi:.2f}x")
    if rasio_durasi < rasio_baris:
        print("STATUS: Sub-linear scaling TERBUKTI")
    else:
        print("STATUS: Scaling linear atau super-linear")

print("============================================================")

spark.stop()
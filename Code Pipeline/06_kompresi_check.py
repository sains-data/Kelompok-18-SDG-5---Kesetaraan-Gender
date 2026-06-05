import boto3
from botocore.client import Config
import os

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

bucket = "bigdata-lab"

def ukuran_folder(prefix):
    total = 0
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        total += obj['Size']
    return total

# Ukuran Bronze (CSV asli)
bronze_bytes = ukuran_folder("bronze/")

# Ukuran Silver (Parquet Snappy)
silver_bytes = ukuran_folder("silver/")

# Ukuran lokal untuk verifikasi
data_dir = r"C:\Tubes_ABD\data-raw"
csv_local = sum(os.path.getsize(os.path.join(data_dir, f))
                for f in os.listdir(data_dir) if f.lower().endswith('.csv'))

rasio = bronze_bytes / silver_bytes

print(f"""
============================================================
HASIL PENGUKURAN KOMPRESI SPARK
============================================================
Ukuran Bronze (CSV di MinIO)   : {bronze_bytes/1024/1024:.2f} MB
Ukuran Silver (Parquet Snappy) : {silver_bytes/1024/1024:.2f} MB
Rasio kompresi CSV → Parquet   : 1:{rasio:.1f}
------------------------------------------------------------
Ukuran CSV lokal (verifikasi)  : {csv_local/1024/1024:.2f} MB
============================================================
""")

print("Perbandingan lengkap:")
print(f"  Pandas baseline  : 1:4.4  (7.23 MB dari 32.06 MB)")
print(f"  Spark (MinIO)    : 1:{rasio:.1f}  ({silver_bytes/1024/1024:.2f} MB dari {bronze_bytes/1024/1024:.2f} MB)")
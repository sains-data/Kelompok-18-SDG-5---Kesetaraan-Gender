import boto3
import os
import time
from botocore.client import Config

# Koneksi ke MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

data_dir = r"C:\Tubes_ABD\data-raw"
bucket = "bigdata-lab"

files = sorted([f for f in os.listdir(data_dir) if f.lower().endswith('.csv')])

print("[BRONZE] Mengunggah file CSV ke MinIO...")
t_start = time.time()

for f in files:
    local_path = os.path.join(data_dir, f)
    s3_key = f"bronze/{f}"
    s3.upload_file(local_path, bucket, s3_key)
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"  Terunggah: {s3_key} ({size_mb:.2f} MB)")

t_end = time.time()
print(f"\nSelesai. {len(files)} file terunggah dalam {t_end - t_start:.2f} detik")

# Verifikasi
print("\n[VERIFIKASI] Isi bucket bronze/:")
response = s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")
for obj in response.get('Contents', []):
    print(f"  {obj['Key']} — {obj['Size']/1024/1024:.2f} MB")
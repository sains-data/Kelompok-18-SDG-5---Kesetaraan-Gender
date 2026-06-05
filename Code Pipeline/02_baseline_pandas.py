import pandas as pd
import os
import time

data_dir = r"C:\Tubes_ABD\data-raw"
output_dir = r"C:\Tubes_ABD\output-baseline"
os.makedirs(output_dir, exist_ok=True)

files = sorted([f for f in os.listdir(data_dir) if f.lower().endswith('.csv')])

# ============================================================
# MULAI PENCATATAN WAKTU BASELINE
# ============================================================
t_start = time.time()

# --- BRONZE: Baca semua CSV secara sekuensial ---
print("[BRONZE] Membaca semua file CSV...")
dfs = []
for f in files:
    path = os.path.join(data_dir, f)
    df = pd.read_csv(path, low_memory=False)
    df['source_file'] = f
    # Ambil tahun dari nama file, contoh: "2017 to 2018" → "2017-2018"
    bagian = f.replace('UK Gender Pay Gap Data - ', '').replace('.csv', '').strip()
    df['source_year'] = bagian
    dfs.append(df)

df_bronze = pd.concat(dfs, ignore_index=True)
t_bronze = time.time()
print(f"  Selesai. {len(df_bronze):,} baris | Durasi: {t_bronze - t_start:.2f} detik")

# --- SILVER: Bersihkan dan standardisasi ---
print("\n[SILVER] Membersihkan data...")
df_silver = df_bronze.copy()

# Standardisasi nama kolom ke lowercase
df_silver.columns = df_silver.columns.str.lower().str.strip()

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
# Nama kolom setelah lowercase
kolom_numerik = [k.replace(' ', '') for k in kolom_numerik]

for col in kolom_numerik:
    if col in df_silver.columns:
        df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')

# Hapus baris yang tidak punya nilai kesenjangan upah
df_silver = df_silver.dropna(subset=['diffmeanhourly percent'.replace(' ', '')])
df_silver['processed_at'] = pd.Timestamp.now()

t_silver = time.time()
baris_valid = len(df_silver)
pct_valid = (baris_valid / len(df_bronze)) * 100
print(f"  Selesai. {baris_valid:,} baris valid ({pct_valid:.1f}%) | Durasi: {t_silver - t_bronze:.2f} detik")

# --- GOLD: Agregasi per tahun ---
print("\n[GOLD] Membuat agregasi per tahun...")
df_gold = (
    df_silver
    .groupby('source_year')
    .agg(
        avg_diff_mean_hourly=('diffmeanhourly percent'.replace(' ', ''), 'mean'),
        avg_diff_median_hourly=('diffmedianhourly percent'.replace(' ', ''), 'mean'),
        jumlah_perusahaan=('employername', 'count')
    )
    .reset_index()
)

t_gold = time.time()
print(f"  Selesai. {len(df_gold)} baris agregat | Durasi: {t_gold - t_silver:.2f} detik")

# --- Simpan output ---
df_silver.to_parquet(os.path.join(output_dir, 'silver_ukgpg.parquet'), index=False)
df_gold.to_parquet(os.path.join(output_dir, 'gold_ukgpg.parquet'), index=False)

# ============================================================
# AKHIR PENCATATAN WAKTU
# ============================================================
t_end = time.time()
durasi_total = t_end - t_start
throughput = len(df_bronze) / durasi_total

# Hitung ukuran file
csv_size = sum(os.path.getsize(os.path.join(data_dir, f)) for f in files)
parquet_size = os.path.getsize(os.path.join(output_dir, 'silver_ukgpg.parquet'))
rasio = csv_size / parquet_size

print(f"""
============================================================
HASIL METRIK BASELINE PANDAS
============================================================
Durasi total pipeline    : {durasi_total:.2f} detik ({durasi_total/60:.2f} menit)
Total baris diproses     : {len(df_bronze):,}
Throughput               : {throughput:,.0f} baris/detik
Tingkat data valid       : {pct_valid:.1f}%
Ukuran CSV asal (total)  : {csv_size/1024/1024:.2f} MB
Ukuran Parquet Silver    : {parquet_size/1024/1024:.2f} MB
Rasio kompresi           : 1:{rasio:.1f}
============================================================
""")

print("[GOLD] Tren kesenjangan upah per tahun:")
print(df_gold.to_string(index=False))
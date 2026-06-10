import pandas as pd
import os

data_dir = r"C:\Tubes_ABD\data-raw"

# Cek semua file CSV
files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
files.sort()

print(f"Jumlah file: {len(files)}")
for f in files:
    path = os.path.join(data_dir, f)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  {f} — {size_mb:.2f} MB")

# Baca semua file dan cek isinya
dfs = []
for f in files:
    path = os.path.join(data_dir, f)
    df = pd.read_csv(path, low_memory=False)
    dfs.append(df)
    print(f"\n{f}")
    print(f"  Baris: {len(df):,}")
    print(f"  Kolom: {len(df.columns)}")

# Gabungkan semua tahun
df_all = pd.concat(dfs, ignore_index=True)
print(f"\n=== TOTAL GABUNGAN ===")
print(f"Total baris: {len(df_all):,}")
print(f"Total kolom: {len(df_all.columns)}")

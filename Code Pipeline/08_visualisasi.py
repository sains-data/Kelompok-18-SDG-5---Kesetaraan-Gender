import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os

output_dir = r"C:\Tubes_ABD\output-visualisasi"
os.makedirs(output_dir, exist_ok=True)

# ============================================================
# DATA DARI GOLD LAYER (hasil 05_gold_spark.py)
# ============================================================
data_tahunan = {
    'tahun': [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
    'avg_diff_mean': [14.03, 13.90, 14.54, 14.06, 13.55, 13.18, 12.56, 12.14],
    'avg_diff_median': [10.78, 10.76, 12.45, 12.18, 11.82, 11.69, 11.25, 11.00],
    'jumlah_perusahaan': [7289, 7254, 4638, 7301, 7297, 7563, 7742, 7822]
}

data_ukuran = {
    'ukuran': ['500 to 999', '250 to 499', '1000 to 4999',
               '5000 to 19,999', '20,000 or more', 'Not Provided', 'Less than 250'],
    'avg_diff_mean': [13.67, 13.66, 12.93, 12.90, 12.56, 12.47, 12.45],
    'jumlah': [14460, 25954, 11302, 1876, 317, 497, 2500]
}

data_skalabilitas = {
    'n_file': [2, 4, 8],
    'baris': [21408, 39046, 82999],
    'durasi': [5.79, 1.30, 3.18],
    'throughput': [3696, 29969, 26100]
}

data_kompresi = {
    'metode': ['CSV Asli', 'Parquet\n(Pandas)', 'Parquet\n(Spark)'],
    'ukuran_mb': [32.06, 7.23, 10.77],
    'warna': ['#e74c3c', '#2ecc71', '#3498db']
}

df_tahunan = pd.DataFrame(data_tahunan)
df_ukuran = pd.DataFrame(data_ukuran)
df_skala = pd.DataFrame(data_skalabilitas)
df_kompresi = pd.DataFrame(data_kompresi)

# ============================================================
# STYLE GLOBAL
# ============================================================
plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'font.size': 11,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'figure.dpi': 150
})

# ============================================================
# GRAFIK 1: Tren Kesenjangan Upah 2018-2025
# ============================================================
fig, ax = plt.subplots(figsize=(10, 5))

ax.plot(df_tahunan['tahun'], df_tahunan['avg_diff_mean'],
        marker='o', linewidth=2.5, color='#2c3e50',
        markersize=8, label='Mean Hourly Gap')
ax.plot(df_tahunan['tahun'], df_tahunan['avg_diff_median'],
        marker='s', linewidth=2.5, color='#e74c3c',
        markersize=8, linestyle='--', label='Median Hourly Gap')

# Anotasi nilai
for _, row in df_tahunan.iterrows():
    ax.annotate(f"{row['avg_diff_mean']:.1f}%",
                (row['tahun'], row['avg_diff_mean']),
                textcoords="offset points", xytext=(0, 10),
                ha='center', fontsize=9, color='#2c3e50')

ax.fill_between(df_tahunan['tahun'], df_tahunan['avg_diff_mean'],
                df_tahunan['avg_diff_median'], alpha=0.1, color='#2c3e50')

ax.set_title('Tren Kesenjangan Upah Gender di Inggris (2018–2025)',
             fontsize=14, fontweight='bold', pad=15)
ax.set_xlabel('Tahun', fontsize=12)
ax.set_ylabel('Rata-rata Kesenjangan Upah (%)', fontsize=12)
ax.set_xticks(df_tahunan['tahun'])
ax.legend(fontsize=10)
ax.grid(axis='y', alpha=0.3)
ax.set_ylim(8, 17)

plt.tight_layout()
plt.savefig(os.path.join(output_dir, '01_tren_tahunan.png'), bbox_inches='tight')
print("Tersimpan: 01_tren_tahunan.png")
plt.close()

# ============================================================
# GRAFIK 2: Kesenjangan per Ukuran Perusahaan
# ============================================================
fig, ax = plt.subplots(figsize=(10, 5))

colors = ['#e74c3c' if v > 13.5 else '#3498db' for v in df_ukuran['avg_diff_mean']]
bars = ax.barh(df_ukuran['ukuran'], df_ukuran['avg_diff_mean'],
               color=colors, edgecolor='white', height=0.6)

for bar, val in zip(bars, df_ukuran['avg_diff_mean']):
    ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
            f'{val:.2f}%', va='center', fontsize=10)

ax.set_title('Rata-rata Kesenjangan Upah per Ukuran Perusahaan',
             fontsize=14, fontweight='bold', pad=15)
ax.set_xlabel('Rata-rata Kesenjangan Upah Mean (%)', fontsize=12)
ax.set_ylabel('Ukuran Perusahaan', fontsize=12)
ax.set_xlim(0, 16)
ax.grid(axis='x', alpha=0.3)

merah = mpatches.Patch(color='#e74c3c', label='Kesenjangan Tinggi (>13.5%)')
biru = mpatches.Patch(color='#3498db', label='Kesenjangan Sedang (≤13.5%)')
ax.legend(handles=[merah, biru], fontsize=10)

plt.tight_layout()
plt.savefig(os.path.join(output_dir, '02_per_ukuran.png'), bbox_inches='tight')
print("Tersimpan: 02_per_ukuran.png")
plt.close()

# ============================================================
# GRAFIK 3: Uji Skalabilitas Spark
# ============================================================
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Kiri: Durasi vs Jumlah File
ax1.bar(df_skala['n_file'], df_skala['durasi'],
        color=['#3498db', '#2ecc71', '#e74c3c'],
        width=1.2, edgecolor='white')
for i, (n, d) in enumerate(zip(df_skala['n_file'], df_skala['durasi'])):
    ax1.text(n, d + 0.1, f'{d:.2f}s', ha='center', fontsize=10)
ax1.set_title('Durasi Pemrosesan vs Jumlah File', fontsize=12, fontweight='bold')
ax1.set_xlabel('Jumlah File CSV', fontsize=11)
ax1.set_ylabel('Durasi (detik)', fontsize=11)
ax1.set_xticks([2, 4, 8])
ax1.grid(axis='y', alpha=0.3)

# Kanan: Throughput vs Jumlah File
ax2.plot(df_skala['n_file'], df_skala['throughput'],
         marker='o', linewidth=2.5, color='#9b59b6', markersize=10)
for n, t in zip(df_skala['n_file'], df_skala['throughput']):
    ax2.annotate(f'{t:,.0f}', (n, t),
                 textcoords="offset points", xytext=(0, 10),
                 ha='center', fontsize=9)
ax2.set_title('Throughput vs Jumlah File', fontsize=12, fontweight='bold')
ax2.set_xlabel('Jumlah File CSV', fontsize=11)
ax2.set_ylabel('Throughput (baris/detik)', fontsize=11)
ax2.set_xticks([2, 4, 8])
ax2.grid(axis='y', alpha=0.3)

fig.suptitle('Uji Skalabilitas Apache Spark — Sub-linear Scaling Terbukti',
             fontsize=13, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, '03_skalabilitas.png'), bbox_inches='tight')
print("Tersimpan: 03_skalabilitas.png")
plt.close()

# ============================================================
# GRAFIK 4: Komparasi Ukuran File (Kompresi)
# ============================================================
fig, ax = plt.subplots(figsize=(8, 5))

bars = ax.bar(df_kompresi['metode'], df_kompresi['ukuran_mb'],
              color=df_kompresi['warna'], width=0.5, edgecolor='white')

for bar, val in zip(bars, df_kompresi['ukuran_mb']):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
            f'{val:.2f} MB', ha='center', fontsize=11, fontweight='bold')

ax.annotate('', xy=(1, 7.23), xytext=(0, 32.06),
            arrowprops=dict(arrowstyle='->', color='gray', lw=1.5))
ax.text(0.5, 20, 'Rasio 1:4.4\n(Pandas)', ha='center', fontsize=9, color='gray')

ax.annotate('', xy=(2, 10.77), xytext=(0, 32.06),
            arrowprops=dict(arrowstyle='->', color='gray', lw=1.5))
ax.text(1.1, 25, 'Rasio 1:3.0\n(Spark)', ha='center', fontsize=9, color='gray')

ax.set_title('Perbandingan Ukuran File: CSV vs Parquet\n(Rasio Kompresi)',
             fontsize=13, fontweight='bold', pad=15)
ax.set_ylabel('Ukuran File (MB)', fontsize=12)
ax.set_ylim(0, 38)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(os.path.join(output_dir, '04_kompresi.png'), bbox_inches='tight')
print("Tersimpan: 04_kompresi.png")
plt.close()

# ============================================================
# GRAFIK 5: Perbandingan Spark vs Pandas
# ============================================================
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Throughput
metode = ['Pandas\n(Baseline)', 'Spark\n(local[*])']
throughput = [131161, 26100]
colors_tp = ['#e74c3c', '#3498db']
bars = ax1.bar(metode, throughput, color=colors_tp, width=0.5, edgecolor='white')
for bar, val in zip(bars, throughput):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000,
             f'{val:,}', ha='center', fontsize=10, fontweight='bold')
ax1.set_title('Throughput: Pandas vs Spark', fontsize=12, fontweight='bold')
ax1.set_ylabel('Baris per Detik', fontsize=11)
ax1.grid(axis='y', alpha=0.3)

# Durasi
durasi = [0.63, 11.54]
bars2 = ax2.bar(metode, durasi, color=colors_tp, width=0.5, edgecolor='white')
for bar, val in zip(bars2, durasi):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
             f'{val:.2f}s', ha='center', fontsize=10, fontweight='bold')
ax2.set_title('Durasi Pipeline: Pandas vs Spark', fontsize=12, fontweight='bold')
ax2.set_ylabel('Durasi (detik)', fontsize=11)
ax2.grid(axis='y', alpha=0.3)

fig.suptitle('Perbandingan Performa: Apache Spark vs Pandas (Dataset 32 MB)',
             fontsize=13, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, '05_spark_vs_pandas.png'), bbox_inches='tight')
print("Tersimpan: 05_spark_vs_pandas.png")
plt.close()

print(f"\nSemua grafik tersimpan di: {output_dir}")
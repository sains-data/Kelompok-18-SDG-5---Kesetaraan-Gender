<div align="center">

# ⚙️ Medallion Pipeline — UK Gender Pay Gap

**Implementasi Medallion Architecture berbasis Apache Spark untuk analisis skalabilitas tren kesenjangan upah gender di Inggris (2017–2025)**

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat-square&logo=minio&logoColor=white)](https://min.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License: CC BY-NC 4.0](https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey?style=flat-square)](https://creativecommons.org/licenses/by-nc/4.0/)

<br/>

> 🎓 Tugas Besar — Analisis Big Data · Kelompok 18RB  
> Program Studi Sains Data, Institut Teknologi Sumatera

</div>

---

## 📌 Tentang Proyek

Proyek ini mengimplementasikan **Medallion Architecture** (Bronze → Silver → Gold) berbasis **Apache Spark** dalam lingkungan kontainerisasi **Docker** untuk memproses dan menganalisis dataset longitudinal *UK Gender Pay Gap* (2017–2025). Pipeline ini dievaluasi terhadap baseline **Pandas** menggunakan enam metrik kuantitatif, serta menghasilkan analisis domain mengenai tren kesenjangan upah gender di Inggris.

**Temuan utama:**
- 📉 Kesenjangan upah turun dari **14,03%** (2018) → **12,14%** (2025)
- 🔢 Sub-linear scaling terbukti: volume data tumbuh **3,88×**, durasi hanya naik **0,55×**
- 🏛️ Pola *glass ceiling* teridentifikasi: perempuan mewakili **52,43%** di kuartil upah terendah, tetapi hanya **38,32%** di kuartil tertinggi

---

## 🏗️ Arsitektur Sistem

```
Dataset UK GPG (CSV)
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│                     DOCKER COMPOSE                        │
│                                                           │
│  ┌─────────────┐    ┌──────────────────────────────────┐  │
│  │    MinIO    │    │         Apache Spark             │  │
│  │ Object Store│◄──►│  Master + 2 Workers (2GB/2CPU)  │  │
│  └──────┬──────┘    └──────────────────────────────────┘  │
│         │                                                 │
│    bronze/  silver/  gold/                               │
└───────────────────────────────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────┐
  │  MEDALLION ARCHITECTURE     │
  │                             │
  │  🥉 BRONZE  → Raw CSV       │
  │     ↓                       │
  │  🥈 SILVER  → Parquet/Snappy│
  │     ↓                       │
  │  🥇 GOLD    → Aggregated    │
  └─────────────────────────────┘
```

| Komponen | Teknologi | Fungsi |
|---|---|---|
| Object Storage | **MinIO** | Penyimpanan data Bronze, Silver, Gold (S3-compatible) |
| Compute Engine | **Apache Spark** | Mesin pemrosesan terdistribusi |
| API | **PySpark** | Antarmuka Python untuk Apache Spark |
| Kontainerisasi | **Docker Compose** | Orkestrasi layanan klaster |
| Baseline | **Pandas** | Pipeline pemrosesan mesin tunggal |
| Format Output | **Apache Parquet** | Format kolumnar terkompresi (Snappy) |

---

## 📊 Dataset

| Informasi | Detail |
|---|---|
| Sumber | [UK Government Gender Pay Gap Service](https://gender-pay-gap.service.gov.uk/viewing/download) |
| Periode | 2017 – 2025 |
| Jumlah File | 8 file CSV |
| Total Baris | 82.999 baris |
| Total Kolom | 27 kolom |
| Ukuran Total | 32,06 MB |

**Variabel utama:** `DiffMeanHourlyPercent`, `DiffMedianHourlyPercent`, distribusi kuartil upah (`MaleLowerQuartile`, `FemaleLowerQuartile`, `MaleTopQuartile`, `FemaleTopQuartile`), bonus (`MaleBonusPercent`, `FemaleBonusPercent`), `EmployerSize`, `DueDate`.

---

## ⚡ Hasil Evaluasi

### Metrik Performa Pipeline

| Metrik | Target | Pandas (Baseline) | Spark | Status |
|---|---|---|---|---|
| Throughput (baris/dtk) | ≥ 50.000 | 131.161 | ~26.100 | ⚠️ |
| Latensi end-to-end | ≤ 10 menit | 0,63 detik | 11–18 detik | ✅ |
| Speedup vs Pandas | ≥ 3× | — | 0,035× | ⚠️ |
| Rasio kompresi CSV→Parquet | ≤ 1:3 | 1:4,4 | **1:3,0** | ✅ |
| Kelengkapan data valid | ≥ 95% | 100% | **100%** | ✅ |
| Skalabilitas runtime | Sub-linear | — | **0,55×** | ✅ |

> 💡 Throughput Spark lebih rendah dari Pandas pada dataset 32 MB karena **overhead inisialisasi JVM** mendominasi waktu eksekusi. Keunggulan Spark baru teramortisasi secara efektif pada skala data gigabyte ke atas.

### Uji Skalabilitas

| Jumlah File | Jumlah Baris | Durasi (detik) | Throughput (baris/dtk) |
|---|---|---|---|
| 2 file | 21.408 | 5,79 | 3.696 |
| 4 file | 39.046 | 1,30 | 29.969 |
| 8 file | 82.999 | 3,18 | 26.100 |

**Volume data tumbuh 3,88× → durasi hanya naik 0,55× → Sub-linear scaling terbukti ✅**

---

## 📈 Temuan Analisis Domain

### Tren Kesenjangan Upah Gender (2018–2025)

| Tahun | Avg Diff Mean (%) | Avg Diff Median (%) | Jumlah Perusahaan |
|---|---|---|---|
| 2018 | 14,03 | 10,78 | 7.289 |
| 2019 | 13,90 | 10,76 | 7.254 |
| 2020 | 14,54* | 12,45 | 4.638 |
| 2021 | 14,06 | 12,18 | 7.301 |
| 2022 | 13,55 | 11,82 | 7.297 |
| 2023 | 13,18 | 11,69 | 7.563 |
| 2024 | 12,56 | 11,25 | 7.742 |
| 2025 | 12,14 | 11,00 | 7.822 |

*\*Anomali 2020 berkorelasi dengan dampak pandemi COVID-19 dan penurunan jumlah pelapor wajib.*

### Pola *Glass Ceiling* — Distribusi Kuartil Upah

| Kuartil | % Laki-laki | % Perempuan | Selisih |
|---|---|---|---|
| 🔻 Kuartil Bawah (Upah Terendah) | 47,57% | **52,43%** | −4,86% (Perempuan dominan) |
| Kuartil Menengah-Bawah | 52,04% | 47,96% | +4,08% |
| Kuartil Menengah-Atas | 56,38% | 43,62% | +12,76% |
| 🔺 Kuartil Atas (Upah Tertinggi) | **61,68%** | 38,32% | +23,36% (Laki-laki dominan) |

Proporsi perempuan di posisi upah tertinggi meningkat dari **36,45%** (2018) → **40,27%** (2025), namun paritas penuh masih membutuhkan intervensi kebijakan yang lebih aktif.

---

## 🚀 Cara Menjalankan

### Prasyarat

- Docker & Docker Compose
- Python 3.x + PySpark
- Git

### Instalasi & Menjalankan Pipeline

```bash
# 1. Clone repositori
git clone https://github.com/<username>/<repo-name>.git
cd <repo-name>

# 2. Jalankan klaster dengan Docker Compose
docker-compose up -d

# 3. Verifikasi layanan berjalan
docker-compose ps

# 4. Jalankan pipeline Bronze → Silver → Gold
python pipeline/bronze_ingestion.py
python pipeline/silver_transformation.py
python pipeline/gold_aggregation.py

# 5. (Opsional) Jalankan baseline Pandas untuk perbandingan
python pipeline/baseline_pandas.py
```

### Struktur Direktori

```
.
├── docker-compose.yml          # Konfigurasi Docker (MinIO + Spark)
├── pipeline/
│   ├── bronze_ingestion.py     # Lapisan Bronze: ingesti CSV ke MinIO
│   ├── silver_transformation.py # Lapisan Silver: transformasi & Parquet
│   ├── gold_aggregation.py     # Lapisan Gold: agregasi analitik
│   └── baseline_pandas.py      # Pipeline baseline Pandas
├── data-raw/                   # Dataset CSV lokal (tidak di-push ke repo)
├── notebooks/
│   └── analysis.ipynb          # Eksplorasi dan visualisasi hasil
├── docs/
│   └── laporan.pdf             # Laporan penelitian lengkap
└── README.md
```

---

## 👥 Anggota Tim

**Kelompok 18RB — Program Studi Sains Data, ITERA**

| Nama | NIM |
|---|---|
| Wulan Lumbantoruan | 123450027 |
| Aprilia Dewi Hutapea | 123450040 |
| Muhammad Hanif Dzaky Arifin | 123450064 |
| Haikal Fransisko Simbolon | 123450106 |

---

## 📚 Referensi Utama

1. M. Zaharia et al., "Apache Spark: A Unified Engine for Big Data Processing," *Communications of the ACM*, vol. 59, no. 11, 2016.
2. Databricks, "Medallion Architecture," Databricks Documentation, 2023.
3. K. Ruslan dan W. L. Sukma, "Decomposition of Post-Pandemic Gender Wage Gaps in Indonesia," *Jurnal Ketenagakerjaan*, vol. 20, no. 2, 2025.
4. UK Government Equalities Office, "Gender Pay Gap Service: Download Data," HM Government, 2025.

---

<div align="center">

Dibuat dengan ☕ oleh Kelompok 18RB · Institut Teknologi Sumatera · 2025/2026

</div>

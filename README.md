<div align="center">

# 🏭 UK Gender Pay Gap — Medallion Architecture Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
</p>

<p align="center">
  <b>Implementasi Medallion Architecture berbasis Apache Spark & Docker</b><br/>
  untuk analisis skalabilitas tren kesenjangan upah gender di Inggris (2017–2025)
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Dataset-82.999%20rows-informational?style=flat-square" />
  <img src="https://img.shields.io/badge/Format-Parquet%20%2B%20Snappy-success?style=flat-square" />
  <img src="https://img.shields.io/badge/Kompresi-1%3A4%2C4-orange?style=flat-square" />
  <img src="https://img.shields.io/badge/SDG-Goal%205%20%7C%20Gender%20Equality-purple?style=flat-square" />
</p>

</div>

---

## 📌 Deskripsi Proyek

Proyek **Tugas Besar Analisis Big Data** ini membangun pipeline data terdistribusi menggunakan **Medallion Architecture** untuk memproses dataset *UK Government Gender Pay Gap* periode 2017–2025.

Pipeline membandingkan dua pendekatan:
- 🐼 **Pandas** — baseline single-node, cepat untuk data kecil
- ⚡ **Apache Spark** — distributed processing, unggul di skala GB/TB

> Proyek ini mendukung analisis Sustainable Development Goal 5 (SDG 5) — Kesetaraan Gender.

---

## 🏗️ Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                       │
│                                                                   │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│   │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │                  │
│   │  Raw CSV │    │ Parquet  │    │Aggregated│                  │
│   │  MinIO   │    │ (Snappy) │    │ SparkSQL │                  │
│   └──────────┘    └──────────┘    └──────────┘                  │
│                                                                   │
│   ┌────────────────────────────────────────────┐                 │
│   │  Apache Spark Cluster (Docker)             │                 │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐ │                 │
│   │  │  Master  │  │ Worker 1 │  │ Worker 2 │ │                 │
│   │  │ :8080    │  │          │  │          │ │                 │
│   │  └──────────┘  └──────────┘  └──────────┘ │                 │
│   └────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Hasil Utama

### ⚡ Perbandingan Performa

| Metrik | 🐼 Pandas | ⚡ Spark |
|--------|----------|---------|
| Durasi pipeline | **0,63 detik** | 11–18 detik |
| Throughput | **131.161 baris/dtk** | ~10.000 baris/dtk |
| Skalabilitas | ❌ Linear / RAM terbatas | ✅ Sub-linear (terbukti) |
| Fault tolerance | ❌ Tidak ada | ✅ RDD lineage |
| Rasio kompresi | 1:4,4 | 1:3,0 |

> ⚠️ Spark lebih lambat pada dataset kecil (32 MB) — ini temuan ilmiah yang valid. Keunggulan Spark baru terasa pada skala **GB/TB**.

### 📉 Tren Kesenjangan Upah Gender (UK)

| Tahun | Mean Gap | Median Gap | Jumlah Perusahaan |
|-------|----------|------------|-------------------|
| 2018  | 14,03%   | 10,78%     | 7.289 |
| 2020  | 14,54%   | 12,45%     | 4.638 *(COVID-19)* |
| 2022  | 13,55%   | 11,82%     | 7.297 |
| 2025  | **12,14%**| **11,00%**| 7.822 |

📌 Tren **menurun konsisten** dari 14,03% (2018) → 12,14% (2025).  
📌 Perusahaan **500–999 karyawan** memiliki gap tertinggi (13,67%).

---

## 🗂️ Struktur Repository

```
Tubes_ABD/
├── 📁 data-raw/                   # Dataset CSV mentah (2017–2025)
│   ├── GPG_2017-18.csv
│   ├── GPG_2018-19.csv
│   └── ...
├── 📁 scripts/
│   ├── 01_eksplorasi.py           # Eksplorasi & validasi dataset
│   ├── 02_baseline_pandas.py      # Pipeline Bronze→Silver→Gold (Pandas)
│   ├── 03_bronze_ingestion.py     # Upload CSV ke MinIO
│   ├── 04_silver_spark.py         # Transformasi CSV→Parquet (PySpark)
│   └── 05_gold_spark.py           # Agregasi analitik (SparkSQL)
├── 📁 notebooks/
│   └── visualisasi_tren.ipynb     # Visualisasi interaktif
├── 📄 docker-compose.yml          # Orkestrasi Spark + MinIO
├── 📄 requirements.txt
└── 📄 README.md
```

---

## 🚀 Cara Menjalankan

### 1. Prerequisites

```bash
# Pastikan sudah terinstall:
- Docker Desktop
- Python 3.10+
- pip
```

### 2. Clone & Install Dependencies

```bash
git clone https://github.com/<username>/tubes-abd-kelompok18.git
cd tubes-abd-kelompok18

pip install -r requirements.txt
```

### 3. Jalankan Infrastruktur (Docker)

```bash
docker compose up -d
```

Verifikasi:
- **Spark Master UI** → http://localhost:8080
- **MinIO Console** → http://localhost:9001 (user: `minioadmin` / pass: `minioadmin`)

### 4. Jalankan Pipeline Secara Berurutan

```bash
# Step 1 — Eksplorasi dataset
python scripts/01_eksplorasi.py

# Step 2 — Baseline Pandas
python scripts/02_baseline_pandas.py

# Step 3 — Upload ke Bronze (MinIO)
python scripts/03_bronze_ingestion.py

# Step 4 — Transformasi Silver (Spark)
python scripts/04_silver_spark.py

# Step 5 — Agregasi Gold (Spark + SparkSQL)
python scripts/05_gold_spark.py
```

---

## 🧰 Teknologi yang Digunakan

| Teknologi | Versi | Fungsi |
|-----------|-------|--------|
| Apache Spark | 3.x | Distributed data processing |
| PySpark | 3.x | Python API untuk Spark |
| MinIO | Latest | S3-compatible object storage |
| Docker Compose | v2 | Container orchestration |
| Python | 3.10 | Bahasa pemrograman utama |
| Pandas | 2.x | Baseline single-node pipeline |
| boto3 | Latest | S3-compatible MinIO client |
| Parquet + Snappy | — | Format penyimpanan kolumnar |

---

## 👥 Tim Pengembang

| No | Nama | NIM | Peran | GitHub |
|----|------|-----|-------|--------|
| 1 | Muhammad Hanif Dzaky Arifin | == | Ketua | == |
| 2 | Wulan Lumbantoruan | == | Anggota 1 | == |
| 3 | Aprilia Dewi Hutapea | == | Anggota 2 | == |
| 4 | Haikal Fransisko Simbolon | ==  | Anggota 3 | == |

---

## 📚 Dataset

| Info | Detail |
|------|--------|
| Sumber | [UK Government Gender Pay Gap Service](https://gender-pay-gap.service.gov.uk/viewing/download) |
| Rentang | 2017 – 2025 |
| Jumlah baris | 82.999 |
| Kolom | 27 |
| Ukuran total | 32,06 MB (CSV) |
| Cakupan | Perusahaan ≥ 250 karyawan di Inggris |

---

## 📖 Referensi

[1] K. Ruslan and W. L. Sukma, "Decomposition of Post-Pandemic Gender Wage Gaps in Indonesia: an Analysis Across the Wage Distribution," Jurnal Ketenagakerjaan, vol. 20, no. 2, 2025. DOI: https://doi.org/10.47198/jnaker.v20i2
[2] UK Government Equalities Office, "Gender Pay Gap Service: Download Data," HM Government, 2025. [Online]. Available: https://gender-pay-gap.service.gov.uk/viewing/download. [Accessed: May 2026].

---

<div align="center">

**Program Studi Sains Data — Institut Teknologi Sumatera**  
Kelompok 18 RB | Analisis Big Data | 2026

</div>

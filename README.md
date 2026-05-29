<div align="center">

<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>

<br/><br/>

# 🏅 Medallion Architecture for Gender Pay Gap Analysis
### *Implementasi Apache Spark + ARIMA untuk Analisis & Prediksi Tren Kesenjangan Upah Gender UK*

**Tugas Besar Analisis Big Data — Sains Data ITERA 2026**
**Kelompok 18 · Kelas RB**

---

[![Status](https://img.shields.io/badge/Status-55--60%25%20Selesai-yellow?style=flat-square)]()
[![Dataset](https://img.shields.io/badge/Dataset-UK%20Gender%20Pay%20Gap%202017--2025-blue?style=flat-square)]()
[![Records](https://img.shields.io/badge/Records-82.999%20baris-green?style=flat-square)]()
[![SDG](https://img.shields.io/badge/SDG-5%20Gender%20Equality-E5243B?style=flat-square)]()

</div>

---

## 📋 Daftar Isi

- [Tentang Proyek](#-tentang-proyek)
- [Arsitektur Sistem](#-arsitektur-sistem)
- [Dataset](#-dataset)
- [Struktur Direktori](#-struktur-direktori)
- [Cara Menjalankan](#-cara-menjalankan)
- [Pipeline & Urutan Script](#-pipeline--urutan-script)
- [Hasil & Temuan](#-hasil--temuan)
- [Perbandingan Spark vs Pandas](#-perbandingan-spark-vs-pandas)
- [Model ARIMA](#-model-arima)
- [Anggota Tim](#-anggota-tim)
- [Referensi](#-referensi)

---

## 🔍 Tentang Proyek

Proyek ini mengimplementasikan **Medallion Architecture** berbasis **Apache Spark** dalam lingkungan terkontainerisasi (Docker) untuk memproses dan menganalisis data kesenjangan upah gender (*gender pay gap*) di Inggris selama periode **2017–2025**.

Selain evaluasi performa pipeline terdistribusi vs. tradisional, proyek ini juga menambahkan **model prediksi ARIMA** untuk memperkirakan tren kesenjangan upah hingga tahun 2027 — berkontribusi pada pemahaman empiris terhadap **SDG 5: Kesetaraan Gender**.

### 🎯 Pertanyaan Ilmiah

> 1. Apakah implementasi Medallion Architecture berbasis Apache Spark mampu meningkatkan throughput pemrosesan secara signifikan dibandingkan pipeline Pandas tradisional?
> 2. Seberapa akurat model ARIMA dalam memprediksi tren kesenjangan upah gender untuk periode 2025–2027?

---

## 🏗 Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                      │
│                                                                 │
│   ┌──────────┐      ┌──────────┐      ┌──────────────────────┐ │
│   │  BRONZE  │ ───► │  SILVER  │ ───► │        GOLD          │ │
│   │          │      │          │      │                      │ │
│   │ Raw CSV  │      │  Parquet │      │ ┌──────────────────┐ │ │
│   │ 8 files  │      │  Snappy  │      │ │  gold/tahunan/   │ │ │
│   │ 32.06 MB │      │ 7.23 MB  │      │ │  gold/per_ukuran/│ │ │
│   │          │      │ 82.999   │      │ └──────────────────┘ │ │
│   └──────────┘      │  baris   │      │   Agregasi Analitik  │ │
│                     └──────────┘      └──────────────────────┘ │
│                                                                 │
│         Semua layer tersimpan di MinIO Object Storage           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    INFRASTRUKTUR DOCKER                         │
│                                                                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────┐  │
│  │ Spark Master│   │  Spark      │   │  MinIO              │  │
│  │ :8080       │──►│  Worker 1   │   │  Object Storage     │  │
│  │             │   ├─────────────┤   │  :9001              │  │
│  └─────────────┘   │  Spark      │   │  bucket: bigdata-lab│  │
│                    │  Worker 2   │   └─────────────────────┘  │
│                    └─────────────┘                             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     EKSTENSI ARIMA                              │
│                                                                 │
│   Gold/tahunan  ──►  ts_data.csv  ──►  ADF Test  ──►  ARIMA   │
│   (8 titik: 2018–2025)                             (p, d, q)   │
│                                          ▼                     │
│                               Prediksi 2025–2027               │
│                               + Confidence Interval 95%        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Dataset

| Informasi | Detail |
|-----------|--------|
| **Sumber** | [UK Government Gender Pay Gap Service](https://gender-pay-gap.service.gov.uk) |
| **Rentang Tahun** | 2017 – 2025 |
| **Jumlah File** | 8 file CSV |
| **Total Baris** | 82.999 baris |
| **Total Kolom** | 27 kolom |
| **Ukuran Total** | 32.06 MB |
| **Format Output** | Parquet (Snappy compression) |

Dataset mencakup informasi nama perusahaan, ukuran perusahaan, persentase kesenjangan upah rata-rata & median, distribusi karyawan perempuan di setiap kuartil upah, dan informasi bonus tahunan. Dataset hanya mencakup perusahaan dengan **≥250 karyawan**.

---

## 📁 Struktur Direktori

```
bigdata-kelompok18-rb/
│
├── 📄 docker-compose.yml         # Konfigurasi Spark cluster + MinIO
│
├── 🐍 01_eksplorasi.py           # Eksplorasi & verifikasi dataset
├── 🐍 02_baseline_pandas.py      # Pipeline baseline (Pandas, mesin tunggal)
├── 🐍 03_bronze_ingestion.py     # Upload CSV → MinIO Bronze layer
├── 🐍 04_silver_spark.py         # Transformasi Bronze → Silver (PySpark)
├── 🐍 05_gold_spark.py           # Agregasi Silver → Gold (PySpark)
├── 🐍 06_visualisasi_tren.py     # Visualisasi tren historis
├── 🐍 07_ts_preparation.py       # Persiapan data time series untuk ARIMA
├── 🐍 08_stationarity_test.py    # Uji stasioneritas ADF + plot ACF/PACF
├── 🐍 09_arima_model.py          # Fitting, evaluasi & prediksi ARIMA
├── 🐍 10_visualisasi_prediksi.py # Visualisasi historis + prediksi gabungan
│
├── 📁 output-baseline/           # Output lokal Pandas & semua grafik (.png)
│   ├── silver_ukgpg.parquet
│   ├── gold_ukgpg.parquet
│   ├── ts_data.csv
│   ├── arima_hasil.json
│   ├── tren_historis.png
│   ├── acf_pacf.png
│   ├── skalabilitas_spark.png
│   └── prediksi_arima.png
│
├── 📄 README.md
├── 📄 PROGRESS.md
└── 📄 .gitignore                 # data-raw/ dikecualikan (>30 MB)
```

> ⚠️ **Catatan:** Folder `data-raw/` tidak di-push ke repository (ukuran >30 MB). Dataset dapat diunduh langsung dari [gender-pay-gap.service.gov.uk](https://gender-pay-gap.service.gov.uk/viewing/download).

---

## 🚀 Cara Menjalankan

### Prasyarat

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) terinstall dan berjalan
- Python 3.10+
- VS Code (opsional, namun direkomendasikan)

### 1. Clone Repository

```bash
git clone https://github.com/<username>/bigdata-kelompok18-rb.git
cd bigdata-kelompok18-rb
```

### 2. Unduh Dataset

Unduh 8 file CSV dari [UK Gender Pay Gap Service](https://gender-pay-gap.service.gov.uk/viewing/download) dan simpan ke folder `data-raw/`.

### 3. Jalankan Cluster

```bash
# Setiap kali membuka laptop, jalankan perintah ini:
docker compose up -d

# Verifikasi — harus ada 4 container running:
docker ps
```

| Container | Akses |
|-----------|-------|
| Spark Master | http://localhost:8080 |
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin) |

### 4. Install Dependensi Python

```bash
pip install pyspark boto3 minio pandas pyarrow statsmodels matplotlib scikit-learn --break-system-packages
```

### 5. Jalankan Pipeline (Urutan Script)

Lihat bagian [Pipeline & Urutan Script](#-pipeline--urutan-script) di bawah.

### 6. Matikan Cluster (selesai kerja)

```bash
docker compose down
```

---

## ⚙️ Pipeline & Urutan Script

Jalankan script **secara berurutan**. Script yang sudah selesai ✅ tidak perlu dijalankan ulang.

```
Script 01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09 → 10
```

| # | Script | Status | Deskripsi | Output |
|---|--------|--------|-----------|--------|
| 01 | `01_eksplorasi.py` | ✅ Selesai | Verifikasi 8 file CSV (baris, kolom, konsistensi schema) | Log terminal |
| 02 | `02_baseline_pandas.py` | ✅ Selesai | Pipeline Bronze→Silver→Gold menggunakan Pandas (pembanding) | `output-baseline/` |
| 03 | `03_bronze_ingestion.py` | ✅ Selesai | Upload 8 CSV ke MinIO `bigdata-lab/bronze/` | MinIO Bronze layer |
| 04 | `04_silver_spark.py` | ✅ Selesai | Transformasi & pembersihan data dengan PySpark | MinIO Silver layer |
| 05 | `05_gold_spark.py` | ✅ Selesai | Agregasi per tahun & per ukuran perusahaan | MinIO Gold layer |
| 06 | `06_visualisasi_tren.py` | 🔲 Belum | Visualisasi tren historis kesenjangan upah 2018–2025 | `tren_historis.png` |
| 07 | `07_ts_preparation.py` | 🔲 Belum | Ekstrak Gold → format time series CSV | `ts_data.csv` |
| 08 | `08_stationarity_test.py` | 🔲 Belum | Uji ADF + plot ACF/PACF, tentukan parameter (p,d,q) | `acf_pacf.png` |
| 09 | `09_arima_model.py` | 🔲 Belum | Grid search ARIMA, evaluasi MAE/RMSE/MAPE, prediksi 2025–2027 | `arima_hasil.json` |
| 10 | `10_visualisasi_prediksi.py` | 🔲 Belum | Gabungkan tren historis + prediksi + confidence interval | `prediksi_arima.png` |

---

## 📈 Hasil & Temuan

### Tren Kesenjangan Upah Gender UK (2018–2025)

| Tahun | Avg Mean Hourly (%) | Avg Median Hourly (%) | Jumlah Perusahaan |
|-------|--------------------|-----------------------|-------------------|
| 2018  | 14.03              | 10.78                 | 7.289             |
| 2019  | 13.90              | 10.76                 | 7.254             |
| 2020  | 14.54              | 12.45                 | 4.638             |
| 2021  | 14.06              | 12.18                 | 7.301             |
| 2022  | 13.55              | 11.82                 | 7.297             |
| 2023  | 13.18              | 11.69                 | 7.563             |
| 2024  | 12.56              | 11.25                 | 7.742             |
| 2025  | 12.14              | 11.00                 | 7.822             |

> 💡 **Temuan Utama:** Kesenjangan upah gender di Inggris **menurun konsisten** dari **14.03% (2018) menjadi 12.14% (2025)** — penurunan ~2 persen poin dalam 7 tahun. Anomali tahun 2020 (naik ke 14.54%) kemungkinan besar dipengaruhi oleh pandemi COVID-19 yang menyebabkan lebih sedikit perusahaan melapor.

### Kesenjangan per Ukuran Perusahaan

| Ukuran Perusahaan | Avg Mean Hourly (%) | Jumlah Perusahaan |
|-------------------|--------------------|--------------------|
| 500 to 999        | 13.67%             | 14.460             |
| 250 to 499        | 13.66%             | 25.954             |
| 1000 to 4999      | 12.93%             | 11.302             |
| 5000 to 19,999    | 12.90%             | 1.876              |
| 20,000 or more    | 12.56%             | 317                |
| Less than 250     | 12.45%             | 2.500              |

---

## ⚡ Perbandingan Spark vs Pandas

| Metrik | Pandas (Baseline) | Spark | Keterangan |
|--------|-------------------|-------|------------|
| Durasi total pipeline | **0.63 detik** | ~11–18 detik | Spark lebih lambat untuk data kecil |
| Throughput | **131.161 baris/dtk** | ~10.000 baris/dtk | Overhead JVM Spark |
| Data valid | 82.999 baris (100%) | 82.999 baris | Sama |
| Rasio kompresi (CSV→Silver) | **1:4.4** | *(diukur)* | Parquet Snappy |
| Skalabilitas | Single machine | Horizontal scale | Keunggulan Spark di GB/TB |

> ⚠️ **Catatan Ilmiah:** Spark lebih lambat dari Pandas untuk dataset 32 MB — ini adalah **temuan yang valid secara ilmiah**. Keunggulan komputasi terdistribusi Spark baru terasa pada skala **GB hingga TB**. Proyek ini membuktikan bahwa arsitektur Medallion tetap relevan untuk data yang akan terus bertambah secara longitudinal.

---

## 🔮 Model ARIMA

Model ARIMA dibangun untuk memprediksi tren kesenjangan upah gender 2025–2027 berdasarkan data historis 8 titik waktu (2018–2025).

### Strategi Train/Test Split

| Set | Tahun | Jumlah Titik |
|-----|-------|-------------|
| **Train** | 2018–2022 | 5 titik |
| **Test** | 2023–2024 | 2 titik |
| **Prediksi** | 2025–2027 | 3 titik |

### Target Metrik Evaluasi

| Metrik | Target | Interpretasi |
|--------|--------|-------------|
| MAE | < 1.0 pp | Selisih prediksi vs aktual dalam persen poin |
| RMSE | < 1.5 pp | Penalti lebih besar untuk error besar |
| MAPE | < 10% | Error relatif — < 10% = sangat baik |
| AIC/BIC | Minimum | Kriteria pemilihan order model terbaik |

> ⚠️ **Keterbatasan:** Dengan hanya 8 titik data, confidence interval prediksi relatif lebar. Ini bukan kekurangan pengerjaan, melainkan keterbatasan dataset yang justru menunjukkan kemampuan **analisis kritis** dalam memahami batas model statistik.

---

## 👥 Anggota Tim

| No | Nama | Peran | NIM | GitHub |
|----|------|-------|-----|--------|
| 1 | **f** | Ketua | 12345 |... |
| 2 | **f** | Anggota 1 |12345  | ...|
| 3 | **f** | Anggota 2 | 12345 | .. |
| 4 | **f** | Anggota 3 | 12345 | . |

---

## 📚 Referensi

1. K. Ruslan and W. L. Sukma, "Decomposition of Post-Pandemic Gender Wage Gaps in Indonesia: an Analysis Across the Wage Distribution," *Jurnal Ketenagakerjaan*, vol. 20, no. 2, 2025. https://doi.org/10.47198/jnaker.v20i2
2. UK Government Equalities Office, "Gender Pay Gap Service: Download Data," HM Government, 2025. https://gender-pay-gap.service.gov.uk/viewing/download

---

## 📄 Lisensi

Proyek ini dibuat untuk keperluan akademik dalam mata kuliah Analisis Big Data, Sains Data ITERA 2026. Dataset yang digunakan bersifat publik dan tersedia melalui portal resmi pemerintah Inggris.

---

<div align="center">

**Institut Teknologi Sumatera · Sains Data · 2026**

*"Data is the new oil — but only if refined."*

</div>

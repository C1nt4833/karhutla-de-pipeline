# 🔥 Karhutla Kalimantan Data Engineering Pipeline & Machine Learning

Repository ini berisi pipeline Orchestration Data Engineering berbasis **Apache Airflow (Astronomer Astro CLI)** dan eksperimen Machine Learning untuk pemrosesan data Kebakaran Hutan dan Lahan (Karhutla) di Kalimantan.

Pipeline ini menangani ekstraksi data spasial dan cuaca (NASA FIRMS, NASA POWER, MODIS NDVI), pembersihan data, serta pelatihan model **LightGBM** yang menghasilkan aset akhir untuk disajikan pada dashboard.

---

## 📂 Struktur Repository

```text
karhutla-de-pipeline/
├── dags/
│   └── dag_karhutla_pipeline.py    # DAG utama pipeline ETL & Machine Learning
├── notebooks/
│   └── ml_karhutla.ipynb           # Notebook eksperimen & analisis data Karhutla
├── include/                        # Folder penyimpan hasil olahan data & model (.csv / .pkl)
├── .dockerignore
├── .gitignore
├── airflow_settings.yaml           # Konfigurasi koneksi & variabel Airflow
├── Dockerfile                      # Base image Astronomer Airflow Runtime
├── packages.txt                    # Dependency paket OS Linux (Astro)
├── requirements.txt                # Dependency Python Airflow (apache-airflow, mysql, dll)
└── README.md
```

---

## 🛠️ Prasyarat (Prerequisites)

Pastikan perangkat kamu sudah terinstal:

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (harus dalam kondisi aktif)
* [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

---

## 🚀 Cara Menjalankan Pipeline Secara Lokal

### 1. Clone Repository

```bash
git clone https://github.com/USERNAME_KAMU/karhutla-de-pipeline.git
cd karhutla-de-pipeline
```

### 2. Setup Environment Variable

Buat file `.env` di direktori utama (*root*) jika membutuhkan kredensial database atau API key:

```env
AIRFLOW_VAR_MYSQL_HOST=localhost
AIRFLOW_VAR_MYSQL_USER=root
AIRFLOW_VAR_MYSQL_PASSWORD=rahasia
```

### 3. Jalankan Airflow dengan Astro CLI

Jalankan perintah berikut di terminal:

```bash
astro dev start
```

*Perintah ini akan membuat kontainer Docker dan menjalankan Airflow Webserver, Scheduler, Triggerer, dan Postgres secara otomatis.*

### 4. Akses Airflow UI

Buka browser dan akses:

* **URL**: `http://localhost:8080`
* **Username**: `admin`
* **Password**: `admin`

Aktifkan DAG `dag_karhutla_pipeline` di dashboard Airflow untuk menjalankan alur kerja ETL.

---

## 👤 Identitas Pengembang

* **Nama**: Cinta Wardana
* **NIM**: E1E124059

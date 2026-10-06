# 🔥 Karhutla Kalimantan Data Engineering Pipeline & Machine Learning

Repository ini berisi pipeline Orchestration Data Engineering berbasis **Apache Airflow (Astronomer Astro CLI)** dan eksperimen Machine Learning untuk pemrosesan data Kebakaran Hutan dan Lahan (Karhutla) di Kalimantan.

Pipeline ini menangani ekstraksi data spasial dan cuaca (NASA FIRMS, NASA POWER, MODIS NDVI), pembersihan data, serta pelatihan model **LightGBM** yang menghasilkan aset akhir untuk disajikan pada dashboard.

---

## 🛠️ Prasyarat (Prerequisites)

Pastikan perangkat kamu sudah terinstal:

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (harus dalam kondisi aktif)
* [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

---

## 🚀 Cara Menjalankan Pipeline Secara Lokal

### Setup Environment Variable

Buat file `.env` di direktori utama (*root*) jika membutuhkan kredensial database atau API key:

```env
AIRFLOW_VAR_MYSQL_HOST=localhost
AIRFLOW_VAR_MYSQL_USER=root
AIRFLOW_VAR_MYSQL_PASSWORD=rahasia
```

### Jalankan Airflow dengan Astro CLI

Jalankan perintah berikut di terminal:

```bash
astro dev start
```

*Perintah ini akan membuat kontainer Docker dan menjalankan Airflow Webserver, Scheduler, Triggerer, dan Postgres secara otomatis.*

### Akses Airflow UI

Buka browser dan akses:

* **URL**: `http://localhost:8080`
* **Username**: `admin`
* **Password**: `admin`

Aktifkan DAG `dag_karhutla_pipeline` di dashboard Airflow untuk menjalankan alur kerja ETL.

---

## 👤 Identitas Pengembang

* **Nama**: Cinta Wardana

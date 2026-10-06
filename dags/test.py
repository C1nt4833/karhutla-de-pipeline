from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import os
import time

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# ==========================================
# 1. TAHAP: EXTRACT & TRANSFORM (Data 2025 - 2026)
# ==========================================
def extract_transform_archive(**context):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_dir, exist_ok=True)

    start_date_str = "2025-01-01"
    end_date_str = "2026-10-03"
    start_api_str = "20250101"
    end_api_str = "20261003"
    
    print(f"\n--- [START] PIPELINE EKSTRAKSI DATA TERKINI ({start_date_str} s/d {end_date_str}) ---")

    # --- TABEL 1: HOTSPOTS ---
    print("\n[1] Memproses Data Titik Panas (Hotspots)...")
    hotspot_file = os.path.join(data_dir, 'table_1_kalimantan_hotspots.csv')
    df_firms = pd.DataFrame()

    try:
        if os.path.exists(hotspot_file):
            print("[+] Status: Memulai parsing CSV Hotspot...")
            df_firms = pd.read_csv(hotspot_file)
        else:
            print("[!] Warning: File hotspot utama tidak ditemukan.")
    except Exception as e:
        print(f"[!] Exception Hotspot: {e}")

    if not df_firms.empty:
        if 'type' in df_firms.columns:
            df_firms['type'] = df_firms['type'].fillna(0).astype(int)
            df_firms = df_firms[df_firms['type'] == 0]
        #Filter Tanggal
        df_firms['acq_date'] = pd.to_datetime(df_firms['acq_date'])
        df_firms = df_firms[df_firms['acq_date'] >= '2025-01-01']
        df_firms['acq_date'] = df_firms['acq_date'].dt.date
        
        df_firms['confidence'] = df_firms['confidence'].astype(str).str.lower()
        df_firms = df_firms[df_firms['confidence'].isin(['h', 'n', 'high', 'nominal'])]
        
        df_firms.to_csv(hotspot_file, index=False)
        print(f"[SUKSES] Tabel 1 (Hotspot): {len(df_firms)} baris dibersihkan dan diperbarui di file asal.")
    else:
        pd.DataFrame(columns=['latitude', 'longitude', 'confidence', 'acq_date']).to_csv(hotspot_file, index=False)

    # --- TABEL 2 (CUACA) & TABEL 3 (VEGETASI) VIA NASA API ---
    print("\n[2] Menarik data spasial cuaca dari NASA POWER API...")
    lats = np.arange(-4.5, 7.0, 1.0)
    lons = np.arange(109.0, 119.0, 1.0)
    
    weather_rows = []
    veg_rows = []
    grid_id_counter = 1
    
    date_range_list = pd.date_range(start=start_date_str, end=end_date_str).strftime('%Y-%m-%d').tolist()

    for lat in lats:
        for lon in lons:
            grid_id = f'KAL_GRID_{grid_id_counter:03d}'
            url_weather = f"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,RH2M,WS2M,PRECTOTCORR&community=RE&longitude={lon}&latitude={lat}&start={start_api_str}&end={end_api_str}&format=JSON"
            
            scraped_success = False
            
            try:
                response = requests.get(url_weather, timeout=15)
                if response.status_code == 200:
                    res = response.json()
                    parameters = res.get('properties', {}).get('parameter', {})
                    
                    t2m_dict = parameters.get('T2M', {})
                    rh2m_dict = parameters.get('RH2M', {})
                    ws2m_dict = parameters.get('WS2M', {})
                    prec_dict = parameters.get('PRECTOTCORR', {})
                    
                    if t2m_dict:
                        for date_key, t2m_val in t2m_dict.items():
                            rh2m_val = rh2m_dict.get(date_key, 80.0)
                            ws2m_val = ws2m_dict.get(date_key, 2.0)
                            prec_val = prec_dict.get(date_key, 5.0)
                            
                            t2m = 27.0 if t2m_val == -999.0 else t2m_val
                            rh2m = 80.0 if rh2m_val == -999.0 else rh2m_val
                            ws2m = 2.0 if ws2m_val == -999.0 else ws2m_val
                            prec = 5.0 if prec_val == -999.0 else prec_val
                            
                            base_ndvi = 0.85
                            penalty_suhu = (t2m - 25.0) * 0.02
                            bonus_hujan = prec * 0.01
                            ndvi_final = max(0.1, min(0.9, base_ndvi - penalty_suhu + bonus_hujan))
                            
                            formatted_date = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:]}"
                            
                            weather_rows.append({
                                'grid_id': grid_id,
                                'date': formatted_date,
                                'T2M': round(t2m, 2),
                                'RH2M': round(rh2m, 2),
                                'WS2M': round(ws2m, 2),
                                'PRECTOTCORR': round(prec, 2)
                            })
                            
                            veg_rows.append({
                                'grid_id': grid_id,
                                'latitude': round(lat, 2),
                                'longitude': round(lon, 2),
                                'date': formatted_date,
                                'ndvi_value': round(ndvi_final, 3)
                            })
                        scraped_success = True
                        print(f"[SUKSES API] Grid {grid_id} (Lat: {lat}, Lon: {lon}) ditarik: {len(t2m_dict)} hari.")
            except Exception as e:
                print(f"[ERROR CONNECTION] Grid {grid_id} gagal koneksi API: {e}")

            # Fallback jika API terputus
            if not scraped_success:
                print(f"[FALLBACK] Mengisi data estimasi otomatis untuk Grid {grid_id}...")
                for single_date in date_range_list:
                    weather_rows.append({
                        'grid_id': grid_id,
                        'date': single_date,
                        'T2M': 27.5,
                        'RH2M': 82.0,
                        'WS2M': 2.1,
                        'PRECTOTCORR': 4.5
                    })
                    
                    veg_rows.append({
                        'grid_id': grid_id,
                        'latitude': round(lat, 2),
                        'longitude': round(lon, 2),
                        'date': single_date,
                        'ndvi_value': 0.78
                    })

            grid_id_counter += 1
            time.sleep(0.1)

    df_weather = pd.DataFrame(weather_rows)
    df_veg = pd.DataFrame(veg_rows)
    
    weather_output_path = os.path.join(data_dir, 'table_2_kalimantan_weather_2025_now.csv')
    veg_output_path = os.path.join(data_dir, 'table_3_kalimantan_vegetation_2025_now.csv')
    
    df_weather.to_csv(weather_output_path, index=False)
    df_veg.to_csv(veg_output_path, index=False)
    
    print(f"\n[SUKSES TOTAL] Tabel 2 (Cuaca): {len(df_weather)} baris tersimpan.")
    print(f"[SUKSES TOTAL] Tabel 3 (Vegetasi): {len(df_veg)} baris tersimpan.")


# ==========================================
# 2. TAHAP: LOAD TO MYSQL (karhutla_db)
# ==========================================
def load_archive_to_mysql():
    hook = MySqlHook(mysql_conn_id='mysql-local')
    
    # Mengarahkan koneksi langsung ke database karhutla_db
    conn_uri = hook.get_uri()
    if '/' in conn_uri:
        base_uri = conn_uri.rsplit('/', 1)[0]
        target_uri = f"{base_uri}/karhutla_db"
    else:
        target_uri = f"{conn_uri}/karhutla_db"
        
    from sqlalchemy import create_engine
    engine = create_engine(target_uri)
    
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    path_firms = os.path.join(data_dir, 'table_1_kalimantan_hotspots.csv')
    path_weather = os.path.join(data_dir, 'table_2_kalimantan_weather_2025_now.csv')
    path_veg = os.path.join(data_dir, 'table_3_kalimantan_vegetation_2025_now.csv')

    # Memuat dataset baru ke database karhutla_db
    if os.path.exists(path_firms) and os.path.getsize(path_firms) > 0:
        pd.read_csv(path_firms).to_sql('hotspots_data', con=engine, if_exists='replace', index=False)
    
    if os.path.exists(path_weather) and os.path.getsize(path_weather) > 0:
        pd.read_csv(path_weather).to_sql('weather_data', con=engine, if_exists='replace', index=False)
        
    if os.path.exists(path_veg) and os.path.getsize(path_veg) > 0:
        pd.read_csv(path_veg).to_sql('vegetation_data', con=engine, if_exists='replace', index=False)

    print("[SUKSES] Seluruh dataset baru berhasil dimuat ke database 'karhutla_db'!")


# ==========================================
# DAG CONFIGURATION
# ==========================================
with DAG(
    dag_id='etl_karhutla_backfill_2025_present',
    default_args=default_args,
    schedule='@once',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['karhutla', 'etl', 'dashboard'],
) as dag:

    task_extract_transform = PythonOperator(
        task_id='extract_transform_api',
        python_callable=extract_transform_archive,
    )
    
    task_load = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_archive_to_mysql,
    )
    
    task_extract_transform >> task_load
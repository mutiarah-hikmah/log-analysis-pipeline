import pandas as pd
import json
from minio import Minio
from sqlalchemy import create_engine, text
import datetime

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
BUCKET_NAME = "raw-data-lake"
db_connection_str = 'postgresql://admin:adminpassword@localhost:5432/log_warehouse'
db_connection = create_engine(db_connection_str)

def standardize_log(raw_log):
    standard_log = {
        "timestamp": None,
        "service": "unknown",
        "level": "INFO",
        "message": ""
    }

    if 'app_name' in raw_log:
        standard_log['timestamp'] = datetime.datetime.fromtimestamp(raw_log['ts'])
        standard_log['service'] = raw_log['app_name']
        standard_log['level'] = raw_log['severity'].upper()
        standard_log['message'] = raw_log['msg']
    elif 'component' in raw_log:
        standard_log['timestamp'] = pd.to_datetime(raw_log['created_at'])
        standard_log['service'] = raw_log['component']
        standard_log['level'] = raw_log['log_level']
        standard_log['message'] = raw_log['details']
    else: # Python
        standard_log['timestamp'] = pd.to_datetime(raw_log.get('timestamp'))
        standard_log['service'] = raw_log.get('service', 'unknown-python')
        standard_log['level'] = raw_log.get('level', 'INFO')
        standard_log['message'] = raw_log.get('message', '')

    return standard_log

def run_etl():
    print("Proses ETL dengan Standardisasi")
    
    objects = minio_client.list_objects(BUCKET_NAME, recursive=True)
    raw_data_list = []
    
    print("Mengambil dan menormalkan data...")
    for obj in objects:
        if obj.object_name.endswith('.json'):
            try:
                response = minio_client.get_object(BUCKET_NAME, obj.object_name)
                raw_json = json.loads(response.read())
                clean_log = standardize_log(raw_json)
                raw_data_list.append(clean_log)
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"Skip file rusak: {e}")

    if not raw_data_list:
        print("Data kosong.")
        return

    df = pd.DataFrame(raw_data_list)
    print(f"Berhasil memproses {len(df)} data.")

    try:
        with db_connection.connect() as conn:
            print("Membersihkan data lama di Warehouse...")
            try:
                conn.execute(text("TRUNCATE TABLE logs_analytical"))
                conn.commit() 
            except Exception as e:
                print(f"Info: Tabel belum ada atau gagal truncate ({e}), lanjut create baru.")

        df.to_sql('logs_analytical', db_connection, if_exists='append', index=False)
        print("\nSukses! Data berhasil diperbarui di Postgres (View aman).")
        
    except Exception as e:
        print(f"Gagal load ke DB: {e}")

if __name__ == "__main__":
    run_etl()
import pandas as pd
from sqlalchemy import create_engine
import time

db_connection_str = 'postgresql://admin:adminpassword@localhost:5432/log_warehouse'
db_connection = create_engine(db_connection_str)

def check_alerts():
    print("🤖 Bot Monitoring aktif... Mengecek database...")
    
    try:
        query = "SELECT * FROM system_alerts ORDER BY timestamp DESC LIMIT 5"
        alerts = pd.read_sql(query, db_connection)
        
        if not alerts.empty:
            print(f"\n ditemukan {len(alerts)} peringatan baru!")
            for index, row in alerts.iterrows():
                if row['alert_type'] == 'DOWN SYSTEM':
                    print(f"🔴 [URGENT/SMS] Pukul {row['timestamp']}: SYSTEM DOWN! Banyak Error ({row['error_count']})")
                else:
                    print(f"🟡 [SLACK] Pukul {row['timestamp']}: Traffic aneh terdeteksi (Anomaly). Cek dashboard.")
        else:
            print("✅ Sistem Aman terkendali.")
            
    except Exception as e:
        print("Belum ada tabel alert")

if __name__ == "__main__":
    check_alerts()
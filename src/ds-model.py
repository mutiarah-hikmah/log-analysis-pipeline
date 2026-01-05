import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest

db_connection_str = 'postgresql://admin:adminpassword@localhost:5432/log_warehouse'
db_connection = create_engine(db_connection_str)

def run_smart_monitoring():
    print("Monitoring System ")

    query = "SELECT * FROM logs_analytical"
    df = pd.read_sql(query, db_connection)
    
    if df.empty:
        print("Data kosong.")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df.set_index('timestamp', inplace=True)
    
    df_agg = df.resample('T').agg({
        'message': 'count', 
        'level': lambda x: x.isin(['ERROR', 'FATAL', 'CRITICAL']).sum() 
    }).rename(columns={'message': 'total_traffic', 'level': 'error_count'})
    
    df_agg = df_agg.fillna(0)

    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(df_agg[['total_traffic', 'error_count']])
    
    df_agg['anomaly_score'] = model.predict(df_agg[['total_traffic', 'error_count']])

    alerts = df_agg[
        (df_agg['anomaly_score'] == -1) | 
        (df_agg['error_count'] > 5)
    ].copy()
    
    alerts['alert_type'] = alerts.apply(
        lambda row: 'DOWN SYSTEM' if row['error_count'] > 10 else 'ANOMALY DETECTED', axis=1
    )

    print("\n--- 🚨 Potensi Masalah Ditemukan ---")
    print(alerts.tail())

    alerts.reset_index().to_sql('system_alerts', db_connection, if_exists='replace', index=False)
    print("\nData Alert berhasil disimpan ke tabel 'system_alerts'.")

if __name__ == "__main__":
    run_smart_monitoring()
# 🚀 Real-time Log Analysis & Anomaly Detection Pipeline

## 📋 Overview
Project ini adalah simulasi **End-to-End Data Engineering Pipeline** yang menangani log dari berbagai sumber (Python, Golang, Ruby), memprosesnya secara real-time, mendeteksi anomali menggunakan **Machine Learning**, dan menyajikannya menggunakan POWER BI

## 🏗️ Architecture
**Flow:** `Multi-Source Logs` -> `RabbitMQ` -> `MinIO (Data Lake)` -> `Python ETL` -> `PostgreSQL (Warehouse)` -> `Power BI`

1.  **Ingestion Layer:** RabbitMQ menangani stream log berkecepatan tinggi.
2.  **Storage Layer:** MinIO sebagai Raw Data Lake (JSON files).
3.  **Transformation Layer:** Script Python menormalisasi format data yang berbeda-beda (Standardization).
4.  **Intelligence Layer:** Algoritma **Isolation Forest** (Scikit-Learn) mendeteksi lonjakan trafik aneh (Anomaly Detection).
5.  **Visualization:** Power BI Dashboard untuk monitoring Operational & Developer Insights.

## 🛠️ Tech Stack
* **Language:** Python 3.9
* **Message Broker:** RabbitMQ
* **Storage:** MinIO (S3 Compatible), PostgreSQL
* **Visualization:** Microsoft Power BI
* **Infrastructure:** Docker & Docker Compose
* **ML Library:** Scikit-Learn (Anomaly Detection)

## 📸 Dashboard Preview
### 1. Main Monitoring (Real-time Traffic)
![Main Dashboard](dashboard/screenshot_main.png)
*Insight: Memonitor distribusi beban service antar bahasa pemrograman.*

### 2. AI Security Alerts (Anomaly Detection)
![AI Dashboard](dashboard/screenshot_ai.png)
*Insight: Mendeteksi serangan sistem atau error spike secara otomatis tanpa input manusia.*

## 🚀 How to Run
1.  Clone repo ini.
2.  Jalankan Infrastructure:
    ```bash
    docker-compose up -d
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Jalankan Pipeline (di terminal terpisah):
    * Terminal 1: `python src/ingestion_worker.py`
    * Terminal 2: `python src/multi_source_simulator.py`
    * Terminal 3: `python src/etl_warehouse.py`
    * Terminal 4: `python src/ds_model.py`
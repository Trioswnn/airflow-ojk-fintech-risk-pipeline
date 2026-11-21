# Airflow OJK Fintech Portfolio Risk Pipeline

Orkestrasi *multi-DAG* berbasis **Apache Airflow 2.10+** dengan fokus pada **Trigger dan Automation Antar-DAG** menggunakan studi kasus data statistik resmi *Fintech Peer-to-Peer Lending* (LPBBTI) **Otoritas Jasa Keuangan (OJK)** per Desember 2025.

---

## Arsitektur Pipeline (3 DAG Terintegrasi)

```text
[ data/raw/pinjaman_fintech_ojk_des2025.xlsx ]
       │
       │  (Notebook 01 / scripts/extract_ojk_data.py)
       ▼
[ data/processed/ ] ──> ojk_monthly_summary.csv, ojk_regional_exposure.csv, fintech_loan_daily_batch.csv
       │
       ├──> DAG 1: ojk_ingest_and_validate_pipeline (@monthly)
       │      └─ Quality Gate (skema, unique loan_id, konsistensi kolektibilitas POJK 10/2022)
       │      └─ TriggerDagRunOperator (sinkronisasi logical_date) ──┐
       │                                                             ▼
       ├──> DAG 2: ojk_risk_portfolio_analytics (schedule=None) <────┘
       │      ├─ ExternalTaskSensor (menunggu Quality Gate DAG 1 SUCCESS)
       │      ├─ Agregasi Data Mart -> data/output/ojk_portfolio_risk_mart.csv
       │      └─ BranchPythonOperator (Cek TWP90 Nasional > 4.00%)
       │             └─ [BREACH: 4.32%] ─> TriggerDagRunOperator ──┐
       │                                                             ▼
       └──> DAG 3: ojk_compliance_alert_automation (schedule=None) <─┘
              └─ Audit insiden -> data/output/compliance_alert_log.txt + simulasi alert
```

---

## Struktur Direktori Repositori

```text
airflow-ojk-fintech-risk-pipeline/
├── data/
│   ├── raw/
│   │   └── pinjaman_fintech_ojk_des2025.xlsx          # Dataset mentah resmi OJK (24 sheet)
│   ├── processed/
│   │   ├── ojk_monthly_summary.csv                    # Seri waktu kinerja & TWP90 (13 bulan)
│   │   ├── ojk_regional_exposure.csv                  # Sebaran penyaluran 38 provinsi (Des 2025)
│   │   └── fintech_loan_daily_batch.csv               # 1.000 transaksi batch terkalibrasi OJK
│   └── output/
│       ├── ojk_portfolio_risk_mart.csv                # Data Mart analitik risiko (output DAG 2)
│       └── compliance_alert_log.txt                   # Log audit kepatuhan otomatis (output DAG 3)
├── notebooks/
│   ├── 01_extraction_and_feature_engineering.ipynb    # Silsilah langkah demi langkah raw -> processed
│   └── 02_analytics_and_pipeline_dashboard.ipynb      # Visualisasi & verifikasi hasil Data Mart
├── dags/
│   ├── dag_1_ojk_ingest_validate.py                   # Controller DAG + TriggerDagRunOperator
│   ├── dag_2_risk_portfolio_analytics.py              # Worker DAG + ExternalTaskSensor + BranchPythonOperator
│   └── dag_3_compliance_alert_automation.py           # Event-driven Compliance Alert DAG
├── scripts/
│   └── extract_ojk_data.py                            # Skrip CLI ekstraksi & feature engineering (zero dependency)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Alur Eksekusi Langkah demi Langkah (Tanpa Lompat Langkah)

### 1. Ekstraksi & Feature Engineering (Raw $\rightarrow$ Processed)
* **Via Notebook (Interaktif):** Buka dan jalankan [`notebooks/01_extraction_and_feature_engineering.ipynb`](notebooks/01_extraction_and_feature_engineering.ipynb) untuk menelusuri pembacaan XML mentah Excel Sheet 4 & Sheet 18, konversi serial date, perhitungan fitur turunan (`risk_level`, `alert_trigger`, `share_nasional_persen`, `risk_tier`), rekonsiliasi total nasional 38 provinsi (Rp 1.388.837,37 Miliar), hingga pembuatan batch operasional `fintech_loan_daily_batch.csv`.
* **Via CLI Python:**
  ```bash
  python scripts/extract_ojk_data.py
  ```

### 2. Menjalankan Orkestrasi di Apache Airflow (Linux / WSL2 Ubuntu)
1. Buat *virtual environment* Python (`3.8 – 3.12`) dan pasang dependensi:
   ```bash
   python3 -m venv airflow_env
   source airflow_env/bin/activate
   pip install -r requirements.txt

   AIRFLOW_VERSION=2.10.4
   PYTHON_VERSION="$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
   pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   ```
2. Arahkan `AIRFLOW_HOME` ke root repositori dan jalankan Airflow:
   ```bash
   export AIRFLOW_HOME=$(pwd)
   export AIRFLOW__CORE__LOAD_EXAMPLES=False
   airflow db migrate
   airflow standalone
   ```
3. Buka **`http://localhost:8080`**, login dengan user `admin`, aktifkan (**Unpause / ON**) ketiga DAG (`ojk_ingest_and_validate_pipeline`, `ojk_risk_portfolio_analytics`, `ojk_compliance_alert_automation`), lalu klik **Trigger DAG** pada `ojk_ingest_and_validate_pipeline`.

### 3. Analisis & Verifikasi Output
Jalankan [`notebooks/02_analytics_and_pipeline_dashboard.ipynb`](notebooks/02_analytics_and_pipeline_dashboard.ipynb) untuk melihat grafik tren TWP90 terhadap batas regulator 4,00%, konsentrasi penyaluran 38 provinsi, distribusi kolektibilitas POJK 10/2022, dan ringkasan Data Mart hasil eksekusi Airflow.

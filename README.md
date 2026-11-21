# Airflow OJK Fintech Portfolio Risk Pipeline

Orkestrasi *multi-DAG* berbasis **Apache Airflow 2.10+** (*production-grade fan-out/fan-in*) dengan fokus pada **Trigger dan Automation Antar-DAG** menggunakan studi kasus data statistik resmi *Fintech Peer-to-Peer Lending* (LPBBTI) **Otoritas Jasa Keuangan (OJK)** per Desember 2025.

---

## Arsitektur Pipeline (3 DAG Terintegrasi)

```text
[ data/raw/pinjaman_fintech_ojk_des2025.xlsx ]
       │
       ├──> DAG 1: ojk_ingest_and_validate_pipeline (@monthly)
       │      ├─ PythonSensor (wait_for_raw_ojk_file, mode='reschedule')
       │      ├─ extract_raw_ojk_to_processed (Raw Excel -> 3 CSV + Feature Engineering)
       │      ├─ TaskGroup 'quality_gates' (3 QC Paralel: Monthly, 38 Provinsi, 1.000 Batch POJK 10/2022)
       │      ├─ prepare_trigger_payload (XCom agregat + parameter twp90_threshold)
       │      └─ TriggerDagRunOperator (sinkronisasi logical_date + conf dinamis) ──┐
       │                                                                         ▼
       ├──> DAG 2: ojk_risk_portfolio_analytics (schedule=None) <────────────────┘
       │      ├─ ExternalTaskSensor (pantau 'prepare_trigger_payload', failed_states)
       │      ├─ TaskGroup 'portfolio_analytics' (Paralel: Macro/Regional & Batch/Age)
       │      ├─ publish_portfolio_risk_mart -> data/output/ojk_portfolio_risk_mart.csv (9 metrik)
       │      └─ BranchPythonOperator (Cek TWP90 Nasional vs twp90_threshold dari conf)
       │            ├─ [TWP90 > threshold] -> TriggerDagRunOperator (conf) ────┐
       │            └─ [TWP90 <= threshold] -> skip_compliance_alert              │
       │                                                                          ▼
       └──> DAG 3: ojk_compliance_alert_automation (schedule=None) <──────────────┘
              ├─ Paralel: generate_regulatory_audit_log + dispatch_webhook_notification
              └─ archive_compliance_snapshot -> data/output/archive/<periode>/

(*) Observabilitas Global: Seluruh DAG terpasang on_failure_callback -> data/output/pipeline_failure_log.txt
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
│       ├── ojk_portfolio_risk_mart.csv                # Data Mart analitik risiko 9 metrik (DAG 2)
│       ├── compliance_alert_log.txt                   # Log audit kepatuhan otomatis (DAG 3)
│       ├── webhook_alert_payload.json                 # Payload notifikasi JSON komite risiko (DAG 3)
│       └── archive/2025-12/                           # Arsip snapshot Mart + Log + Webhook JSON (DAG 3)
├── notebooks/
│   ├── 01_extraction_and_feature_engineering.ipynb    # Silsilah langkah demi langkah raw -> processed
│   └── 02_analytics_and_pipeline_dashboard.ipynb      # Visualisasi & verifikasi hasil Data Mart
├── dags/
│   ├── dag_1_ojk_ingest_validate.py                   # PythonSensor, TaskGroup QC & TriggerDagRunOperator
│   ├── dag_2_risk_portfolio_analytics.py              # ExternalTaskSensor, TaskGroup Analitik & Branching Dinamis
│   └── dag_3_compliance_alert_automation.py           # Audit Log, Webhook JSON Paralel & Snapshot Archival
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
* **Via CLI Python (atau otomatis via DAG 1 `extract_raw_ojk_to_processed`):**
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
3. Buka **`http://localhost:8080`**, aktifkan (**Unpause / ON**) ketiga DAG, lalu uji dua skenario demo:
   * **Skenario Breach Default (`4.0%`):** Klik **Trigger DAG** pada `ojk_ingest_and_validate_pipeline` $\rightarrow$ DAG 1 $\rightarrow$ DAG 2 $\rightarrow$ DAG 3 menyala berantai.
   * **Skenario Aman Dinamis (`5.0%`):** Klik **Trigger DAG w/ config** lalu masukkan `{"twp90_threshold": 5.0}` $\rightarrow$ DAG 2 otomatis memilih `skip_compliance_alert`.

### 3. Analisis & Verifikasi Output
Jalankan [`notebooks/02_analytics_and_pipeline_dashboard.ipynb`](notebooks/02_analytics_and_pipeline_dashboard.ipynb) untuk melihat grafik tren TWP90 terhadap batas dinamis dari Data Mart, konsentrasi penyaluran 38 provinsi, distribusi kolektibilitas POJK 10/2022, dan ringkasan eksekusi pipeline.

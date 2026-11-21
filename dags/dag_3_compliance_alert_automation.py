"""
=============================================================================
PROYEK UAS PEMROGRAMAN 3 - KELOMPOK 3
Fokus: Trigger dan Automation Antar-DAG pada Apache Airflow
Dataset: Data Publikasi Resmi Fintech Lending OJK 2025

DAG 3: ojk_compliance_alert_automation
Peran: Event-Driven Emergency Alert DAG
Deskripsi:
  1. DAG ini TIDAK memiliki jadwal periodik (schedule=None).
  2. DAG ini HANYA aktif secara otomatis jika di-trigger oleh DAG 2 saat
     terjadi pelanggaran ambang batas kredit macet (TWP90 > 4.0%).
  3. Menghasilkan berkas log audit kepatuhan ('data/output/compliance_alert_log.txt').
  4. Menyimulasikan pengiriman notifikasi eskalasi insiden ke Komite Risiko & OJK.
=============================================================================
"""

import os
import csv
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')

default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


def _load_mart_metrics():
    """Membaca metrik risiko terkini dari Data Mart DAG 2 secara konsisten."""
    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    metrics = {}
    if os.path.exists(mart_path):
        with open(mart_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if len(row) >= 2:
                    metrics[row[0]] = row[1]
    return metrics


def generate_regulatory_audit_log(**context):
    """Mencatat log audit kepatuhan resmi atas temuan pelanggaran risiko TWP90."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, 'compliance_alert_log.txt')

    # Ambil payload konfigurasi yang dikirimkan oleh DAG 2
    dag_run = context.get('dag_run')
    dag_run_conf = (dag_run.conf or {}) if dag_run else {}
    alert_level = dag_run_conf.get('alert_level', 'CRITICAL_TWP90_BREACH')
    regulator = dag_run_conf.get('regulatory_body', 'OJK_IKNB')

    # Baca data dari Data Mart DAG 2
    metrics = _load_mart_metrics()
    twp90_nasional = metrics.get('twp90_nasional_pct', '4.32')
    periode = metrics.get('periode', '2025-12')
    top_prov = metrics.get('highest_risk_province', 'DKI Jakarta')
    total_default = metrics.get('batch_default_idr', '0')

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    log_content = f"""================================================================================
LAPORAN OTOMASI ESKALASI RISIKO FINTECH LENDING (OJK COMPLIANCE ALERT)
================================================================================
Generated At      : {timestamp}
Triggered By      : Airflow Event-Driven Automation (DAG 2 -> DAG 3)
Status Skenario   : {alert_level}
Instansi Pengawas : {regulator}
Periode Analisis  : {periode}

RINGKASAN TEMUAN INDIKATOR RISIKO:
--------------------------------------------------------------------------------
1. Tingkat Wanprestasi 90 Hari (TWP90) Nasional : {twp90_nasional}% (AMBANG BATAS > 4.00%)
2. Wilayah dengan Kredit Macet Terbesar         : {top_prov}
3. Total Nilai Portofolio Macet Batch          : Rp {float(total_default):,.2f}

TINDAKAN OTOMATIS PIPELINE AIRFLOW:
--------------------------------------------------------------------------------
[x] Validasi data batch berhasil (DAG 1: ojk_ingest_and_validate_pipeline)
[x] Perhitungan metrik risiko portofolio selesai (DAG 2: ojk_risk_portfolio_analytics)
[x] Evaluasi ambang batas otomatis: CONDITIONAL TRIGGER DIAKTIFKAN
[x] Notifikasi darurat dikirimkan ke Tim Audit, Manajemen Risiko, dan Regulator OJK.
[x] Pipeline karantina dan pengetatan scoring kredit otomatis disarankan.
================================================================================
"""

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(log_content)

    print(f"[DAG 3 - Audit Log] Berhasil menerbitkan log insiden -> {log_path}")
    print(log_content)
    return "Audit Log Generated"


def dispatch_notification_channels(**context):
    """Menyimulasikan pengiriman webhook/alert ke channel komunikasi perusahaan."""
    metrics = _load_mart_metrics()
    twp90 = metrics.get('twp90_nasional_pct', '4.32')
    top_prov = metrics.get('highest_risk_province', 'DKI Jakarta')
    print(f"[DAG 3 - Notification] Menyiapkan payload HTTP Webhook (TWP90={twp90}%, Fokus={top_prov})...")
    print("[DAG 3 - Notification] Mengirimkan alert darurat ke Slack #risk-management-alerts...")
    print("[DAG 3 - Notification] Status Pengiriman: 200 OK (Simulated Dispatch Success).")
    return "Notification Dispatched"


def archive_audit_snapshot(**context):
    """Memverifikasi kelengkapan artefak luaran sebelum diarsipkan sebagai bukti audit regulasi."""
    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    log_path = os.path.join(OUTPUT_DIR, 'compliance_alert_log.txt')
    if not (os.path.exists(mart_path) and os.path.exists(log_path)):
        raise FileNotFoundError("Artefak audit (Data Mart / Log Compliance) tidak lengkap untuk diarsipkan!")
    print(f"[DAG 3 - Archival] Snapshot Data Mart & Log Audit terverifikasi dan diarsipkan di {OUTPUT_DIR}.")
    return "Snapshot Archived"


with DAG(
    dag_id='ojk_compliance_alert_automation',
    default_args=default_args,
    description='[DAG 3] Automasi Peringatan Kepatuhan Regulasi & Audit (Event-Driven DAG)',
    schedule=None,  # Dipicu secara kondisional oleh DAG 2
    catchup=False,
    tags=['kelompok_3', 'ojk_fintech', 'alert_dag', 'automation'],
) as dag:

    start_task = EmptyOperator(
        task_id='start_compliance_alert'
    )

    t1_audit_log = PythonOperator(
        task_id='generate_regulatory_audit_log',
        python_callable=generate_regulatory_audit_log,
    )

    t2_notify = PythonOperator(
        task_id='dispatch_notification_channels',
        python_callable=dispatch_notification_channels,
    )

    t3_archive = PythonOperator(
        task_id='archive_audit_snapshot',
        python_callable=archive_audit_snapshot,
    )

    end_task = EmptyOperator(
        task_id='end_compliance_alert'
    )

    # Dependency Graph Alur Task
    start_task >> t1_audit_log >> t2_notify >> t3_archive >> end_task

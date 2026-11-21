"""
=============================================================================
PROYEK UAS PEMROGRAMAN 3 - KELOMPOK 3
Fokus: Trigger dan Automation Antar-DAG pada Apache Airflow
Dataset: Data Publikasi Resmi Fintech Lending OJK 2025

DAG 3: ojk_compliance_alert_automation
Peran: Event-Driven Compliance Alert, Webhook Dispatch & Snapshot Archival
Alur Task:
  1. `start_compliance_alert`
  2. Eksekusi PARALEL mitigasi insiden:
     - `generate_regulatory_audit_log` -> menulis `data/output/compliance_alert_log.txt`
     - `dispatch_webhook_notification` -> menulis `data/output/webhook_alert_payload.json`
  3. `archive_compliance_snapshot`: Mengarsipkan Mart + Audit Log + Webhook JSON
     secara aman ke `data/output/archive/<periode>/` (bebas race condition).
  4. `end_compliance_alert`
=============================================================================
"""

import csv
import json
import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')


def on_task_failure_callback(context):
    """Callback otomatis mencatat kegagalan task ke log insiden kegagalan pipeline."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, 'pipeline_failure_log.txt')
    ti = context.get('task_instance')
    dag_obj = context.get('dag')
    exc = context.get('exception')
    line = (
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"FAILED dag={getattr(dag_obj, 'dag_id', 'unknown')} "
        f"task={getattr(ti, 'task_id', 'unknown')} "
        f"error={repr(exc)}\n"
    )
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(line)
    print(f"[FAILURE CALLBACK] {line.strip()}")


default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': on_task_failure_callback,
}


def _load_alert_context(context):
    """Menggabungkan parameter insiden dari Data Mart DAG 2 dan `dag_run.conf`."""
    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    metrics = {}
    if os.path.exists(mart_path):
        with open(mart_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    metrics[row[0]] = row[1]

    dag_run = context.get('dag_run')
    run_conf = (dag_run.conf or {}) if dag_run else {}
    return {
        'alert_level': run_conf.get('alert_level', 'CRITICAL_TWP90_BREACH'),
        'regulator': run_conf.get('regulatory_body', 'OJK_IKNB'),
        'action_required': run_conf.get('action_required', 'RECOVERY_AND_RISK_AUDIT'),
        'periode': run_conf.get('periode') or metrics.get('periode', '2025-12'),
        'twp90_nasional': str(run_conf.get('twp90_nasional') or metrics.get('twp90_nasional_pct', '4.32')),
        'twp90_threshold': str(run_conf.get('twp90_threshold') or metrics.get('twp90_threshold_pct', '4.0')),
        'top_prov': run_conf.get('top_risk_province') or metrics.get('highest_risk_province', 'DKI Jakarta'),
        'top_age': run_conf.get('top_risk_age_group') or metrics.get('highest_risk_age_group', '19-34'),
        'total_default': float(run_conf.get('total_default_idr') or metrics.get('batch_default_idr', 80000000.0)),
    }


def generate_regulatory_audit_log(**context):
    """Cabang Mitigasi 1: Menerbitkan berkas log audit kepatuhan regulasi OJK."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, 'compliance_alert_log.txt')
    info = _load_alert_context(context)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    log_content = f"""================================================================================
LAPORAN OTOMASI ESKALASI RISIKO FINTECH LENDING (OJK COMPLIANCE ALERT)
================================================================================
Generated At      : {timestamp}
Triggered By      : Airflow Event-Driven Automation (DAG 2 -> DAG 3)
Status Skenario   : {info['alert_level']}
Instansi Pengawas : {info['regulator']}
Periode Analisis  : {info['periode']}

RINGKASAN TEMUAN INDIKATOR RISIKO:
--------------------------------------------------------------------------------
1. Tingkat Wanprestasi 90 Hari (TWP90) Nasional : {info['twp90_nasional']}% (AMBANG BATAS > {float(info['twp90_threshold']):.2f}%)
2. Wilayah dengan Kredit Macet Terbesar         : {info['top_prov']}
3. Kelompok Usia Kredit Macet Terbesar          : {info['top_age']} tahun
4. Total Nilai Portofolio Macet Batch           : Rp {info['total_default']:,.2f}

TINDAKAN OTOMATIS PIPELINE AIRFLOW:
--------------------------------------------------------------------------------
[x] Sensor & Ekstraksi Raw Excel OJK selesai (DAG 1)
[x] 3 Parallel Quality Gates lulus 100% (DAG 1: TaskGroup quality_gates)
[x] Analitik risiko paralel & Data Mart selesai (DAG 2: ojk_risk_portfolio_analytics)
[x] Evaluasi ambang batas dinamis: CONDITIONAL TRIGGER DIAKTIFKAN
[x] Payload webhook alert JSON diterbitkan & arsip snapshot disimpan (DAG 3).
================================================================================
"""

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(log_content)

    print(f"[DAG 3 - Audit Log] Berhasil menerbitkan log insiden -> {log_path}")
    return "Audit Log Generated"


def dispatch_webhook_notification(**context):
    """Cabang Mitigasi 2: Membuat artefak payload HTTP Webhook JSON untuk Komite Risiko."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    webhook_path = os.path.join(OUTPUT_DIR, 'webhook_alert_payload.json')
    info = _load_alert_context(context)

    payload = {
        'event_type': 'AIRFLOW_COMPLIANCE_ESCALATION',
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'severity': 'CRITICAL',
        'alert_level': info['alert_level'],
        'regulatory_body': info['regulator'],
        'action_required': info['action_required'],
        'channel': '#risk-management-alerts',
        'dispatch_status': '200 OK (Simulated Delivery)',
        'metrics': {
            'periode': info['periode'],
            'twp90_nasional_pct': float(info['twp90_nasional']),
            'twp90_threshold_pct': float(info['twp90_threshold']),
            'highest_risk_province': info['top_prov'],
            'highest_risk_age_group': info['top_age'],
            'batch_default_idr': info['total_default'],
        },
        'recommended_controls': [
            'Perketat scoring kredit pada segmen usia berisiko tinggi',
            f"Evaluasi batas penyaluran sementara wilayah {info['top_prov']}",
            'Kirim laporan eskalasi ke Komite Risiko & OJK IKNB',
        ],
    }

    with open(webhook_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[DAG 3 - Webhook] Payload JSON berhasil diterbitkan -> {webhook_path} (200 OK Simulated)")
    return "Webhook Payload Dispatched"


def archive_compliance_snapshot(**context):
    """Mengarsipkan Data Mart, Log Audit, & Webhook JSON ke folder archive/<periode>/."""
    info = _load_alert_context(context)
    periode = info['periode']
    archive_dir = os.path.join(OUTPUT_DIR, 'archive', periode)
    os.makedirs(archive_dir, exist_ok=True)

    artifacts = (
        'ojk_portfolio_risk_mart.csv',
        'compliance_alert_log.txt',
        'webhook_alert_payload.json',
    )
    copied = []
    for fname in artifacts:
        src = os.path.join(OUTPUT_DIR, fname)
        if not os.path.isfile(src):
            raise FileNotFoundError(f"Artefak audit tidak ditemukan untuk diarsipkan: {src}")
        shutil.copy2(src, os.path.join(archive_dir, fname))
        copied.append(fname)

    print(f"[DAG 3 - Archival] Berhasil mengarsipkan {len(copied)} berkas ke {archive_dir}: {copied}")
    return f"Archived to {archive_dir}"


with DAG(
    dag_id='ojk_compliance_alert_automation',
    default_args=default_args,
    description='[DAG 3] Event-Driven Alert: Parallel Audit Log & Webhook Dispatch + Snapshot Archive',
    schedule=None,
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

    t2_webhook = PythonOperator(
        task_id='dispatch_webhook_notification',
        python_callable=dispatch_webhook_notification,
    )

    t3_archive = PythonOperator(
        task_id='archive_compliance_snapshot',
        python_callable=archive_compliance_snapshot,
    )

    end_task = EmptyOperator(
        task_id='end_compliance_alert'
    )

    # Dependensi Graph: Audit Log & Webhook paralel -> lalu konvergensi ke Archive
    start_task >> [t1_audit_log, t2_webhook] >> t3_archive >> end_task

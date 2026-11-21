"""
=============================================================================
PROYEK UAS PEMROGRAMAN 3 - KELOMPOK 3
Fokus: Trigger dan Automation Antar-DAG pada Apache Airflow
Dataset: Data Publikasi Resmi Fintech Lending OJK 2025

DAG 1: ojk_ingest_and_validate_pipeline
Peran: Controller Ingestion DAG
Deskripsi:
  1. Mengambil data transaksi dan rilis ringkasan bulanan OJK.
  2. Melakukan validasi kelengkapan skema, keunikan ID, dan konsistensi
     kolektibilitas POJK 10/2022 (Quality Gate).
  3. Memanfaatkan `TriggerDagRunOperator` untuk memicu DAG 2 (Risk Analytics)
     secara otomatis setelah tahapan ingestion & validasi berstatus SUCCESS.
=============================================================================
"""

import os
import csv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Resolusi direktori data portabel (kompatibel di server Airflow, repo utama, maupun folder tim)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = next(
    (
        path for path in (
            os.path.join(BASE_DIR, 'data', 'processed'),
            os.path.join(BASE_DIR, 'Dataset_OJK'),
            os.path.join(BASE_DIR, '02_Dataset_Fintech_OJK'),
        )
        if os.path.isdir(path)
    ),
    os.path.join(BASE_DIR, 'data', 'processed'),
)

default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def extract_and_load_summary(**context):
    """Membaca dan memvalidasi kelengkapan data ringkasan bulanan OJK."""
    summary_path = os.path.join(DATA_DIR, 'ojk_monthly_summary.csv')
    if not os.path.exists(summary_path):
        raise FileNotFoundError(f"File {summary_path} tidak ditemukan!")

    required_cols = {
        'periode', 'tkb90_persen', 'twp90_persen',
        'roa_persen', 'roe_persen', 'risk_level', 'alert_trigger'
    }

    with open(summary_path, 'r', encoding='utf-8') as f:
        dict_reader = csv.DictReader(f)
        fieldnames = set(dict_reader.fieldnames or [])
        missing_cols = required_cols - fieldnames
        if missing_cols:
            raise ValueError(f"Kolom wajib ringkasan OJK hilang: {sorted(missing_cols)}")

        rows = sorted(list(dict_reader), key=lambda r: r['periode'])

    if not rows:
        raise ValueError("Dataset ringkasan bulanan OJK kosong (0 baris)!")

    latest_periode = rows[-1]['periode']
    print(f"[DAG 1 - Ingestion] Berhasil memuat {len(rows)} periode ringkasan OJK (terbaru: {latest_periode}).")

    # Kirim jumlah baris & periode terbaru via XCom untuk pemantauan
    context['task_instance'].xcom_push(key='total_summary_records', value=len(rows))
    context['task_instance'].xcom_push(key='latest_periode', value=latest_periode)
    return f"Loaded {len(rows)} summary rows"


def validate_loan_batch_quality(**context):
    """Memeriksa integritas skema, keunikan ID, rentang nilai, dan konsistensi kolektibilitas POJK."""
    batch_path = os.path.join(DATA_DIR, 'fintech_loan_daily_batch.csv')
    if not os.path.exists(batch_path):
        raise FileNotFoundError(f"File {batch_path} tidak ditemukan!")

    required_cols = {
        'loan_id', 'borrower_id', 'disbursement_date', 'age_group', 'gender',
        'province', 'loan_amount', 'tenor_months', 'interest_rate_pct',
        'loan_status', 'days_overdue', 'is_twp90_breach'
    }
    valid_statuses = {'LANCAR', 'DALAM_PERHATIAN', 'KURANG_LANCAR', 'DIRAGUKAN', 'MACET'}

    total_valid = 0
    total_invalid = 0
    seen_loan_ids = set()

    with open(batch_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing_cols = required_cols - fieldnames
        if missing_cols:
            raise ValueError(f"Skema batch transaksi tidak lengkap, kolom hilang: {sorted(missing_cols)}")

        for row in reader:
            try:
                loan_id = (row.get('loan_id') or '').strip()
                status = (row.get('loan_status') or '').strip()
                prov = (row.get('province') or '').strip()
                datetime.strptime((row.get('disbursement_date') or '').strip(), '%Y-%m-%d')

                amt = float(row['loan_amount'])
                tenor = int(row['tenor_months'])
                rate = float(row['interest_rate_pct'])
                overdue = int(row['days_overdue'])
                breach = int(row['is_twp90_breach'])

                is_unique = bool(loan_id) and loan_id not in seen_loan_ids
                seen_loan_ids.add(loan_id)

                # Konsistensi status kolektibilitas POJK 10/POJK.05/2022 & TWP90 breach flag
                status_consistent = (
                    (status == 'LANCAR' and overdue == 0)
                    or (status == 'DALAM_PERHATIAN' and 1 <= overdue <= 30)
                    or (status == 'KURANG_LANCAR' and 31 <= overdue <= 60)
                    or (status == 'DIRAGUKAN' and 61 <= overdue <= 90)
                    or (status == 'MACET' and overdue > 90)
                )
                breach_consistent = breach == (1 if overdue > 90 else 0)

                if (
                    not is_unique
                    or not prov
                    or amt <= 0
                    or tenor <= 0
                    or not (0 < rate <= 36.0)
                    or status not in valid_statuses
                    or not status_consistent
                    or not breach_consistent
                ):
                    total_invalid += 1
                else:
                    total_valid += 1
            except (ValueError, TypeError):
                total_invalid += 1

    print(f"[DAG 1 - Quality Gate] Validasi Selesai: {total_valid} Valid, {total_invalid} Anomali.")
    if total_valid == 0:
        raise ValueError("Batch pinjaman kosong (0 baris valid)!")
    if total_invalid > 0:
        raise ValueError(f"Ditemukan {total_invalid} data tidak valid pada batch pinjaman!")

    context['task_instance'].xcom_push(key='batch_valid_count', value=total_valid)
    return "Batch Data 100% Valid"


with DAG(
    dag_id='ojk_ingest_and_validate_pipeline',
    default_args=default_args,
    description='[DAG 1] Pipeline Ingest dan Validasi Data OJK (Controller DAG)',
    schedule='@monthly',  # Penjadwalan otomatis periodik bulanan (Airflow 2.4+ / 2.10)
    catchup=False,
    tags=['kelompok_3', 'ojk_fintech', 'controller_dag'],
) as dag:

    start_task = EmptyOperator(
        task_id='start_ingestion'
    )

    t1_ingest_summary = PythonOperator(
        task_id='ingest_ojk_monthly_summary',
        python_callable=extract_and_load_summary,
    )

    t2_validate_batch = PythonOperator(
        task_id='validate_loan_batch_quality',
        python_callable=validate_loan_batch_quality,
    )

    # =========================================================================
    # FITUR UTAMA KELOMPOK 3: TriggerDagRunOperator
    # Memicu DAG 2 ('ojk_risk_portfolio_analytics') begitu DAG 1 sukses 100%
    # =========================================================================
    t3_trigger_analytics_dag = TriggerDagRunOperator(
        task_id='trigger_risk_portfolio_analytics',
        trigger_dag_id='ojk_risk_portfolio_analytics',
        logical_date='{{ logical_date }}',  # Sinkronisasi logical_date untuk ExternalTaskSensor
        conf={
            'triggered_by': 'ojk_ingest_and_validate_pipeline',
            'execution_status': 'SOURCE_VALIDATED',
            'timestamp': '{{ ts }}'
        },
        wait_for_completion=False,  # Asynchronous downstream execution
        reset_dag_run=True,
    )

    end_task = EmptyOperator(
        task_id='end_ingestion'
    )

    # Dependency Graph Alur Task
    start_task >> t1_ingest_summary >> t2_validate_batch >> t3_trigger_analytics_dag >> end_task

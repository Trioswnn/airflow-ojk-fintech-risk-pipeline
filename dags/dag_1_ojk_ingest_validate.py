"""
=============================================================================
PROYEK UAS PEMROGRAMAN 3 - KELOMPOK 3
Fokus: Trigger dan Automation Antar-DAG pada Apache Airflow
Dataset: Data Publikasi Resmi Fintech Lending OJK 2025

DAG 1: ojk_ingest_and_validate_pipeline
Peran: Controller Ingestion & Parallel Quality Gate DAG (End-to-End dari Raw)
Alur Task:
  1. PythonSensor (`wait_for_raw_ojk_file`): Memantau berkas mentah Excel OJK.
  2. Ekstraksi Raw -> Processed (`extract_raw_ojk_to_processed`).
  3. TaskGroup `quality_gates` (Eksekusi PARALEL):
     - `validate_monthly_summary_quality`
     - `validate_regional_exposure_quality`
     - `validate_loan_batch_quality`
  4. `prepare_trigger_payload`: Mengumpulkan metrik QC & parameter ambang batas.
  5. `TriggerDagRunOperator`: Memicu DAG 2 dengan `logical_date` tersinkron
     dan payload `conf` dinamis (termasuk `twp90_threshold`).
=============================================================================
"""

import csv
import importlib.util
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

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
RAW_EXCEL_PATH = next(
    (
        path for path in (
            os.path.join(BASE_DIR, 'data', 'raw', 'pinjaman_fintech_ojk_des2025.xlsx'),
            os.path.join(BASE_DIR, 'pinjaman_fintech_ojk_des2025.xlsx'),
            os.path.join(BASE_DIR, 'Dataset_OJK', 'pinjaman_fintech_ojk_des2025.xlsx'),
            os.path.join(BASE_DIR, '02_Dataset_Fintech_OJK', 'pinjaman_fintech_ojk_des2025.xlsx'),
        )
        if os.path.exists(path)
    ),
    os.path.join(BASE_DIR, 'data', 'raw', 'pinjaman_fintech_ojk_des2025.xlsx'),
)
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
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': on_task_failure_callback,
}


def check_raw_ojk_file_exists(**context):
    """Sensor pengecekan ketersediaan berkas Excel mentah publikasi OJK."""
    exists = os.path.isfile(RAW_EXCEL_PATH) and os.path.getsize(RAW_EXCEL_PATH) > 0
    if not exists:
        fallback_ready = all(
            os.path.isfile(os.path.join(DATA_DIR, f))
            for f in ('ojk_monthly_summary.csv', 'ojk_regional_exposure.csv', 'fintech_loan_daily_batch.csv')
        )
        print(f"[DAG 1 - Sensor] Raw Excel belum ditemukan di {RAW_EXCEL_PATH} (fallback processed={fallback_ready})")
        return fallback_ready
    print(f"[DAG 1 - Sensor] Berkas mentah OJK terdeteksi siap: {RAW_EXCEL_PATH}")
    return True


def extract_raw_ojk_to_processed(**context):
    """Menjalankan ekstraksi Excel OJK mentah -> CSV processed dengan feature engineering."""
    extractor_candidates = [
        os.path.join(BASE_DIR, 'scripts', 'extract_ojk_data.py'),
    ]
    extractor_path = next((p for p in extractor_candidates if os.path.isfile(p)), None)

    if extractor_path and os.path.isfile(RAW_EXCEL_PATH):
        spec = importlib.util.spec_from_file_location('ojk_extractor', extractor_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.extract_ojk_metrics(RAW_EXCEL_PATH, DATA_DIR)
        return "Extracted directly from raw Excel via scripts/extract_ojk_data.py"

    # Fallback aman apabila hanya folder dags/ + data/processed/ yang di-deploy standalone
    required = ('ojk_monthly_summary.csv', 'ojk_regional_exposure.csv', 'fintech_loan_daily_batch.csv')
    missing = [f for f in required if not os.path.isfile(os.path.join(DATA_DIR, f))]
    if missing:
        raise FileNotFoundError(f"Extractor/Raw tidak tersedia dan CSV berikut hilang: {missing}")
    print("[DAG 1 - Extract] Mode standalone: menggunakan CSV hasil ekstraksi terverifikasi di DATA_DIR.")
    return "Verified pre-extracted processed CSVs"


def validate_monthly_summary_quality(**context):
    """Quality Gate 1: Validasi seri waktu rasio keuangan OJK (Sheet 4)."""
    summary_path = os.path.join(DATA_DIR, 'ojk_monthly_summary.csv')
    if not os.path.exists(summary_path):
        raise FileNotFoundError(f"File {summary_path} tidak ditemukan!")

    required_cols = {
        'periode', 'tkb90_persen', 'twp90_persen',
        'roa_persen', 'roe_persen', 'risk_level', 'alert_trigger'
    }
    with open(summary_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        missing_cols = required_cols - set(reader.fieldnames or [])
        if missing_cols:
            raise ValueError(f"Kolom ringkasan OJK hilang: {sorted(missing_cols)}")
        rows = sorted(list(reader), key=lambda r: r['periode'])

    if not rows:
        raise ValueError("Dataset ringkasan bulanan OJK kosong (0 baris)!")

    for row in rows:
        tkb = float(row['tkb90_persen'])
        twp = float(row['twp90_persen'])
        if abs((tkb + twp) - 100.0) > 0.1:
            raise ValueError(f"Anomali TKB90 + TWP90 != 100% pada {row['periode']}: {tkb} + {twp}")

    latest_periode = rows[-1]['periode']
    ti = context['task_instance']
    ti.xcom_push(key='total_summary_records', value=len(rows))
    ti.xcom_push(key='latest_periode', value=latest_periode)
    print(f"[DAG 1 - QC Monthly] Lolos {len(rows)} periode (terbaru: {latest_periode}).")
    return f"Monthly QC OK ({len(rows)} rows)"


def validate_regional_exposure_quality(**context):
    """Quality Gate 2: Validasi kelengkapan 38 provinsi & rekonsiliasi total nasional (Sheet 18)."""
    regional_path = os.path.join(DATA_DIR, 'ojk_regional_exposure.csv')
    if not os.path.exists(regional_path):
        raise FileNotFoundError(f"File {regional_path} tidak ditemukan!")

    required_cols = {
        'periode', 'wilayah', 'nama_provinsi',
        'penyaluran_miliar', 'share_nasional_persen', 'risk_tier'
    }
    with open(regional_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        missing_cols = required_cols - set(reader.fieldnames or [])
        if missing_cols:
            raise ValueError(f"Kolom sebaran regional hilang: {sorted(missing_cols)}")
        rows = list(reader)

    if len(rows) != 38:
        raise ValueError(f"Jumlah provinsi tidak genap 38! Ditemukan: {len(rows)}")

    total_miliar = round(sum(float(r['penyaluran_miliar']) for r in rows), 2)
    if total_miliar <= 0:
        raise ValueError("Total penyaluran nasional tidak valid!")

    ti = context['task_instance']
    ti.xcom_push(key='province_count', value=len(rows))
    ti.xcom_push(key='total_penyaluran_miliar', value=total_miliar)
    print(f"[DAG 1 - QC Regional] Lolos {len(rows)} provinsi (Total: Rp {total_miliar:,.2f} Miliar).")
    return f"Regional QC OK ({len(rows)} provinces)"


def validate_loan_batch_quality(**context):
    """Quality Gate 3: Validasi skema, keunikan ID, rentang nilai, & konsistensi POJK 10/2022."""
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
        missing_cols = required_cols - set(reader.fieldnames or [])
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

    print(f"[DAG 1 - QC Batch] Validasi Selesai: {total_valid} Valid, {total_invalid} Anomali.")
    if total_valid == 0:
        raise ValueError("Batch pinjaman kosong (0 baris valid)!")
    if total_invalid > 0:
        raise ValueError(f"Ditemukan {total_invalid} data tidak valid pada batch pinjaman!")

    context['task_instance'].xcom_push(key='batch_valid_count', value=total_valid)
    return "Batch Data 100% Valid"


def prepare_trigger_payload(**context):
    """Menggabungkan hasil ke-3 Quality Gate paralel dan menyiapkan threshold untuk DAG 2."""
    ti = context['task_instance']
    dag_run = context.get('dag_run')
    run_conf = (dag_run.conf or {}) if dag_run else {}
    params = context.get('params') or {}

    threshold = float(run_conf.get('twp90_threshold', params.get('twp90_threshold', 4.0)))
    latest_periode = ti.xcom_pull(task_ids='quality_gates.validate_monthly_summary_quality', key='latest_periode') or '2025-12'
    province_count = ti.xcom_pull(task_ids='quality_gates.validate_regional_exposure_quality', key='province_count') or 38
    batch_valid_count = ti.xcom_pull(task_ids='quality_gates.validate_loan_batch_quality', key='batch_valid_count') or 1000

    ti.xcom_push(key='latest_periode', value=latest_periode)
    ti.xcom_push(key='twp90_threshold', value=threshold)
    ti.xcom_push(key='province_count', value=province_count)
    ti.xcom_push(key='batch_valid_count', value=batch_valid_count)

    print(
        f"[DAG 1 - Trigger Prep] QC 3 cabang lulus: periode={latest_periode}, "
        f"provinsi={province_count}, batch={batch_valid_count}, twp90_threshold={threshold}%"
    )
    return "Payload Ready"


with DAG(
    dag_id='ojk_ingest_and_validate_pipeline',
    default_args=default_args,
    description='[DAG 1] Controller End-to-End: Raw Sensor, Extract, Parallel Quality Gate & Trigger DAG 2',
    schedule='@monthly',
    catchup=False,
    params={'twp90_threshold': 4.0},
    tags=['kelompok_3', 'ojk_fintech', 'controller_dag', 'task_group'],
) as dag:

    wait_raw_sensor = PythonSensor(
        task_id='wait_for_raw_ojk_file',
        python_callable=check_raw_ojk_file_exists,
        poke_interval=10,
        timeout=60,
        mode='reschedule',
    )

    t_extract_raw = PythonOperator(
        task_id='extract_raw_ojk_to_processed',
        python_callable=extract_raw_ojk_to_processed,
    )

    # =========================================================================
    # TaskGroup: 3 Quality Gate berjalan PARALEL (Fan-Out / Fan-In)
    # =========================================================================
    with TaskGroup(group_id='quality_gates') as quality_gates:
        qc_monthly = PythonOperator(
            task_id='validate_monthly_summary_quality',
            python_callable=validate_monthly_summary_quality,
        )

        qc_regional = PythonOperator(
            task_id='validate_regional_exposure_quality',
            python_callable=validate_regional_exposure_quality,
        )

        qc_batch = PythonOperator(
            task_id='validate_loan_batch_quality',
            python_callable=validate_loan_batch_quality,
        )

    t_prepare_payload = PythonOperator(
        task_id='prepare_trigger_payload',
        python_callable=prepare_trigger_payload,
    )

    # =========================================================================
    # FITUR UTAMA KELOMPOK 3: TriggerDagRunOperator (conf dinamis & logical_date)
    # =========================================================================
    t_trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_risk_portfolio_analytics',
        trigger_dag_id='ojk_risk_portfolio_analytics',
        logical_date='{{ logical_date }}',
        conf={
            'triggered_by': 'ojk_ingest_and_validate_pipeline',
            'execution_status': 'ALL_QUALITY_GATES_PASSED',
            'latest_periode': "{{ ti.xcom_pull(task_ids='prepare_trigger_payload', key='latest_periode') }}",
            'twp90_threshold': "{{ (dag_run.conf or {}).get('twp90_threshold', params.twp90_threshold) }}",
            'validated_batch_rows': "{{ ti.xcom_pull(task_ids='prepare_trigger_payload', key='batch_valid_count') }}",
            'timestamp': '{{ ts }}',
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    end_task = EmptyOperator(
        task_id='end_ingestion'
    )

    # Dependensi Graph Fan-Out / Fan-In
    wait_raw_sensor >> t_extract_raw >> quality_gates >> t_prepare_payload >> t_trigger_dag2 >> end_task

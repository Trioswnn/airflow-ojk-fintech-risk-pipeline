"""
=============================================================================
PROYEK UAS PEMROGRAMAN 3 - KELOMPOK 3
Fokus: Trigger dan Automation Antar-DAG pada Apache Airflow
Dataset: Data Publikasi Resmi Fintech Lending OJK 2025

DAG 2: ojk_risk_portfolio_analytics
Peran: Target Worker DAG, Parallel Portfolio Analytics & Dynamic Decision Gate
Alur Task:
  1. `ExternalTaskSensor` (`wait_for_dag1_validation`): Menunggu Quality Gate DAG 1.
  2. TaskGroup `portfolio_analytics` (Eksekusi PARALEL):
     - `compute_macro_and_regional_risk`: Analisis TWP90 nasional & tier regional.
     - `compute_batch_and_age_risk`: Analisis kredit macet per provinsi & kelompok usia.
  3. `publish_portfolio_risk_mart`: Menggabungkan XCom kedua cabang ke Data Mart CSV.
  4. `BranchPythonOperator` (`evaluate_risk_threshold`): Membandingkan TWP90 vs
     `twp90_threshold` dinamis dari `dag_run.conf` DAG 1.
     - TWP90 > threshold  -> `trigger_compliance_alert` (TriggerDagRunOperator ke DAG 3)
     - TWP90 <= threshold -> `skip_compliance_alert`
=============================================================================
"""

import csv
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
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


def compute_macro_and_regional_risk(**context):
    """Cabang Analitik 1: Menghitung rasio TWP90 nasional & konsentrasi regional Risk Tier TINGGI."""
    summary_path = os.path.join(DATA_DIR, 'ojk_monthly_summary.csv')
    regional_path = os.path.join(DATA_DIR, 'ojk_regional_exposure.csv')

    with open(summary_path, 'r', encoding='utf-8') as f:
        summaries = list(csv.DictReader(f))
        if not summaries:
            raise ValueError(f"Data ringkasan OJK pada {summary_path} kosong!")
        latest_summary = sorted(summaries, key=lambda r: r['periode'])[-1]
        twp90_nasional = float(latest_summary['twp90_persen'])
        periode = latest_summary['periode']

    high_risk_regions = []
    if os.path.exists(regional_path):
        with open(regional_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if row.get('risk_tier') == 'TINGGI':
                    high_risk_regions.append(row['nama_provinsi'].split('. ', 1)[-1])

    ti = context['task_instance']
    ti.xcom_push(key='periode', value=periode)
    ti.xcom_push(key='twp90_nasional', value=twp90_nasional)
    ti.xcom_push(key='high_risk_regions', value=high_risk_regions)
    print(f"[DAG 2 - Macro/Regional] Periode={periode}, TWP90={twp90_nasional}%, Tier TINGGI={high_risk_regions}")
    return "Macro & Regional Analytics Completed"


def compute_batch_and_age_risk(**context):
    """Cabang Analitik 2: Menghitung total penyaluran, kredit macet batch, provinsi & usia terparah."""
    batch_path = os.path.join(DATA_DIR, 'fintech_loan_daily_batch.csv')

    total_loans = 0
    total_disbursed_idr = 0.0
    total_default_idr = 0.0
    macet_count = 0
    province_risk = {}
    age_risk = {}

    with open(batch_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            total_loans += 1
            amt = float(row['loan_amount'])
            total_disbursed_idr += amt
            prov = row['province']
            age = row.get('age_group', 'UNKNOWN')

            if row['loan_status'] == 'MACET' or int(row['is_twp90_breach']) == 1:
                macet_count += 1
                total_default_idr += amt
                province_risk[prov] = province_risk.get(prov, 0.0) + amt
                age_risk[age] = age_risk.get(age, 0.0) + amt

    batch_twp90_pct = round((macet_count / total_loans) * 100, 2) if total_loans > 0 else 0.0
    top_risk_province = max(province_risk, key=province_risk.get) if province_risk else "N/A"
    top_risk_amount = province_risk.get(top_risk_province, 0.0)
    top_risk_age = max(age_risk, key=age_risk.get) if age_risk else "N/A"
    top_age_amount = age_risk.get(top_risk_age, 0.0)

    ti = context['task_instance']
    ti.xcom_push(key='batch_total_loans', value=total_loans)
    ti.xcom_push(key='batch_disbursed_idr', value=total_disbursed_idr)
    ti.xcom_push(key='batch_default_idr', value=total_default_idr)
    ti.xcom_push(key='batch_twp90_pct', value=batch_twp90_pct)
    ti.xcom_push(key='top_risk_province', value=top_risk_province)
    ti.xcom_push(key='top_risk_amount', value=top_risk_amount)
    ti.xcom_push(key='top_risk_age_group', value=top_risk_age)
    ti.xcom_push(key='top_age_amount', value=top_age_amount)
    print(f"[DAG 2 - Batch/Age] Loans={total_loans}, TopProv={top_risk_province}, TopAge={top_risk_age}")
    return "Batch & Age Analytics Completed"


def publish_portfolio_risk_mart(**context):
    """Menggabungkan hasil kedua cabang analitik paralel ke Data Mart CSV resmi."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ti = context['task_instance']
    dag_run = context.get('dag_run')
    run_conf = (dag_run.conf or {}) if dag_run else {}
    params = context.get('params') or {}

    threshold = float(run_conf.get('twp90_threshold', params.get('twp90_threshold', 4.0)))
    periode = ti.xcom_pull(task_ids='portfolio_analytics.compute_macro_and_regional_risk', key='periode') or '2025-12'
    twp90_nasional = float(ti.xcom_pull(task_ids='portfolio_analytics.compute_macro_and_regional_risk', key='twp90_nasional') or 4.32)
    high_risk_regions = ti.xcom_pull(task_ids='portfolio_analytics.compute_macro_and_regional_risk', key='high_risk_regions') or []

    total_loans = int(ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='batch_total_loans') or 0)
    total_disbursed_idr = float(ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='batch_disbursed_idr') or 0.0)
    total_default_idr = float(ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='batch_default_idr') or 0.0)
    batch_twp90_pct = float(ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='batch_twp90_pct') or 0.0)
    top_risk_province = ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='top_risk_province') or 'N/A'
    top_risk_amount = float(ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='top_risk_amount') or 0.0)
    top_risk_age = ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='top_risk_age_group') or 'N/A'
    top_age_amount = float(ti.xcom_pull(task_ids='portfolio_analytics.compute_batch_and_age_risk', key='top_age_amount') or 0.0)

    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    with open(mart_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'value', 'keterangan'])
        writer.writerow(['periode', periode, 'Periode Data OJK'])
        writer.writerow(['twp90_nasional_pct', twp90_nasional, 'Rasio Kredit Macet Nasional OJK'])
        writer.writerow(['twp90_threshold_pct', threshold, 'Ambang Batas Toleransi Regulasi Dinamis'])
        writer.writerow(['batch_total_loans', total_loans, 'Jumlah Akun Pinjaman Terdaftar'])
        writer.writerow(['batch_disbursed_idr', total_disbursed_idr, 'Total Penyaluran Pinjaman (IDR)'])
        writer.writerow(['batch_default_idr', total_default_idr, 'Total Portofolio Macet (IDR)'])
        writer.writerow(['batch_twp90_pct', batch_twp90_pct, 'Tingkat Wanprestasi 90 Hari Batch'])
        writer.writerow(['highest_risk_province', top_risk_province, f'Provinsi Kredit Macet Tertinggi: Rp {top_risk_amount:,.0f}'])
        writer.writerow([
            'highest_risk_age_group',
            top_risk_age,
            f'Kelompok Usia Macet Tertinggi: Rp {top_age_amount:,.0f} | Tier TINGGI: {", ".join(high_risk_regions)}'
        ])

    ti.xcom_push(key='twp90_nasional', value=twp90_nasional)
    ti.xcom_push(key='twp90_threshold', value=threshold)
    ti.xcom_push(key='periode', value=periode)
    ti.xcom_push(key='top_risk_province', value=top_risk_province)
    ti.xcom_push(key='top_risk_age_group', value=top_risk_age)
    ti.xcom_push(key='total_default_idr', value=total_default_idr)
    print(f"[DAG 2 - Data Mart] Terbit -> {mart_path} (TWP90={twp90_nasional}% vs Threshold={threshold}%)")
    return "Data Mart Published"


def evaluate_risk_threshold(**context):
    """
    Logika Pengkondisian Otomasi Dinamis (Branching):
    Membandingkan TWP90 nasional terhadap ambang batas dari `dag_run.conf` DAG 1.
    """
    ti = context['task_instance']
    dag_run = context.get('dag_run')
    run_conf = (dag_run.conf or {}) if dag_run else {}
    params = context.get('params') or {}

    twp90 = ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='twp90_nasional')
    threshold = ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='twp90_threshold')
    if threshold is None:
        threshold = float(run_conf.get('twp90_threshold', params.get('twp90_threshold', 4.0)))

    if twp90 is None:
        mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
        if os.path.exists(mart_path):
            with open(mart_path, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    if row.get('metric') == 'twp90_nasional_pct':
                        twp90 = float(row['value'])
                        break
        if twp90 is None:
            print("[WARNING] XCom twp90_nasional tidak ditemukan, menggunakan fallback 4.32%")
            twp90 = 4.32

    print(f"[DAG 2 - Decision Gate] Evaluasi Dinamis: TWP90 ({float(twp90):.2f}%) vs Threshold ({float(threshold):.2f}%)")

    if float(twp90) > float(threshold):
        print("[ALERT TRIGGERED] TWP90 melampaui batas toleransi! Mengarahkan ke 'trigger_compliance_alert'...")
        return 'trigger_compliance_alert'
    else:
        print("[NORMAL] TWP90 aman di bawah batas toleransi. Mengarahkan ke 'skip_compliance_alert'.")
        return 'skip_compliance_alert'


with DAG(
    dag_id='ojk_risk_portfolio_analytics',
    default_args=default_args,
    description='[DAG 2] Target Worker: Sensor DAG 1, Parallel Analytics TaskGroup & Dynamic Branching',
    schedule=None,
    catchup=False,
    params={'twp90_threshold': 4.0},
    tags=['kelompok_3', 'ojk_fintech', 'worker_dag', 'branching', 'task_group'],
) as dag:

    # =========================================================================
    # FITUR KELOMPOK 3: ExternalTaskSensor
    # Memastikan seluruh Quality Gate DAG 1 ('prepare_trigger_payload') sukses
    # =========================================================================
    sensor_wait_for_dag1 = ExternalTaskSensor(
        task_id='wait_for_dag1_validation',
        external_dag_id='ojk_ingest_and_validate_pipeline',
        external_task_id='prepare_trigger_payload',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        check_existence=True,
        mode='reschedule',
        poke_interval=15,
        timeout=180
    )

    # =========================================================================
    # TaskGroup: 2 Cabang Analitik Risiko berjalan PARALEL
    # =========================================================================
    with TaskGroup(group_id='portfolio_analytics') as portfolio_analytics:
        t_macro_regional = PythonOperator(
            task_id='compute_macro_and_regional_risk',
            python_callable=compute_macro_and_regional_risk,
        )

        t_batch_age = PythonOperator(
            task_id='compute_batch_and_age_risk',
            python_callable=compute_batch_and_age_risk,
        )

    t_publish_mart = PythonOperator(
        task_id='publish_portfolio_risk_mart',
        python_callable=publish_portfolio_risk_mart,
    )

    # =========================================================================
    # FITUR KELOMPOK 3: BranchPythonOperator (Dynamic Threshold Automation)
    # =========================================================================
    t_branch_decision = BranchPythonOperator(
        task_id='evaluate_risk_threshold',
        python_callable=evaluate_risk_threshold,
    )

    # =========================================================================
    # FITUR KELOMPOK 3: Conditional Trigger ke DAG 3
    # =========================================================================
    t_trigger_alert_dag = TriggerDagRunOperator(
        task_id='trigger_compliance_alert',
        trigger_dag_id='ojk_compliance_alert_automation',
        logical_date='{{ logical_date }}',
        conf={
            'alert_level': 'CRITICAL_TWP90_BREACH',
            'regulatory_body': 'OJK_IKNB',
            'action_required': 'RECOVERY_AND_RISK_AUDIT',
            'periode': "{{ ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='periode') }}",
            'twp90_nasional': "{{ ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='twp90_nasional') }}",
            'twp90_threshold': "{{ ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='twp90_threshold') }}",
            'top_risk_province': "{{ ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='top_risk_province') }}",
            'top_risk_age_group': "{{ ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='top_risk_age_group') }}",
            'total_default_idr': "{{ ti.xcom_pull(task_ids='publish_portfolio_risk_mart', key='total_default_idr') }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_skip_alert = EmptyOperator(
        task_id='skip_compliance_alert'
    )

    end_task = EmptyOperator(
        task_id='end_risk_analytics',
        trigger_rule='none_failed_min_one_success'
    )

    # Dependensi Graph Fan-Out / Fan-In + Branching
    sensor_wait_for_dag1 >> portfolio_analytics >> t_publish_mart >> t_branch_decision
    t_branch_decision >> t_trigger_alert_dag >> end_task
    t_branch_decision >> t_skip_alert >> end_task

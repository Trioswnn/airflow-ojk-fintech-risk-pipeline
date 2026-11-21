"""
=============================================================================
PROYEK UAS PEMROGRAMAN 3 - KELOMPOK 3
Fokus: Trigger dan Automation Antar-DAG pada Apache Airflow
Dataset: Data Publikasi Resmi Fintech Lending OJK 2025

DAG 2: ojk_risk_portfolio_analytics
Peran: Target Worker DAG & Decision Automator
Deskripsi:
  1. Memanfaatkan `ExternalTaskSensor` untuk memastikan validasi di DAG 1 sukses.
  2. Mengagregasi portofolio risiko kredit macet (TWP90), regional, dan usia peminjam.
  3. Menyimpan hasil olahan analitik ke Data Mart ('data/output/ojk_portfolio_risk_mart.csv').
  4. Memanfaatkan `BranchPythonOperator` untuk mengevaluasi ambang batas risiko OJK:
     - Jika TWP90 > 4.0%: Otomatis memicu DAG 3 via `TriggerDagRunOperator`.
     - Jika TWP90 <= 4.0%: Melewati alert (skip).
=============================================================================
"""

import os
import csv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

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

default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def aggregate_portfolio_risk(**context):
    """Menghitung agregasi risiko portofolio kredit macet nasional, regional, dan kelompok usia."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    batch_path = os.path.join(DATA_DIR, 'fintech_loan_daily_batch.csv')
    summary_path = os.path.join(DATA_DIR, 'ojk_monthly_summary.csv')
    regional_path = os.path.join(DATA_DIR, 'ojk_regional_exposure.csv')

    # 1. Baca data ringkasan nasional terbaru (urutkan berdasarkan periode agar presisi)
    with open(summary_path, 'r', encoding='utf-8') as f:
        summaries = list(csv.DictReader(f))
        if not summaries:
            raise ValueError(f"Data ringkasan OJK pada {summary_path} kosong!")
        latest_summary = sorted(summaries, key=lambda r: r['periode'])[-1]
        twp90_nasional = float(latest_summary['twp90_persen'])

    # 2. Baca eksposur regional resmi OJK (38 Provinsi) jika tersedia
    high_risk_regions = []
    if os.path.exists(regional_path):
        with open(regional_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if row.get('risk_tier') == 'TINGGI':
                    high_risk_regions.append(row['nama_provinsi'].split('. ', 1)[-1])

    # 3. Agregasi batch pinjaman (wilayah & kelompok usia peminjam)
    total_loans = 0
    total_disbursed_idr = 0.0
    total_default_idr = 0.0
    macet_count = 0
    province_risk = {}
    age_risk = {}

    with open(batch_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
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

    # 4. Simpan ke Data Mart (7 metrik inti standar pipeline)
    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    with open(mart_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'value', 'keterangan'])
        writer.writerow(['periode', latest_summary['periode'], 'Periode Data OJK'])
        writer.writerow(['twp90_nasional_pct', twp90_nasional, 'Rasio Kredit Macet Nasional OJK'])
        writer.writerow(['batch_total_loans', total_loans, 'Jumlah Akun Pinjaman Terdaftar'])
        writer.writerow(['batch_disbursed_idr', total_disbursed_idr, 'Total Penyaluran Pinjaman (IDR)'])
        writer.writerow(['batch_default_idr', total_default_idr, 'Total Portofolio Macet (IDR)'])
        writer.writerow(['batch_twp90_pct', batch_twp90_pct, 'Tingkat Wanprestasi 90 Hari Batch'])
        writer.writerow(['highest_risk_province', top_risk_province, f'Provinsi Kredit Macet Tertinggi: Rp {top_risk_amount:,.0f}'])

    print(f"[DAG 2 - Analytics] Sukses menulis Data Mart -> {mart_path}")
    print(f"[DAG 2 - Metrik] TWP90 Nasional: {twp90_nasional}%, Batch TWP90: {batch_twp90_pct}%")
    print(f"[DAG 2 - Segmen] Tier TINGGI OJK: {', '.join(high_risk_regions) or 'N/A'} | Macet Terbesar per Usia: {top_risk_age}")

    # Simpan metrik ke XCom untuk evaluasi branching dan pelaporan downstream
    ti = context['task_instance']
    ti.xcom_push(key='twp90_nasional', value=twp90_nasional)
    ti.xcom_push(key='top_risk_province', value=top_risk_province)
    ti.xcom_push(key='total_default_idr', value=total_default_idr)
    ti.xcom_push(key='top_risk_age_group', value=top_risk_age)
    ti.xcom_push(key='high_risk_regions', value=high_risk_regions)
    return "Analytics Completed"


def evaluate_risk_threshold(**context):
    """
    Logika Pengkondisian Otomasi (Branching):
    Mengecek apakah tingkat kredit macet melampaui batas toleransi OJK (Threshold > 4.0%).
    """
    ti = context['task_instance']
    twp90 = ti.xcom_pull(task_ids='aggregate_portfolio_risk', key='twp90_nasional')

    if twp90 is None:
        # Fallback terukur membaca langsung dari Data Mart jika dijalankan via standalone task test
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

    print(f"[DAG 2 - Decision Gate] Memeriksa Ambang Batas TWP90 OJK: {twp90}% vs Batas Regulasi (4.00%)")

    if float(twp90) > 4.0:
        print("[ALERT TRIGGERED] TWP90 melampaui batas aman OJK! Mengalihkan ke trigger_compliance_alert...")
        return 'trigger_compliance_alert'
    else:
        print("[NORMAL] TWP90 berada dalam batas wajar. Melewati alert.")
        return 'skip_compliance_alert'


with DAG(
    dag_id='ojk_risk_portfolio_analytics',
    default_args=default_args,
    description='[DAG 2] Analisis Risiko Portofolio & Decision Gate (Target Worker DAG)',
    schedule=None,  # Dipicu via TriggerDagRunOperator dari DAG 1
    catchup=False,
    tags=['kelompok_3', 'ojk_fintech', 'worker_dag', 'branching'],
) as dag:

    # =========================================================================
    # FITUR KELOMPOK 3: ExternalTaskSensor
    # Memantau dan memastikan task 'validate_loan_batch_quality' pada DAG 1 sukses
    # =========================================================================
    sensor_wait_for_dag1 = ExternalTaskSensor(
        task_id='wait_for_dag1_validation',
        external_dag_id='ojk_ingest_and_validate_pipeline',
        external_task_id='validate_loan_batch_quality',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        check_existence=True,
        mode='reschedule',
        poke_interval=15,
        timeout=180
    )

    t1_process_risk = PythonOperator(
        task_id='aggregate_portfolio_risk',
        python_callable=aggregate_portfolio_risk,
    )

    # =========================================================================
    # FITUR KELOMPOK 3: BranchPythonOperator (Conditional Automation)
    # Menentukan apakah kondisi data memerlukan eskalasi peringatan darurat
    # =========================================================================
    t2_branch_decision = BranchPythonOperator(
        task_id='evaluate_risk_threshold',
        python_callable=evaluate_risk_threshold,
    )

    # =========================================================================
    # FITUR KELOMPOK 3: Conditional Trigger ke DAG 3
    # Hanya dijalankan jika hasil branch menyatakan kondisi TWP90 > 4.0%
    # =========================================================================
    t3_trigger_alert_dag = TriggerDagRunOperator(
        task_id='trigger_compliance_alert',
        trigger_dag_id='ojk_compliance_alert_automation',
        logical_date='{{ logical_date }}',  # Menjaga jejak logical_date yang sama dengan DAG 1 & 2
        conf={
            'alert_level': 'CRITICAL_TWP90_BREACH',
            'regulatory_body': 'OJK_IKNB',
            'action_required': 'RECOVERY_AND_RISK_AUDIT'
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t3_skip_alert = EmptyOperator(
        task_id='skip_compliance_alert'
    )

    end_task = EmptyOperator(
        task_id='end_risk_analytics',
        trigger_rule='none_failed_min_one_success'  # Sinkronisasi hasil branch
    )

    # Dependency Graph Alur Task
    sensor_wait_for_dag1 >> t1_process_risk >> t2_branch_decision
    t2_branch_decision >> t3_trigger_alert_dag >> end_task
    t2_branch_decision >> t3_skip_alert >> end_task

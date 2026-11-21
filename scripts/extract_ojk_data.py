"""
Extractor Data Statistik Fintech Lending OJK Desember 2025
Menggunakan modul bawaan Python (zipfile & xml.etree.ElementTree)
tanpa dependensi eksternal (zero dependency).
Mencakup LENGKAP 38 Provinsi di Indonesia (6 Jawa + 32 Luar Jawa termasuk 4 DOB Papua)
dan Generator Batch Transaksi Realistis berbobot OJK.
"""

import os
import zipfile
import xml.etree.ElementTree as ET
import datetime
import csv
import random

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _parse_shared_strings(z):
    """Membaca tabel sharedStrings.xml per elemen <si> agar indeks string selalu akurat."""
    shared_strings = []
    if 'xl/sharedStrings.xml' not in z.namelist():
        return shared_strings
    tree = ET.fromstring(z.read('xl/sharedStrings.xml'))
    for si in tree.iter():
        if si.tag.endswith('si'):
            parts = [t.text for t in si.iter() if t.tag.endswith('t') and t.text]
            shared_strings.append(''.join(parts))
    return shared_strings


def _parse_sheet_rows(ws_xml, shared_strings):
    """Memetakan isi sel worksheet XML menjadi kamus {row_idx: {col_letters: value}}."""
    rows = {}
    ns_v = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v'
    for c in ws_xml.iter():
        if c.tag.endswith('c'):
            r_ref = c.attrib.get('r', '')
            if not r_ref:
                continue
            r_idx = int(''.join(filter(str.isdigit, r_ref)))
            c_ref = ''.join(filter(str.isalpha, r_ref))
            t = c.attrib.get('t')
            v = c.find(ns_v)
            val = ''
            if v is not None and v.text is not None:
                val = shared_strings[int(v.text)] if t == 's' else v.text
            rows.setdefault(r_idx, {})[c_ref] = val
    return rows


def extract_ojk_metrics(excel_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"File {excel_path} tidak ditemukan!")

    with zipfile.ZipFile(excel_path, 'r') as z:
        # 1. Baca Shared Strings
        shared_strings = _parse_shared_strings(z)

        # 2. Relasi sheet dengan penanganan path target yang aman
        wb = ET.fromstring(z.read('xl/workbook.xml'))
        rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
        rel_map = {r.attrib['Id']: r.attrib['Target'] for r in rels.iter() if r.tag.endswith('Relationship')}

        sheet_targets = {}
        rel_attr = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'
        for s in wb.iter():
            if s.tag.endswith('sheet'):
                name = s.attrib['name'].strip()
                r_id = s.attrib[rel_attr]
                target = rel_map[r_id].lstrip('/')
                if not target.startswith('xl/'):
                    target = 'xl/' + target
                sheet_targets[name] = target

        # 3. Ekstrak Sheet 4 (Rasio Keuangan & TWP90)
        ws4 = ET.fromstring(z.read(sheet_targets['4']))
        rows4 = _parse_sheet_rows(ws4, shared_strings)

        # Urutkan kolom berdasarkan panjang huruf lalu alfabet ('C'..'Z' sebelum 'AA'..)
        date_cols = [
            c for c in sorted(rows4[2].keys(), key=lambda col: (len(col), col))
            if rows4[2][c].isdigit()
        ]

        monthly_summary = []
        for c in date_cols:
            serial = int(rows4[2][c])
            dt = datetime.date(1899, 12, 30) + datetime.timedelta(days=serial)
            periode = dt.strftime('%Y-%m')

            tkb90 = float(rows4[3].get(c, 0)) * 100
            twp90 = float(rows4[4].get(c, 0)) * 100
            roa = float(rows4[5].get(c, 0)) * 100 if c in rows4.get(5, {}) and rows4[5][c] else 0.0
            roe = float(rows4[6].get(c, 0)) * 100 if c in rows4.get(6, {}) and rows4[6][c] else 0.0

            risk_level = "WASPADA" if twp90 > 4.0 else ("PERHATIAN" if twp90 > 3.0 else "NORMAL")
            alert_trigger = 1 if twp90 > 4.0 else 0

            monthly_summary.append({
                'periode': periode,
                'tkb90_persen': round(tkb90, 2),
                'twp90_persen': round(twp90, 2),
                'roa_persen': round(roa, 2),
                'roe_persen': round(roe, 2),
                'risk_level': risk_level,
                'alert_trigger': alert_trigger
            })

        csv1_path = os.path.join(output_dir, 'ojk_monthly_summary.csv')
        with open(csv1_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['periode', 'tkb90_persen', 'twp90_persen', 'roa_persen', 'roe_persen', 'risk_level', 'alert_trigger'])
            writer.writeheader()
            writer.writerows(monthly_summary)
        print(f"[OK] Berhasil mengekstrak {len(monthly_summary)} baris -> {csv1_path}")

        # 4. Ekstrak Sheet 18 (Penyaluran Pinjaman per Lokasi / 38 Provinsi)
        ws18 = ET.fromstring(z.read(sheet_targets['18']))
        rows18 = _parse_sheet_rows(ws18, shared_strings)

        # Total Nasional Baris 44 Kolom O = 1.388.837,37 Miliar Rp
        total_nasional = float(rows18.get(44, {}).get('O', 1388837.37))

        # 38 Provinsi Lengkap (6 Jawa + 32 Luar Jawa termasuk 4 Provinsi DOB Papua)
        province_map = [
            # Jawa (Row 5 s.d. 10)
            (5, "Jawa", "1. Banten"),
            (6, "Jawa", "2. DKI Jakarta"),
            (7, "Jawa", "3. Jawa Barat"),
            (8, "Jawa", "4. Jawa Tengah"),
            (9, "Jawa", "5. DI Yogyakarta"),
            (10, "Jawa", "6. Jawa Timur"),
            # Luar Jawa (Row 12 s.d. 43)
            (12, "Luar Jawa", "1. Nangroe Aceh Darussalam"),
            (13, "Luar Jawa", "2. Sumatera Utara"),
            (14, "Luar Jawa", "3. Sumatera Barat"),
            (15, "Luar Jawa", "4. Riau"),
            (16, "Luar Jawa", "5. Kepulauan Riau"),
            (17, "Luar Jawa", "6. Kepualauan Bangka Belitung"),
            (18, "Luar Jawa", "7. Jambi"),
            (19, "Luar Jawa", "8. Sumatera Selatan"),
            (20, "Luar Jawa", "9. Bengkulu"),
            (21, "Luar Jawa", "10. Lampung"),
            (22, "Luar Jawa", "11. Kalimantan Barat"),
            (23, "Luar Jawa", "12. Kalimantan Tengah"),
            (24, "Luar Jawa", "13. Kalimantan Utara"),
            (25, "Luar Jawa", "14. Kalimantan Timur"),
            (26, "Luar Jawa", "15. Kalimantan Selatan"),
            (27, "Luar Jawa", "16. Sulawesi Utara"),
            (28, "Luar Jawa", "17. Gorontalo"),
            (29, "Luar Jawa", "18. Sulawesi Tengah"),
            (30, "Luar Jawa", "19. Sulawesi Barat"),
            (31, "Luar Jawa", "20. Sulawesi Selatan"),
            (32, "Luar Jawa", "21. Sulawesi Tenggara"),
            (33, "Luar Jawa", "22. Bali"),
            (34, "Luar Jawa", "23. Nusa Tenggara Barat"),
            (35, "Luar Jawa", "24. Nusa Tenggara Timur"),
            (36, "Luar Jawa", "25. Maluku Utara"),
            (37, "Luar Jawa", "26. Maluku"),
            (38, "Luar Jawa", "27. Papua Barat"),
            (39, "Luar Jawa", "28. Papua"),
            (40, "Luar Jawa", "29. Papua Tengah"),
            (41, "Luar Jawa", "30. Papua Pegunungan"),
            (42, "Luar Jawa", "31. Papua Selatan"),
            (43, "Luar Jawa", "32. Papua Barat Daya"),
        ]

        regional_rows = []
        clean_province_names = []
        province_weights = []

        for r_idx, region, prov_name in province_map:
            val_o = rows18.get(r_idx, {}).get('O', '')
            if not val_o:
                continue
            penyaluran = float(val_o)
            share = round((penyaluran / total_nasional) * 100, 2)
            tier = "TINGGI" if share > 10.0 else ("SEDANG" if share > 2.0 else "RENDAH")

            regional_rows.append({
                'periode': '2025-12',
                'wilayah': region,
                'nama_provinsi': prov_name,
                'penyaluran_miliar': round(penyaluran, 2),
                'share_nasional_persen': share,
                'risk_tier': tier
            })

            clean_name = prov_name.split('. ', 1)[-1]
            clean_province_names.append(clean_name)
            province_weights.append(share)

        csv2_path = os.path.join(output_dir, 'ojk_regional_exposure.csv')
        with open(csv2_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['periode', 'wilayah', 'nama_provinsi', 'penyaluran_miliar', 'share_nasional_persen', 'risk_tier'])
            writer.writeheader()
            writer.writerows(regional_rows)
        print(f"[OK] Berhasil mengekstrak {len(regional_rows)} provinsi lengkap -> {csv2_path}")

        # 5. Buat CSV 3: Batch Transaksi Peminjam (1.000 data)
        snapshot_date = datetime.date(2025, 12, 30)
        random.seed(42)
        transactions = []
        age_groups = ['<19', '19-34', '35-54', '>54']
        age_weights = [0.05, 0.60, 0.30, 0.05]
        genders = ['L', 'P']
        statuses = ['LANCAR', 'DALAM_PERHATIAN', 'KURANG_LANCAR', 'DIRAGUKAN', 'MACET']
        status_weights = [0.85, 0.08, 0.03, 0.02, 0.02]

        for i in range(1, 1001):
            status = random.choices(statuses, weights=status_weights)[0]
            tenor = random.choice([1, 3, 6, 12])

            # Hitung hari keterlambatan dan tanggal pencairan yang konsisten logis
            if status == 'LANCAR':
                overdue = 0
                disburse_days_ago = random.randint(10, 90)
            elif status == 'DALAM_PERHATIAN':
                overdue = random.randint(1, 30)
                disburse_days_ago = (tenor * 30) + overdue
            elif status == 'KURANG_LANCAR':
                overdue = random.randint(31, 60)
                disburse_days_ago = (tenor * 30) + overdue
            elif status == 'DIRAGUKAN':
                overdue = random.randint(61, 90)
                disburse_days_ago = (tenor * 30) + overdue
            else:  # MACET
                overdue = random.randint(91, 180)
                disburse_days_ago = (tenor * 30) + overdue

            disburse_dt = snapshot_date - datetime.timedelta(days=disburse_days_ago)
            prov = random.choices(clean_province_names, weights=province_weights, k=1)[0]

            transactions.append({
                'loan_id': f"LN202512{i:04d}",
                'borrower_id': f"BRW-{random.randint(10000, 99999)}",
                'disbursement_date': disburse_dt.strftime('%Y-%m-%d'),
                'age_group': random.choices(age_groups, weights=age_weights)[0],
                'gender': random.choice(genders),
                'province': prov,
                'loan_amount': random.choice([1500000, 2500000, 3000000, 5000000, 8000000, 10000000]),
                'tenor_months': tenor,
                'interest_rate_pct': round(random.uniform(7.5, 12.0), 1),
                'loan_status': status,
                'days_overdue': overdue,
                'is_twp90_breach': 1 if overdue > 90 else 0
            })

        csv3_path = os.path.join(output_dir, 'fintech_loan_daily_batch.csv')
        with open(csv3_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'loan_id', 'borrower_id', 'disbursement_date', 'age_group', 'gender',
                'province', 'loan_amount', 'tenor_months', 'interest_rate_pct',
                'loan_status', 'days_overdue', 'is_twp90_breach'
            ])
            writer.writeheader()
            writer.writerows(transactions)
        print(f"[OK] Berhasil membuat batch transaksi 100% realistis & logis -> {csv3_path}")


if __name__ == '__main__':
    raw_candidates = [
        os.path.join(BASE_DIR, 'data', 'raw', 'pinjaman_fintech_ojk_des2025.xlsx'),
        os.path.join(BASE_DIR, 'pinjaman_fintech_ojk_des2025.xlsx'),
    ]
    excel_file = next((p for p in raw_candidates if os.path.exists(p)), raw_candidates[0])
    target_dir = os.path.join(BASE_DIR, 'data', 'processed')
    extract_ojk_metrics(excel_file, target_dir)

import os
import re
import argparse
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'raw_data')
HEALTH_DIR = os.path.join(RAW_DIR, 'kesehatan')
SECURITY_DIR = os.path.join(RAW_DIR, 'keamanan')
STATIC_DIR = os.path.join(RAW_DIR, 'static')


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def download_to_path(url: str, out_path: str) -> None:
    """Download file dari direct URL ke path lokal."""
    ensure_dir(os.path.dirname(out_path))
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def load_path_to_standard_csv(input_source: str, out_static_csv: str, dataset_type: str = 'health') -> str:
    """Load file lokal atau URL, standarisasi untuk dataset yang sesuai, dan simpan ke CSV di raw_data."""
    ensure_dir(os.path.dirname(out_static_csv))

    df, source_path = load_source_to_df(input_source)

    if dataset_type == 'health':
        df = standardize_health_facilities(df, source_path=source_path)
    elif dataset_type == 'generic':
        df = standardize_kecamatan_year(df)
    else:
        raise ValueError(f"Unknown dataset_type: {dataset_type}")

    df.to_csv(out_static_csv, index=False)
    return out_static_csv


def safe_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    for cand in candidates:
        for lc, orig in cols.items():
            if cand.lower() in lc:
                return orig
    return None


def read_table_by_extension(path: str) -> pd.DataFrame:
    low = path.lower()
    if low.endswith('.csv'):
        return pd.read_csv(path, header=0, dtype=str)
    if low.endswith('.json'):
        return pd.read_json(path)
    if low.endswith('.xlsx') or low.endswith('.xls'):
        return pd.read_excel(path, dtype=str)
    if low.endswith('.parquet'):
        return pd.read_parquet(path)
    return pd.read_csv(path, header=0, dtype=str)


def infer_year_from_source(source_path: Optional[str]) -> int:
    if source_path is None:
        return 2024
    m = re.search(r'(20\d{2}|19\d{2})', os.path.basename(source_path))
    if m:
        return int(m.group(1))
    return 2024


def load_source_to_df(input_source: str) -> tuple[pd.DataFrame, str]:
    if input_source.lower().startswith(('http://', 'https://')):
        tmp_dir = os.path.join(RAW_DIR, '_tmp_download')
        ensure_dir(tmp_dir)
        ext = 'bin'
        m = re.search(r'\.(csv|xlsx|xls|json|parquet)(\?|$)', input_source, flags=re.IGNORECASE)
        if m:
            ext = m.group(1).lower()
        tmp_path = os.path.join(tmp_dir, f'direct.{ext}')
        download_to_path(input_source, tmp_path)
        return read_table_by_extension(tmp_path), tmp_path
    if not os.path.exists(input_source):
        raise FileNotFoundError(f"Input source tidak ditemukan: {input_source}")
    return read_table_by_extension(input_source), input_source


def standardize_health_facilities(df: pd.DataFrame, source_path: Optional[str] = None) -> pd.DataFrame:
    id_col = safe_col(df, ['_id', 'id'])
    kec_col = safe_col(df, ['kecamatan', 'Kecamatan', 'kec', 'district', 'nama_kecamatan'])
    jenis_col = safe_col(df, ['Jenis Faskes', 'jenis_faskes', 'jenis faskes', 'type', 'facility_type'])
    penyelenggara_col = safe_col(df, ['Penyelenggara Faskes', 'penyelenggara_faskes', 'penyelenggara faskes', 'provider', 'penyelenggara'])
    nama_col = safe_col(df, ['Nama Faskes', 'nama_faskes', 'nama faskes', 'name'])

    if kec_col is None:
        raise ValueError(
            f"Tidak menemukan kolom kecamatan pada dataset kesehatan. Columns={list(df.columns)}"
        )

    rename_map = {}
    if id_col:
        rename_map[id_col] = '_id'
    rename_map[kec_col] = 'kecamatan'
    if jenis_col:
        rename_map[jenis_col] = 'jenis_faskes'
    if penyelenggara_col:
        rename_map[penyelenggara_col] = 'penyelenggara_faskes'
    if nama_col:
        rename_map[nama_col] = 'nama_faskes'

    out = df.rename(columns=rename_map).copy()
    out['kecamatan'] = out['kecamatan'].astype(str).str.strip()
    out['tahun'] = infer_year_from_source(source_path)

    keep_cols = [c for c in ['_id', 'kecamatan', 'jenis_faskes', 'penyelenggara_faskes', 'nama_faskes', 'tahun'] if c in out.columns]
    out = out[keep_cols]
    return out


def clean_numeric_string(value: Any) -> Optional[float]:
    if pd.isna(value):
        return None
    text = str(value).strip().replace('"', '').replace("'", '').replace('.', '').replace(',', '')
    if text == '':
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_security_bps_table(path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(path, header=None, dtype=str, skip_blank_lines=False)
    raw = raw.fillna('').astype(str).apply(lambda col: col.str.replace('\r', ' ', regex=False)
                                                          .str.replace('\n', ' ', regex=False)
                                                          .str.strip())

    section = None
    records = {'regency': [], 'municipality': []}
    for _, row in raw.iterrows():
        first_cell = row.iloc[0].strip()
        if not first_cell:
            continue
        lower = first_cell.lower()
        if 'kabupaten/kota' in lower or ('kabupaten' in lower and 'regency' in lower):
            section = 'regency'
            continue
        if 'kota/municipality' in lower:
            section = 'municipality'
            continue
        if lower.startswith('catatan') or lower.startswith('sumber') or 'jawa timur' in lower:
            continue
        if section is None:
            continue
        if first_cell in ['Kabupaten/Regency', 'Kota/Municipality', 'Kabupaten/Kota', 'Kabupaten/Kota\nRegency/Municipality']:
            continue

        area = first_cell
        values = [row.iloc[i] if i < len(row) else '' for i in range(6)]
        for year, value in [('2019', values[2]), ('2020', values[3]), ('2021', values[4]), ('2022', values[5])]:
            amount = clean_numeric_string(value)
            if amount is not None:
                records[section].append({
                    'kecamatan': area,
                    'tahun': int(year),
                    'jumlah_kejahatan': amount,
                })

    regency = pd.DataFrame(records['regency'])
    municipality = pd.DataFrame(records['municipality'])
    return regency, municipality


def save_security_dataset(input_source: str, out_regency_csv: str, out_municipality_csv: str, out_combined_csv: str) -> str:
    _, source_path = load_source_to_df(input_source)
    ensure_dir(os.path.dirname(out_regency_csv))
    ensure_dir(os.path.dirname(out_municipality_csv))
    ensure_dir(os.path.dirname(out_combined_csv))

    regency_df, municipality_df = parse_security_bps_table(source_path)
    regency_df.to_csv(out_regency_csv, index=False)
    municipality_df.to_csv(out_municipality_csv, index=False)

    combined = pd.concat([regency_df, municipality_df], ignore_index=True)
    combined.to_csv(out_combined_csv, index=False)
    return out_combined_csv


def standardize_kecamatan_year(df: pd.DataFrame) -> pd.DataFrame:
    kec_col = safe_col(df, ['kecamatan', 'nama_kecamatan', 'kec', 'district', 'kecamatan_name'])
    year_col = safe_col(df, ['tahun', 'year', 'tah'])

    if kec_col is None or year_col is None:
        raise ValueError(
            f"Cannot infer kecamatan/year columns. kec_col={kec_col}, year_col={year_col}. Columns={list(df.columns)}"
        )

    out = df.copy()
    out.rename(columns={kec_col: 'kecamatan', year_col: 'tahun'}, inplace=True)
    out['kecamatan'] = out['kecamatan'].astype(str).str.strip()
    out['tahun'] = pd.to_numeric(out['tahun'], errors='coerce').astype('Int64')
    out = out.dropna(subset=['kecamatan', 'tahun'])
    return out


def pick_numeric_column(df: pd.DataFrame, prefer: List[str]) -> Optional[str]:
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    if not num_cols:
        tmp = df.copy()
        for c in tmp.columns:
            if c in ['kecamatan', 'tahun']:
                continue
            tmp[c] = pd.to_numeric(tmp[c], errors='coerce')
        num_cols = [c for c in tmp.columns if pd.api.types.is_numeric_dtype(tmp[c])]

    for cand in prefer:
        match = safe_col(df, [cand])
        if match and match in df.columns:
            return match

    return num_cols[0] if num_cols else None


def minmax_risk(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors='coerce')
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mn) / (mx - mn)


def compute_pre_silver_index(
    health_static_csv: str,
    security_static_csv: str,
    out_index_csv: str,
    a_security: float = 0.5,
) -> None:
    health = pd.read_csv(health_static_csv)
    security = pd.read_csv(security_static_csv)

    # ---------- HEALTH normalisasi schema ----------
    # Target akhir: kesehatan punya kolom ['kecamatan','tahun','jumlah_fasilitas_kesehatan']
    if 'kecamatan' not in health.columns or 'tahun' not in health.columns:
        kec_col = safe_col(health, ['kecamatan', 'Kecamatan', 'kec', 'district', 'nama_kecamatan'])
        if kec_col is None:
            raise ValueError(
                f"Tidak menemukan kolom kecamatan di dataset kesehatan. Columns={list(health.columns)}"
            )

        # tahun biasanya 2024 (sesuai dataset Anda) atau bisa ditebak dari nama file
        year_guess = None
        m = re.search(r'(19\d{2}|20\d{2})', os.path.basename(health_static_csv))
        if m:
            year_guess = int(m.group(1))
        if year_guess is None:
            year_guess = 2024

        hc = health.copy()
        hc['kecamatan'] = hc[kec_col].astype(str).str.strip()
        hc['tahun'] = int(year_guess)

        fasilitas_col = safe_col(hc, ['jumlah_fasilitas_kesehatan', 'jumlah', 'count', 'fasilitas', 'fasilitas_kesehatan', 'value'])
        if fasilitas_col is None:
            # fallback: hitung jumlah baris sebagai proxy jumlah fasilitas
            health = hc.groupby(['kecamatan', 'tahun'], as_index=False).size().rename(columns={'size': 'jumlah_fasilitas_kesehatan'})
        else:
            hc['jumlah_fasilitas_kesehatan'] = pd.to_numeric(hc[fasilitas_col], errors='coerce')
            health = hc.groupby(['kecamatan', 'tahun'], as_index=False)['jumlah_fasilitas_kesehatan'].sum()

    # pastikan jumlah_fasilitas_kesehatan ada
    if 'jumlah_fasilitas_kesehatan' not in health.columns:
        jumlah_col = safe_col(health, ['jumlah_fasilitas_kesehatan', 'jumlah', 'count', 'fasilitas', 'fasilitas_kesehatan', 'value'])
        if jumlah_col is None:
            health = health.groupby(['kecamatan', 'tahun'], as_index=False).size().rename(columns={'size': 'jumlah_fasilitas_kesehatan'})
        else:
            health = health.rename(columns={jumlah_col: 'jumlah_fasilitas_kesehatan'})

    health['jumlah_fasilitas_kesehatan'] = pd.to_numeric(health['jumlah_fasilitas_kesehatan'], errors='coerce')
    health = health.dropna(subset=['jumlah_fasilitas_kesehatan'])

    health['risk_fasilitas_kesehatan'] = 1.0 - minmax_risk(health['jumlah_fasilitas_kesehatan'])
    health['I_kesehatan'] = health['risk_fasilitas_kesehatan']

    # ---------- SECURITY normalisasi schema ----------
    # Target akhir: keamanan punya kolom ['kecamatan','tahun','jumlah_kejahatan']
    # Data BPS Anda: tabel wide, header tahun 2019..2022, dan kolom pertama berisi wilayah (mis. Kabupaten/Kota)

    def norm_cell(x: Any) -> str:
        return str(x).replace('\r', ' ').replace('\n', ' ').strip()

    if 'kecamatan' not in security.columns or 'tahun' not in security.columns:
        # wilayah: gunakan kolom pertama
        resort_col = security.columns[0]

        # deteksi kolom tahun
        year_cols = [c for c in security.columns if norm_cell(c) in ['2019', '2020', '2021', '2022']]
        if not year_cols:
            for c in security.columns:
                m = re.search(r'(19\d{2}|20\d{2})', norm_cell(c))
                if m and m.group(1) in ['2019', '2020', '2021', '2022']:
                    year_cols.append(c)

        if not year_cols:
            raise ValueError(
                'Tidak menemukan kolom tahun (2019-2022) di dataset keamanan. '
                f"Columns={list(security.columns)}"
            )

        sc = security.copy()
        # konversi kolom tahun ke numerik
        for yc in year_cols:
            sc[yc] = pd.to_numeric(sc[yc], errors='coerce')
        # buang baris yang kosong pada semua tahun
        sc = sc.dropna(subset=year_cols, how='all')

        sc = sc.rename(columns={resort_col: 'kecamatan'})
        sc['kecamatan'] = sc['kecamatan'].astype(str).str.replace(r'\r?\n', ' ', regex=True).str.strip()

        long_parts = []
        for yc in year_cols:
            tmp = sc[['kecamatan', yc]].copy()
            tmp = tmp.rename(columns={yc: 'jumlah_kejahatan'})
            tmp['tahun'] = int(norm_cell(yc))
            long_parts.append(tmp)
        security = pd.concat(long_parts, ignore_index=True)

    # pastikan jumlah_kejahatan ada
    if 'jumlah_kejahatan' not in security.columns:
        jumlah_col = pick_numeric_column(security, prefer=['jumlah_kejahatan', 'kejahatan', 'crime', 'jumlah', 'count', 'value'])
        if jumlah_col is None:
            raise ValueError(f"Tidak menemukan kolom numerik untuk keamanan. Columns={list(security.columns)}")
        security = security.rename(columns={jumlah_col: 'jumlah_kejahatan'})

    security['jumlah_kejahatan'] = pd.to_numeric(security['jumlah_kejahatan'], errors='coerce')
    security = security.dropna(subset=['jumlah_kejahatan'])

    security['risk_kejahatan'] = minmax_risk(security['jumlah_kejahatan'])
    security['I_keamanan'] = security['risk_kejahatan']

    merged = pd.merge(
        health[['kecamatan', 'tahun', 'I_kesehatan']],
        security[['kecamatan', 'tahun', 'I_keamanan']],
        on=['kecamatan', 'tahun'],
        how='outer'
    )

    merged['I_kesehatan'] = merged['I_kesehatan'].fillna(merged['I_kesehatan'].median())
    merged['I_keamanan'] = merged['I_keamanan'].fillna(merged['I_keamanan'].median())

    merged['Index_Keamanan_Kesehatan'] = a_security * merged['I_keamanan'] + (1.0 - a_security) * merged['I_kesehatan']

    ensure_dir(os.path.dirname(out_index_csv))
    merged.to_csv(out_index_csv, index=False)


def main():
    parser = argparse.ArgumentParser()

    # Direct download / external file mode
    parser.add_argument('--health_input_file', required=False, default=None,
                        help='Path lokal atau URL dataset kesehatan yang akan disimpan di raw_data/kesehatan.')
    parser.add_argument('--health_output_csv', required=False, default='./raw_data/kesehatan/kesehatan_fasilitas_ckan_standard.csv',
                        help='Output CSV untuk dataset kesehatan yang distandarkan.')
    parser.add_argument('--security_input_file', required=False, default=None,
                        help='Path lokal atau URL dataset keamanan yang akan disimpan di raw_data/keamanan.')
    parser.add_argument('--security_output_csv', required=False, default='./raw_data/keamanan/keamanan_kriminalitas_bps.csv',
                        help='Output CSV gabungan untuk dataset keamanan yang distandarkan.')
    parser.add_argument('--security_output_csv_regency', required=False, default='./raw_data/keamanan/keamanan_kriminalitas_bps_regency.csv',
                        help='Output CSV untuk dataset keamanan Regency/Regency.')
    parser.add_argument('--security_output_csv_municipality', required=False, default='./raw_data/keamanan/keamanan_kriminalitas_bps_municipality.csv',
                        help='Output CSV untuk dataset keamanan Municipality/Kota.')

    # index computation
    parser.add_argument('--health_static_csv', required=False, default='./raw_data/kesehatan/kesehatan_fasilitas_ckan_standard.csv')
    parser.add_argument('--security_static_csv', required=False, default='./raw_data/keamanan/keamanan_kriminalitas_bps.csv')
    parser.add_argument('--out_index_csv', required=False, default='./lakehouse/index/index_keamanan_kesehatan.csv')
    parser.add_argument('--a_security_weight', type=float, default=0.5)

    args = parser.parse_args()

    ensure_dir(RAW_DIR)
    ensure_dir(STATIC_DIR)

    if args.health_input_file:
        health_output = os.path.join(BASE_DIR, args.health_output_csv.replace('./', ''))
        print(f'[ingest] Standarisasi kesehatan: {args.health_input_file}')
        print(f'[ingest] Menyimpan ke: {health_output}')
        load_path_to_standard_csv(args.health_input_file, health_output, dataset_type='health')
        print('[ingest] Kesehatan disimpan ke raw_data/kesehatan.')

    if args.security_input_file:
        security_output = os.path.join(BASE_DIR, args.security_output_csv.replace('./', ''))
        security_output_regency = os.path.join(BASE_DIR, args.security_output_csv_regency.replace('./', ''))
        security_output_municipality = os.path.join(BASE_DIR, args.security_output_csv_municipality.replace('./', ''))

        print(f'[ingest] Standarisasi keamanan: {args.security_input_file}')
        print(f'[ingest] Menyimpan gabungan ke: {security_output}')
        print(f'[ingest] Menyimpan Regency ke: {security_output_regency}')
        print(f'[ingest] Menyimpan Municipality ke: {security_output_municipality}')
        save_security_dataset(
            args.security_input_file,
            out_regency_csv=security_output_regency,
            out_municipality_csv=security_output_municipality,
            out_combined_csv=security_output,
        )
        print('[ingest] Keamanan disimpan ke raw_data/keamanan.')

    print('[index] Compute pre-silver index...')
    health_csv = os.path.join(BASE_DIR, args.health_static_csv.replace('./', ''))
    security_csv = os.path.join(BASE_DIR, args.security_static_csv.replace('./', ''))
    out_index = os.path.join(BASE_DIR, args.out_index_csv.replace('./', ''))

    compute_pre_silver_index(
        health_static_csv=health_csv,
        security_static_csv=security_csv,
        out_index_csv=out_index,
        a_security=args.a_security_weight,
    )

    print(f'[index] Wrote: {out_index}')


if __name__ == '__main__':
    main()


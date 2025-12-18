import duckdb
import os

# --- KONFIGURASI PATH ---
input_csv = 'seeds/raw_payroll.csv'
output_folder = 'file_parquet'
output_filename = 'dirty_data.parquet'
# Menggabungkan path folder dan nama file
output_path = os.path.join(output_folder, output_filename)

print(f"--- MULAI EXPORT DATA KOTOR (RAW) KE PARQUET ---")

# 1. Validasi File Sumber
if not os.path.exists(input_csv):
    print(f"[ERROR] File sumber tidak ditemukan: {input_csv}")
    print("Pastikan file 'raw_payroll.csv' ada di folder 'seeds'.")
    exit()

# 2. Koneksi DuckDB (In-Memory)
# Kita pakai in-memory supaya prosesnya terisolasi dan cepat.
con = duckdb.connect(':memory:')

# 3. Eksekusi Export (Mode: All Varchar / Paksa Jadi Teks)
# all_varchar=True adalah kuncinya. Ini memaksa DuckDB membaca angka '$5000' sebagai TEKS.
# Sehingga nanti di Power BI tidak bisa dijumlahkan (Simulasi Masalah).
query = f"""
    COPY (
        SELECT * FROM read_csv('{input_csv}', header=True, all_varchar=True)
    ) TO '{output_path}' (FORMAT PARQUET)
"""

try:
    print(f"[PROCESS] Sedang membaca CSV dan menulis ke '{output_path}'...")
    con.sql(query)
    print(f"[SUCCESS] Berhasil! File '{output_filename}' sudah ada di folder '{output_folder}'.")
except Exception as e:
    print(f"[FAIL] Gagal melakukan export: {e}")
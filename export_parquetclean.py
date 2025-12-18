import duckdb
import os

db_path = 'payroll_db.duckdb'
csv_path = 'seeds/raw_payroll.csv'

# Cek apakah database ada
if not os.path.exists(db_path):
    print(f"Error: {db_path} tidak ditemukan! Jalankan 'sqlmesh plan' dulu bro.")
    exit()

con = duckdb.connect(db_path)

# 1. Bikin file 'clean_data.parquet' di folder 'file_parquet' (Solusi Kamu)
print("2. Sedang membuat clean_data.parquet di folder 'file_parquet' (Data Solusi)...")
# Pastikan folder tujuan ada
os.makedirs('file_parquet', exist_ok=True)

# Gunakan path absolut agar DuckDB bisa menulis tanpa masalah
target_path = os.path.join('file_parquet', 'clean_data.parquet')
sql_path = os.path.abspath(target_path).replace('\\', '/')  # DuckDB suka forward-slash di Windows
try:
    con.sql(f"COPY payroll.fct_payroll TO '{sql_path}' (FORMAT PARQUET)")
    print(f"   Sukses! {target_path} berhasil dibuat (Isinya data bersih & angka valid).")
except Exception as e:
    print(f"   Gagal ekspor data bersih. Pastikan model 'fct_payroll' ada.\nError: {e}")

print("\n--- SELESAI ---")
import pyarrow.flight as flight
import pyarrow.csv as csv
import os

# Konfigurasi
SERVER_URL = "grpc://localhost:9999" # Port baru kita
CSV_PATH = "seeds/raw_payroll.csv"   # Lokasi data mentah

def main():
    print(f"\n🚀 [CLIENT SENDER] Memulai Proses Upload...")
    
    # 1. Cek File
    if not os.path.exists(CSV_PATH):
        print(f"❌ Error: File {CSV_PATH} tidak ditemukan.")
        return

    client = flight.FlightClient(SERVER_URL)
    
    # 2. Baca Raw Data
    print(f"📄 Membaca data mentah dari: {CSV_PATH}")
    try:
        # PyArrow membaca CSV dengan sangat cepat
        my_table = csv.read_csv(CSV_PATH)
        print(f"   -> Terbaca {my_table.num_rows} baris.")
    except Exception as e:
        print(f"❌ Gagal baca CSV: {e}")
        return

    # 3. Kirim ke Server (Trigger ETL Process)
    print("📡 Mengirim ke Server (Ingesting & Transforming)...")
    
    # Kita namakan paket ini 'raw_upload'
    # Saat ini sampai di server, logic SQL (Staging -> Fct) di server otomatis jalan
    upload_descriptor = flight.FlightDescriptor.for_path("raw_upload")
    
    try:
        writer, _ = client.do_put(upload_descriptor, my_table.schema)
        writer.write_table(my_table)
        writer.close()
        print("✅ UPLOAD SUKSES!")
        print("   Server sekarang sedang membersihkan data (Logic SQLMesh/ETL berjalan di background).")
        print("   Silakan jalankan 'clientgov_req.py' untuk melihat hasilnya.")
        
    except Exception as e:
        print(f"❌ Gagal Upload ke Server: {e}")

if __name__ == "__main__":
    main()
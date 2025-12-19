import pyarrow.flight as flight
import pandas as pd
import json
import os

def get_data(action, params=None):
    # Koneksi: Menelepon Server di port 8815
    client = flight.connect("grpc://localhost:8815")
    
    # Packing Request:
    # Membungkus perintah (action) ke dalam JSON, lalu di-encode jadi bytes.
    # Ini ibarat nulis surat pesanan.
    command = {"action": action}
    if params:
        command.update(params)
    ticket_bytes = json.dumps(command).encode('utf-8')
    
    try:
        # Sending & Receiving:
        # Client kirim tiket -> Server proses -> Client terima Stream
        reader = client.do_get(flight.Ticket(ticket_bytes))
        
        # Materialization:
        # Mengubah stream data Arrow menjadi Pandas DataFrame (Tabel Excel-nya Python)
        return reader.read_all().to_pandas()
    except Exception as e:
        print(f"Server Error: {e}")
        return pd.DataFrame()

def main():
    while True:
        # Membersihkan layar terminal biar rapi
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("=== 🦅 PAYROLL DATA FLIGHT CONSOLE ===")
        print("1. Lihat Semua Data (Preview)")
        print("2. Cari Berdasarkan Nama Jabatan (LIVE Search)")
        print("3. Lihat Total Pengeluaran per Jabatan (Aggregated)")
        print("4. Keluar")
        
        choice = input("\nPilih menu (1-4): ")
        df = pd.DataFrame()
        
        if choice == '1':
            print("Fetching all data...")
            df = get_data("get_all")
            
        elif choice == '2':
            # Input User
            # User mengetik kata kunci, misal "Teacher" atau "Engineer"
            dept = input("Masukkan Nama Jabatan (contoh: Teacher): ")
            print(f"Requesting data for '{dept}'...")
            
            # Request dikirim dengan parameter tambahan "department"
            df = get_data("filter_dept", {"department": dept})
            
        elif choice == '3':
            print("Calculating stats on server...")
            # Request statistik, server yang akan melakukan GROUP BY
            df = get_data("summary_stats")
            
        elif choice == '4':
            print("Bye!")
            break
            
        # Display
        if not df.empty:
            print("\n" + "="*50)
            print(f"Diterima {len(df)} baris data.")
            print("="*50)
            
            # Kalau ini Menu 3 (Stats), format angka uangnya biar cantik
            if choice == '3' and 'total_spend' in df.columns:
                 pd.options.display.float_format = '${:,.2f}'.format
            
            print(df.head(10)) # Tampilkan 10 baris teratas saja
            input("\nTekan Enter untuk kembali ke menu...")
        else:
            print("\n[!] Data kosong atau tidak ditemukan.")
            input("\nTekan Enter...")

if __name__ == '__main__':
    main()
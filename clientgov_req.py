import pyarrow.flight as flight
import json
import pandas as pd
import os
import time

SERVER_URL = "grpc://localhost:9999"

def request_data(action_code, filename_output):
    client = flight.FlightClient(SERVER_URL)
    
    # Bikin Tiket
    ticket_json = json.dumps({'action': action_code})
    ticket = flight.Ticket(ticket_json.encode('utf-8'))
    
    print(f"\n⏳ Sedang mengambil data dari Server...")
    try:
        start_time = time.time()
        reader = client.do_get(ticket)
        arrow_table = reader.read_all()
        df = arrow_table.to_pandas()
        end_time = time.time()
        
        # Simpan ke Excel/CSV
        if filename_output.endswith('.xlsx'):
            df.to_excel(filename_output, index=False)
        else:
            df.to_csv(filename_output, index=False)
            
        print(f"✅ SUKSES! Data diterima dalam {end_time - start_time:.2f} detik.")
        print(f"📄 File tersimpan: {os.path.abspath(filename_output)}")
        print(f"📊 Total Baris: {len(df)}")
        
        # Preview Sedikit
        print("\n--- PREVIEW DATA ---")
        print(df.head())
        print("--------------------")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print("Tips: Apakah kamu sudah menjalankan SQLMesh Plan sehingga tabelnya terbentuk?")

def main():
    while True:
        print("\n" + "="*40)
        print("   SISTEM PELAPORAN GAJI PEMERINTAH")
        print("="*40)
        print("1. Download DATA BERSIH (Full Database)")
        print("2. Download LAPORAN ANGGARAN (Summary)")
        print("0. Keluar")
        
        pilihan = input(">> Masukkan Pilihan (1/2): ")
        
        if pilihan == '1':
            request_data('get_full_clean', 'Laporan_Full_Clean.xlsx')
        elif pilihan == '2':
            request_data('get_budget_report', 'Laporan_Anggaran_Per_Posisi.xlsx')
        elif pilihan == '0':
            print("Bye bye!")
            break
        else:
            print("Pilihan tidak valid!")

if __name__ == "__main__":
    main()
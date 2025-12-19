import pyarrow.flight as flight
import pandas as pd
import os

import json # Jangan lupa import ini di paling atas

def fetch_data_from_server():
    print("🔌 Menghubungkan ke Server Payroll...")
    try:
        client = flight.connect("grpc://localhost:8815")
        
        # --- UPDATE DISINI: Pakai JSON Ticket ---
        # Kita minta 'get_all' supaya bisa diolah jadi Excel lengkap
        ticket_dict = {"action": "get_all"} 
        ticket_bytes = json.dumps(ticket_dict).encode('utf-8')
        
        ticket = flight.Ticket(ticket_bytes)
        # ----------------------------------------

        reader = client.do_get(ticket)
        df = reader.read_all().to_pandas()
        return df
    except Exception as e:
        print(f"❌ Gagal koneksi: {e}")
        return None

def analyze_and_export(df):
    print("\n🕵️ Memulai Analisis Bisnis (Human Readable Mode)...")
    
    # --- LOGIC BARU: PRIORITASKAN MANUSIA, BUKAN ROBOT ---
    
    cols = [c.lower() for c in df.columns]
    
    # 1. Deteksi Kolom Grouping (Cari Job Title / Department yang BUKAN ID)
    # Kita cari kolom yang namanya persis atau mengandung kata kunci ini
    readable_keywords = ['job_title', 'title', 'role', 'position', 'department_name', 'dept_name']
    
    # Coba cari exact match dulu atau partial match yang kuat
    group_col = next((col for col in df.columns if any(k in col.lower() for k in readable_keywords)), None)
    
    # Kalau gak nemu, cari yang mengandung 'job' atau 'dept' TAPI 'id' nya gak boleh ada
    if not group_col:
        group_col = next((col for col in df.columns if ('job' in col.lower() or 'dept' in col.lower()) and 'id' not in col.lower()), None)
    
    # Kalau terpaksa banget baru pake ID (daripada error)
    if not group_col:
        print("⚠️ Warning: Tidak ditemukan kolom Jabatan/Departemen yang readable. Terpaksa pakai ID.")
        group_col = next((col for col in df.columns if 'id' in col.lower()), df.columns[0])

    # 2. Deteksi Kolom Gaji
    salary_col = next((col for col in df.columns if any(x in col.lower() for x in ['salary', 'pay', 'gross', 'net'])), None)
    
    # 3. Ekstra: Bikin Nama Lengkap (Kalau ada first/last name terpisah)
    # Ini biar sheet 'Raw Data' lebih enak dibaca
    if 'first_name' in df.columns and 'last_name' in df.columns:
        df['Full_Name'] = df['first_name'] + ' ' + df['last_name']
        # Pindahkan Full_Name ke depan biar langsung kelihatan
        cols = list(df.columns)
        cols.insert(0, cols.pop(cols.index('Full_Name')))
        df = df[cols]

    if salary_col and group_col:
        print(f"   🎯 Grouping Berdasarkan: '{group_col}' (Bukan ID)")
        print(f"   💰 Metrics Keuangan: '{salary_col}'")
        
        # AGREGASI
        summary = df.groupby(group_col)[salary_col].sum().reset_index()
        summary = summary.sort_values(by=salary_col, ascending=False)
        
        # Format Uang biar cantik (Opsional, efek visual di terminal)
        pd.options.display.float_format = '${:,.2f}'.format
        
        print("\n📊 Preview Laporan (Top 5 Pengeluaran Tertinggi):")
        print(summary.head())

        # EXPORT
        output_file = "Laporan_Gaji_Readable.xlsx"
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Sheet Summary
            summary.to_excel(writer, sheet_name='Executive Summary', index=False)
            
            # Auto-adjust column width (Biar kolom gak sempit kayak kejepit pintu)
            worksheet = writer.sheets['Executive Summary']
            worksheet.set_column(0, 0, 25) # Lebar kolom A (Nama Jabatan)
            worksheet.set_column(1, 1, 20) # Lebar kolom B (Total Duit)
            
            # Sheet Detail
            df.to_excel(writer, sheet_name='Employee Detail', index=False)
            
        print(f"\n✅ Laporan FIXED! Cek file: {output_file}")
        
    else:
        print("⚠️ Masih gagal deteksi kolom. Cek lagi query SQL kamu, pastikan select 'job_title' bukan cuma 'job_id'.")
        print(f"Kolom tersedia: {list(df.columns)}")

if __name__ == '__main__':
    df = fetch_data_from_server()
    if df is not None:
        analyze_and_export(df)
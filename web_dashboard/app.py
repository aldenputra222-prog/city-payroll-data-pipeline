import streamlit as st
import time
import pandas as pd
from backend_client import PayrollClient 
import altair as alt

# --- Konfigurasi Halaman Dasar ---
# Judul tab browser, icon, dan layout lebar
st.set_page_config(
    page_title="Payroll X Dashboard",
    page_icon="💸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Styling CSS Custom ---
# Biar tampilan metrik dan layout lebih rapi (menghilangkan padding bawaan yg kegedean)
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    div[data-testid="stMetric"] {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #ff4b4b;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }

    div[data-testid="stMetric"] label[data-testid="stMetricLabel"] {
        color: #31333F !important;
    }
    
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #000000 !important;
    }

    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- Inisialisasi Koneksi ke Server ---
# Coba connect ke backend gRPC, kalau gagal langsung stop aplikasi
try:
    grpc_client = PayrollClient("grpc://localhost:9999")
except Exception as e:
    st.error(f"❌ Gagal connect ke Server gRPC: {e}")
    st.info("💡 Pastikan server.py sudah running di port 9999")
    st.stop()

# --- Manajemen Session State ---
# Variabel memori sementara biar data gak ilang saat user klik tombol (refresh parsial)
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if 'creds' not in st.session_state:
    st.session_state['creds'] = {"id": "", "pass": ""}

if 'selected_db_file' not in st.session_state:
    st.session_state['selected_db_file'] = None

if 'summary_data' not in st.session_state:
    st.session_state['summary_data'] = None

if 'show_summary' not in st.session_state:
    st.session_state['show_summary'] = False

# --- Halaman Login ---
# Kalau user belum login, tampilkan form login
if not st.session_state['logged_in']:
    col_center = st.columns([1, 2, 1])
    
    with col_center[1]:
        st.title("🔐 Enterprise Payroll")
        st.write("Secure Access Gateway")
        st.markdown("---")
        
        st.info("Gunakan Client ID & Password yang terdaftar.")
        
        client_id = st.text_input("Client ID")
        password = st.text_input("Password", type="password")
        
        if st.button("Masuk Dashboard 🚀", use_container_width=True):
            if not client_id or not password:
                st.warning("⚠️ Harap isi Client ID dan Password!")
            else:
                with st.spinner("Memverifikasi Kredensial..."):
                    # Cek password ke server
                    success, response = grpc_client.get_file_list(client_id, password)
                
                if success:
                    # Simpan kredensial di session dan set status login
                    st.session_state['creds'] = {"id": client_id, "pass": password}
                    st.session_state['logged_in'] = True
                    
                    st.success("✅ Verifikasi Berhasil!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ Login Gagal: ID atau Password tidak valid!")
                    st.info("💡 Hubungi administrator jika lupa password")

# --- Halaman Utama Dashboard ---
# Kalau user sudah login, masuk ke sini
else:
    # --- Sidebar Navigasi ---
    with st.sidebar:
        st.title(f"🏢 {st.session_state['creds']['id']}")
        st.caption("Enterprise Edition v1.0")
        
        # Indikator status server
        status_col1, status_col2 = st.columns([1, 3])
        with status_col1:
            st.success("●")
        with status_col2:
            st.caption("System Online")
        
        with st.expander("ℹ️ Queue System Info"):
            st.markdown("""
            **Bagaimana Sistem Bekerja:**
            
            Ketika Anda upload file:
            1. 📤 File dikirim ke server
            2. ⏳ Jika ada client lain sedang proses, Anda akan antri
            3. 🔄 Server memproses satu request per waktu
            4. ✅ Anda mendapat notifikasi saat selesai
            
            **Estimasi Waktu:**
            - File < 10MB: ~30-60 detik
            - File 10-100MB: ~1-3 menit
            - File > 100MB: ~3-10 menit
            
            *Waktu dapat lebih lama jika ada antrian.*
            """)
        
        st.markdown("### 📂 Data Management")
        
        # Ambil daftar file yang tersedia di server
        has_files, file_data = grpc_client.get_file_list(
            st.session_state['creds']['id'], 
            st.session_state['creds']['pass']
        )
        
        # Dropdown buat milih database DuckDB
        if has_files:
            clean_files = file_data.get('clean', [])
            
            if clean_files:
                selected = st.selectbox(
                    "Pilih Database:", 
                    clean_files, 
                    index=0,
                    help="Pilih file DuckDB yang sudah diproses"
                )
                st.session_state['selected_db_file'] = selected
            else:
                st.warning("⚠️ Belum ada data bersih.")
                st.info("Upload file CSV di tab 'Ingest Data'")
                st.session_state['selected_db_file'] = None

            # List file mentah (raw) buat audit
            with st.expander("🔍 Audit Raw Files"):
                raw_files = file_data.get('raw', [])
                if raw_files:
                    for idx, file in enumerate(raw_files, 1):
                        st.text(f"{idx}. {file}")
                else:
                    st.caption("Tidak ada file raw")
        else:
            st.error("❌ Gagal memuat daftar file")
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.rerun()
            
        st.markdown("---")
        
        # Tombol Logout
        if st.button("🚪 Logout", use_container_width=True, type="secondary"):
            st.session_state['logged_in'] = False
            st.session_state['selected_db_file'] = None
            st.rerun()

    # --- Area Konten Utama ---
    st.title("📊 Executive Dashboard")
    st.markdown("Pantau kinerja anggaran dan distribusi gaji secara *real-time*.")
    st.markdown("---")

    tab1, tab2 = st.tabs(["📤 Ingest Data", "📈 Financial Insights"])

    # --- TAB 1: Upload Data ---
    with tab1:
        st.info("""
        📋 **Upload file CSV mentah (Raw) untuk diproses pipeline.**
        
        ℹ️ *Sistem menggunakan queue mechanism: Jika ada client lain yang sedang memproses, 
        request Anda akan menunggu hingga proses sebelumnya selesai. Ini memastikan data integrity.*
        """)
        
        # Widget Upload File
        uploaded_file = st.file_uploader(
            "Drop CSV Here", 
            type=["csv"],
            help="Sistem dapat memproses file CSV dengan ukuran berapapun"
        )
        
        if uploaded_file:
            # Hitung ukuran file buat estimasi waktu
            file_size_mb = uploaded_file.size / (1024 * 1024)
            file_size_gb = file_size_mb / 1024
            
            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.metric("Nama File", uploaded_file.name)
            with col_info2:
                if file_size_mb < 1024:
                    st.metric("Ukuran", f"{file_size_mb:.2f} MB")
                else:
                    st.metric("Ukuran", f"{file_size_gb:.2f} GB")
            
            # Peringatan kalau file kegedean
            if file_size_mb > 1000:
                st.warning(f"⚠️ File sangat besar ({file_size_gb:.2f} GB)")
                st.info("""
                💡 **Tips untuk File Besar:**
                - Proses akan memakan waktu lebih lama (~{:.0f} menit)
                - Pastikan koneksi stabil
                - Pertimbangkan split per periode jika data historical
                """.format(file_size_mb / 100))
            elif file_size_mb > 100:
                st.info(f"📊 File berukuran menengah. Estimasi upload: ~{file_size_mb/50:.0f} menit")
            
            # Preview isi CSV
            with st.expander("👁️ Preview Data (5 baris pertama)"):
                try:
                    df_preview = pd.read_csv(uploaded_file, nrows=5)
                    st.dataframe(df_preview, use_container_width=True)
                    uploaded_file.seek(0)
                except Exception as e:
                    st.error(f"Gagal preview: {e}")
            
            st.success(f"✅ File siap dikirim ke Raw Zone")
            
            # Tombol Eksekusi Upload
            if st.button("🚀 Proses & Bersihkan Data", type="primary", use_container_width=True):
                uploaded_file.seek(0)
                
                spinner_msg = "🔄 Streaming data ke gRPC Server..."
                if file_size_mb > 100:
                    spinner_msg = f"⏳ Uploading {file_size_mb:.0f}MB... Mohon tunggu, jangan refresh browser!"
                
                with st.spinner(spinner_msg):
                    # Kirim file ke backend
                    success, msg = grpc_client.upload_csv(
                        uploaded_file, 
                        st.session_state['creds']['id'],
                        st.session_state['creds']['pass']
                    )
                
                if success:
                    st.balloons()
                    st.success(msg)
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(msg)
                    st.info("💡 Periksa log server untuk detail error")

    # --- TAB 2: Laporan & Visualisasi ---
    with tab2:
        target_file = st.session_state.get('selected_db_file')

        if not target_file:
            st.info("👈 Pilih file database di sidebar untuk memulai analisis")
            st.markdown("### Langkah-langkah:")
            st.markdown("""
            1. 📤 Upload CSV di tab **Ingest Data**
            2. ⏳ Tunggu proses transformasi selesai
            3. 📂 Pilih file di **sidebar**
            4. ⚡ Klik tombol **Tarik Laporan**
            """)
        else:
            # Header Laporan
            col_header, col_action = st.columns([3, 1])
            
            with col_header:
                st.markdown(f"### 📂 Active Dataset")
                st.code(target_file, language=None)
            
            with col_action:
                refresh_btn = st.button(
                    "⚡ Tarik Laporan", 
                    type="primary", 
                    use_container_width=True
                )
            
            # Proses Tarik Data (Aggregasi)
            if refresh_btn:
                with st.spinner('🔍 Querying DuckDB & Aggregating Data...'):
                    # Request data summary ke backend
                    success, data = grpc_client.get_summary_report(
                        st.session_state['creds']['id'],
                        st.session_state['creds']['pass'],
                        target_file
                    )
                
                if success:
                    st.session_state['summary_data'] = data
                    st.session_state['show_summary'] = True
                else:
                    st.session_state['show_summary'] = False
                    st.error(f"❌ Gagal mengambil data: {data}")
                    st.info("💡 Periksa koneksi server atau coba refresh")
            
            # Tampilkan Hasil Laporan
            if st.session_state['show_summary'] and st.session_state['summary_data'] is not None:
                data = st.session_state['summary_data']
                
                if data.empty:
                    st.warning("⚠️ Data kosong atau belum ada transaksi")
                else:
                    # Normalisasi nama kolom (biar aman kalau ada spasi/huruf besar)
                    data.columns = data.columns.str.lower().str.strip().str.replace(' ', '_')
                    
                    # Pastikan tipe data angka benar
                    numeric_cols = ['total_budget', 'total_employee']
                    for col in numeric_cols:
                        if col in data.columns:
                            data[col] = pd.to_numeric(data[col], errors='coerce')
                    
                    data = data.dropna(subset=['job_title', 'total_budget'])
                    
                    if data.empty:
                        st.error("❌ Data tidak valid setelah preprocessing")
                    else:
                        # --- KPI Cards (Angka Penting) ---
                        total_budget = data['total_budget'].sum() if 'total_budget' in data.columns else 0
                        total_emp = data['total_employee'].sum() if 'total_employee' in data.columns else 0
                        avg_salary = total_budget / total_emp if total_emp > 0 else 0

                        m1, m2, m3 = st.columns(3)
                        
                        with m1:
                            st.metric(
                                "💰 Total Budget", 
                                f"${total_budget:,.0f}", 
                                delta="YTD",
                                help="Total budget keseluruhan tahun ini"
                            )
                        
                        with m2:
                            st.metric(
                                "👥 Total Karyawan", 
                                f"{total_emp:,}", 
                                delta="Active",
                                help="Jumlah karyawan aktif"
                            )
                        
                        with m3:
                            st.metric(
                                "📊 Rata-rata Gaji", 
                                f"${avg_salary:,.0f}", 
                                delta="Per Head",
                                help="Budget rata-rata per karyawan"
                            )

                        st.markdown("---")

                        # --- Visualisasi & Tabel ---
                        col_chart, col_table = st.columns([1.5, 1])
                        
                        with col_chart:
                            st.subheader("📊 Top 10 Posisi Termahal")
                            
                            with st.expander("🔍 Debug Info - Kolom Data"):
                                st.write("Kolom yang tersedia:", list(data.columns))
                                st.write("Sample data (3 baris pertama):")
                                st.dataframe(data.head(3))
                            
                            required_cols = ['job_title', 'total_budget']
                            missing_cols = [col for col in required_cols if col not in data.columns]
                            
                            if missing_cols:
                                st.error(f"❌ Kolom tidak ditemukan: {missing_cols}")
                                st.info("💡 Pastikan server query menghasilkan kolom: job_title, total_budget, total_employee")
                            else:
                                top_data = data.sort_values(
                                    by='total_budget', 
                                    ascending=False
                                ).head(10)
                                
                                if top_data.empty:
                                    st.warning("⚠️ Tidak ada data untuk ditampilkan")
                                else:
                                    st.caption(f"Menampilkan {len(top_data)} posisi dari total {len(data)} posisi")
                                    
                                    # Bikin Chart pakai Altair
                                    chart = alt.Chart(top_data).mark_bar(
                                        cornerRadius=5,
                                        size=30
                                    ).encode(
                                        x=alt.X(
                                            'total_budget:Q',
                                            title='Total Budget',
                                            axis=alt.Axis(
                                                format='$,.0f',
                                                labelColor='white',
                                                titleColor='white'
                                            )
                                        ),
                                        y=alt.Y(
                                            'job_title:N',
                                            sort='-x',
                                            title='Posisi',
                                            axis=alt.Axis(
                                                labelColor='white',
                                                titleColor='white',
                                                labelLimit=200
                                            )
                                        ),
                                        color=alt.Color(
                                            'total_budget:Q',
                                            scale=alt.Scale(
                                                scheme='turbo',
                                                domain=[
                                                    top_data['total_budget'].min(),
                                                    top_data['total_budget'].max()
                                                ]
                                            ),
                                            legend=None
                                        ),
                                        tooltip=[
                                            alt.Tooltip('job_title:N', title='Posisi'),
                                            alt.Tooltip('total_budget:Q', title='Budget', format='$,.0f'),
                                            alt.Tooltip('total_employee:Q', title='Karyawan', format=',d')
                                        ]
                                    ).properties(
                                        height=450,
                                        background='#0E1117'
                                    ).configure_axis(
                                        labelFontSize=12,
                                        titleFontSize=14,
                                        gridColor='#262730',
                                        domainColor='white'
                                    ).configure_view(
                                        strokeWidth=0
                                    )
                                    
                                    st.altair_chart(chart, use_container_width=True)
                        
                        with col_table:
                            st.subheader("📋 Rincian Data")
                            
                            st.dataframe(
                                data, 
                                use_container_width=True,
                                height=450,
                                column_config={
                                    "total_budget": st.column_config.ProgressColumn(
                                        "Budget Consumption",
                                        format="$%f",
                                        min_value=0,
                                        max_value=int(data['total_budget'].max()) if not data.empty else 100,
                                    ),
                                    "total_employee": st.column_config.NumberColumn(
                                        "Staff",
                                        format="%d 👤"
                                    )
                                }
                            )
                            
                            # Tombol Download Summary CSV
                            csv_export = data.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="📥 Download CSV",
                                data=csv_export,
                                file_name=f"report_{target_file.replace('.duckdb', '')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        
                        # --- Statistik Tambahan ---
                        with st.expander("📈 Detail Statistik"):
                            stats_col1, stats_col2 = st.columns(2)
                            
                            with stats_col1:
                                st.metric("Total Posisi", len(data))
                                st.metric("Budget Tertinggi", f"${data['total_budget'].max():,.0f}")
                            
                            with stats_col2:
                                st.metric("Budget Terendah", f"${data['total_budget'].min():,.0f}")
                                st.metric("Median Budget", f"${data['total_budget'].median():,.0f}")

                        # --- Fitur Download Full Data ---
                        st.markdown("---")
                        st.subheader("📥 Full Data Export")
                        st.caption("Download seluruh dataset dari .duckdb file (converted to CSV)")
                        
                        col_download, col_info = st.columns([2, 1])
                        
                        with col_download:
                            export_btn = st.button(
                                "⬇️ Download Full Dataset (CSV)", 
                                type="primary", 
                                use_container_width=True,
                                key="btn_export_full_data"
                            )
                        
                        with col_info:
                            st.metric("File Format", "CSV", help="Converted dari .duckdb")
                        
                        # Logic Download Full Data
                        if export_btn:
                            with st.spinner('⬇️ Mengunduh data lengkap dari .duckdb...'):
                                success_full, full_data = grpc_client.get_full_data(
                                    st.session_state['creds']['id'],
                                    st.session_state['creds']['pass'],
                                    target_file
                                )
                            
                            if success_full:
                                if full_data.empty:
                                    st.warning("⚠️ Data kosong - tidak ada baris untuk didownload")
                                else:
                                    full_data.columns = full_data.columns.str.lower().str.strip().str.replace(' ', '_')
                                    
                                    csv_full = full_data.to_csv(index=False).encode('utf-8')
                                    
                                    file_timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                                    export_filename = f"full_export_{target_file.replace('.duckdb', '')}_{file_timestamp}.csv"
                                    
                                    st.download_button(
                                        label=f"💾 {export_filename}",
                                        data=csv_full,
                                        file_name=export_filename,
                                        mime="text/csv",
                                        use_container_width=True,
                                        key="btn_download_full_csv"
                                    )
                                    
                                    # Info ukuran file
                                    info_col1, info_col2, info_col3 = st.columns(3)
                                    with info_col1:
                                        st.metric("Total Baris", f"{len(full_data):,}", help="Jumlah record dalam dataset")
                                    with info_col2:
                                        st.metric("Total Kolom", len(full_data.columns), help="Jumlah field/atribut")
                                    with info_col3:
                                        csv_size_kb = len(csv_full) / 1024
                                        if csv_size_kb > 1024:
                                            st.metric("Estimasi Ukuran", f"{csv_size_kb/1024:.2f} MB")
                                        else:
                                            st.metric("Estimasi Ukuran", f"{csv_size_kb:.2f} KB")
                                    
                                    st.success("✅ Siap untuk didownload!")
                                    
                                    with st.expander("👁️ Preview Data (10 baris pertama)"):
                                        st.dataframe(full_data.head(10), use_container_width=True)
                            else:
                                st.error(f"❌ Gagal mengunduh data: {full_data}")
                                st.info("💡 Periksa koneksi server atau coba refresh")

# --- Footer ---
st.markdown("---")
st.caption("🔐 Secure Multi-Tenant Platform | Powered by SQLMesh & DuckDB")
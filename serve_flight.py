import pyarrow.flight as flight
import pyarrow as pa
import duckdb
import json
import logging
import time
import os
import pandas as pd
import gc
import hashlib
import threading

from sqlmesh.core.context import Context

# ============================================================================
# KONFIGURASI LOGGING
# ============================================================================
# Setup biar kita bisa lihat aktivitas server di terminal (monitoring)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [SERVER] - %(message)s'
)

class BusinessSolutionServer(flight.FlightServerBase):
    
    def __init__(self, location):
        # Inisialisasi server Arrow Flight standar
        super(BusinessSolutionServer, self).__init__(location)
        
        # Info logging bahwa server mulai start
        logging.info("🔧 Initializing SQLMesh Engine (Multi-Tenant Mode)...")
        
        # Load engine SQLMesh (otak pemrosesan data) dari folder saat ini
        self.mesh_context = Context(paths=".")
        
        # Lokasi file database user (JSON)
        self.user_db_path = "users.json"
        
        # KUNCI PENTING: Lock ini biar kalau ada banyak user upload barengan,
        # prosesnya antri satu-satu (biar SQLMesh gak crash/race condition)
        self.upload_lock = threading.Lock()
        logging.info("🔒 Thread-safe upload lock initialized")

    def _hash_password(self, plain_password):
        # Fitur Keamanan: Ubah password teks biasa jadi kode acak (SHA-256)
        # Biar kalau database bocor, password asli gak ketahuan
        return hashlib.sha256(str(plain_password).strip().encode()).hexdigest()

    def _verify_credentials(self, client_id, password):
        # Cek apakah file database user ada?
        if not os.path.exists(self.user_db_path):
            logging.error("❌ Database users.json tidak ditemukan!")
            return False

        # Baca data user dari file JSON
        with open(self.user_db_path, "r") as f:
            users = json.load(f)

        # Cek 1: Apakah Username/Client ID terdaftar?
        if client_id not in users:
            logging.warning(f"🔐 Akses Ditolak: Client_ID '{client_id}' tidak terdaftar.")
            return False

        # Ambil password hash yang tersimpan di DB
        stored_hash = users[client_id].get('password')
        # Hash password yang diinput user sekarang
        input_hash = self._hash_password(password)
        
        # Cek 2: Bandingkan hash input vs hash tersimpan (harus sama persis)
        if input_hash != stored_hash:
            logging.warning(f"🔐 Akses Ditolak: Password salah untuk Client_ID: {client_id}")
            return False

        # Cek 3: Pastikan user punya folder penyimpanan sendiri (Tenant Isolation)
        tenant_path = os.path.join("storage", client_id)
        if not os.path.exists(tenant_path):
            logging.error(f"⚠️ Infrastruktur folder untuk {client_id} belum ada!")
            return False
            
        logging.info(f"✅ Login Berhasil: {client_id}")
        return True

    # Fungsi utama untuk menangani UPLOAD data (Put)
    def do_put(self, context, descriptor, reader, writer):
        temp_context = None 
        clean_db_path = None # Variable penampung path DB (buat jaga-jaga kalau perlu dihapus)
        
        try:
            # STEP 1: PARSING & AUTHENTICATION
            # Baca metadata yang dikirim client (ID, Pass, Nama File)
            metadata = json.loads(descriptor.path[0].decode('utf-8'))
            client_id = metadata.get('client_id')
            password = metadata.get('password')
            target_file = metadata.get('filename')
            
            # Buka DB user lagi buat ambil info detail user
            with open(self.user_db_path, "r") as f:
                users = json.load(f)
            
            user_data = users.get(client_id)
            
            # Verifikasi password lagi sebelum proses lanjut
            if not user_data or self._hash_password(password) != user_data.get('password'):
                raise flight.FlightServerError("❌ AUTHENTICATION_FAILED")

            # Ambil jenis industri user (corporate/education/hospital)
            industry_type = user_data.get('industry_type', 'corporate').lower()
            
            # ================================================================
            # [VALIDASI NAMA FILE] - Security Check
            # ================================================================
            # Ubah nama file jadi huruf kecil semua biar gak masalah huruf besar/kecil
            filename_check = target_file.lower()
            
            logging.info(f"🔍 Validasi Nama File: User='{industry_type}' vs File='{target_file}'")
            
            # Cek: Apakah nama file mengandung kata jenis industri user?
            if industry_type not in filename_check:
                error_msg = (
                    f"❌ REJECTED! User tipe '{industry_type}' hanya boleh upload file "
                    f"yang mengandung kata '{industry_type}' di namanya."
                )
                logging.error(error_msg)
                # Kalau gak ada, TOLAK dan STOP proses disini.
                raise flight.FlightServerError(error_msg)
                
            logging.info("✅ Validasi Nama File: OK")
            # ================================================================

            # STEP 2: PATH CONSTRUCTION (Siapkan lokasi file)
            # Bersihkan ekstensi file (.csv) dari nama
            base_filename = os.path.splitext(target_file)[0]
            # Bikin nama file database output yang unik
            clean_db_name = f"{client_id}_{industry_type}_{base_filename}.duckdb"
            
            # Tentukan path lengkap folder Raw (File Mentah)
            raw_file_path = os.path.abspath(
                os.path.join("storage", client_id, "Raw", target_file)
            ).replace("\\", "/")
            
            # Tentukan path lengkap folder Clean (Hasil Olahan)
            clean_db_path = os.path.abspath(
                os.path.join("storage", client_id, "Clean", clean_db_name)
            ).replace("\\", "/")

            # STEP 3: SAVE RAW CSV
            # Pastikan folder Raw ada, kalau belum ada, buat dulu
            os.makedirs(os.path.dirname(raw_file_path), exist_ok=True)
            
            # Baca data stream dari client dan ubah jadi Pandas DataFrame
            inp_table = reader.read_all()
            df = inp_table.to_pandas()
            # Simpan file asli ke folder Raw (Backup data mentah)
            df.to_csv(raw_file_path, index=False)
            
            logging.info(f"💾 File Raw Tersimpan: {target_file}")

            # STEP 4: SQLMESH TRANSFORMATION EXECUTION
            logging.info(f"⏳ Waiting for SQLMesh lock... (Client: {client_id})")
            
            # Mulai mode Antrian (Thread Safe). Hanya 1 proses SQLMesh jalan di satu waktu.
            with self.upload_lock:
                logging.info(f"🔒 LOCK ACQUIRED: {client_id}")
                
                # Masukkan path file raw dan db output ke variable lingkungan
                # Biar SQLMesh tau file mana yang harus diproses
                os.environ["SQLMESH__VARIABLES__CLIENT_RAW_PATH"] = raw_file_path
                os.environ["SQLMESH__GATEWAYS__LOCAL__CONNECTION__DATABASE"] = clean_db_path
                
                # Tentukan model SQL mana yang mau dijalankan (sesuai industri user)
                target_models = [
                    f"{industry_type}.stg_{industry_type}", # Staging
                    f"{industry_type}.fct_{industry_type}" # Fact Table
                ]
                
                logging.info(f"🎯 SQLMesh Planning for: {target_models}")
                
                # Inisialisasi context sementara dan jalankan Plan
                temp_context = Context(paths=".")
                temp_context.plan(
                    select_models=target_models,
                    auto_apply=True,       # Langsung eksekusi tanpa tanya
                    no_prompts=True,       # Jangan munculin prompt di terminal
                    include_unmodified=True # Proses ulang walaupun model gak berubah
                )
                
                logging.info(f"🔓 LOCK RELEASED: {client_id}")
                # Kunci dilepas, user lain boleh masuk
            
            # STEP 5: WAL FILE CLEANUP (Pembersihan File Sampah)
            # Hapus context SQLMesh dari memori
            del temp_context
            temp_context = None
            gc.collect() # Panggil tukang sampah (Garbage Collector) RAM
            time.sleep(0.5) # Istirahat bentar biar OS melepas file lock
            
            # Proses Checkpointing: Gabungkan file sementara (.wal) ke file utama (.duckdb)
            logging.info("🧹 Merging WAL file (Checkpointing)...")
            with duckdb.connect(clean_db_path) as con:
                con.execute("CHECKPOINT;") # Simpan permanen
                con.execute("VACUUM;")     # Padatkan ukuran file
            
            logging.info(f"✅ SUCCESS: {industry_type.upper()} Data Processed & Cleaned.")

        except Exception as e:
            # Kalau ada error apa saja...
            logging.error(f"❌ Error do_put: {e}")
            
            # [SAFETY FEATURE] HAPUS FILE CORRUPT
            # Kalau proses gagal di tengah jalan, file .duckdb biasanya rusak.
            # Kita hapus biar gak menuh-menuhin storage dan gak bikin error kedepannya.
            if clean_db_path and os.path.exists(clean_db_path):
                try:
                    if temp_context: del temp_context
                    gc.collect()
                    logging.warning(f"🧹 Membersihkan file corrupt/gagal: {clean_db_path}")
                    os.remove(clean_db_path) # Hapus file .duckdb
                    
                    # Hapus juga file .wal (Write Ahead Log) jika tertinggal
                    wal_path = clean_db_path + ".wal"
                    if os.path.exists(wal_path):
                        os.remove(wal_path)
                except Exception as cleanup_err:
                    logging.error(f"⚠️ Gagal cleanup file: {cleanup_err}")

            # Lempar error ke client biar user tau kalau gagal
            raise flight.FlightServerError(str(e))
            
        finally:
            # Pastikan memori selalu dibersihkan walau sukses atau gagal
            if temp_context is not None:
                del temp_context
            gc.collect()

    # Fungsi utama untuk menangani DOWNLOAD/QUERY data (Get)

    def do_get(self, context, ticket):
        try:
            # Parse perintah dari client (JSON)
            command = json.loads(ticket.ticket.decode('utf-8'))
            client_id = command.get('client_id')
            password = command.get('password')
            action = command.get('action')
            target_file = command.get('target_file')
            should_save = command.get('save_copy', True)

            # Validasi input dasar
            if not client_id: raise flight.FlightServerError("Client ID Missing!")
            if not os.path.exists(self.user_db_path): raise flight.FlightServerError("User DB Missing!")
            
            # Auth Check
            with open(self.user_db_path, "r") as f:
                users = json.load(f)
            
            user_data = users.get(client_id)
            if not user_data or self._hash_password(password) != user_data.get('password'):
                raise flight.FlightServerError("❌ AUTHENTICATION_FAILED")

            industry_type = user_data.get('industry_type', 'corporate').lower()
            
            # Setup path folder Clean dan Downloads
            clean_dir = os.path.abspath(os.path.join("storage", client_id, "Clean")).replace("\\", "/")
            download_dir = os.path.abspath(os.path.join("storage", client_id, "Downloads")).replace("\\", "/")
            if should_save: os.makedirs(download_dir, exist_ok=True)

            # Aksi 1: List Files
            if action == 'list_files':
                if not os.path.exists(clean_dir):
                    return flight.RecordBatchStream(pa.Table.from_pandas(pd.DataFrame({'filename': []})))
                files = [f for f in os.listdir(clean_dir) if f.endswith('.duckdb')]
                files.sort()
                return flight.RecordBatchStream(pa.Table.from_pandas(pd.DataFrame({'filename': files})))

            # Validasi: Harus pilih file sebelum query
            if not target_file: raise flight.FlightServerError("Pilih file dulu!")
            db_path = os.path.join(clean_dir, target_file).replace("\\", "/")
            con = None
            try:
                # Pola Koneksi Adaptif
                try:
                    logging.info(f"🔌 Connecting to DB (RW): {target_file}")
                    con = duckdb.connect(database=db_path, read_only=False)
                except Exception as rw_err:
                    logging.warning(f"⚠️ RW Fail, Retrying RO... ({rw_err})")
                    con = duckdb.connect(database=db_path, read_only=True)
                
                # Tentukan tabel target berdasarkan jenis industri user
                target_table = f"{industry_type}.fct_{industry_type}"
                money_col = "total_amount"
                logging.info(f"🔍 Querying table: {target_table}")

                # Aksi 2: Get Full Clean (Ambil semua data bersih TANPA AGGREGATION)
                if action == 'get_full_clean':
                    query = f"SELECT * FROM {target_table} ORDER BY job_title"
                    logging.info(f"📤 Full Export Mode: {target_file} - Fetching all rows...")
                # Aksi 3: Get Budget Report (Ambil ringkasan/agregasi)
                elif action == 'get_budget_report':
                    query = f"SELECT job_title, COUNT(*) as total_employee, SUM({money_col}) as total_budget FROM {target_table} GROUP BY 1 ORDER BY total_budget DESC"
                else:
                    raise flight.FlightServerError(f"Unknown action: {action}")
                
                # Jalankan query dan ubah hasil jadi Arrow Table
                logging.info(f"Executing query: {query}")
                arrow_table = con.execute(query).fetch_arrow_table()
                logging.info(f"✅ Query successful: {arrow_table.num_rows} rows returned")
                
            except Exception as db_err:
                error_str = str(db_err)
                logging.error(f"Database error: {error_str}")
                
                # Error handling khusus kalau tabel belum terbentuk/rusak
                if "Catalog Error" in error_str or "Binder Error" in error_str or "not found" in error_str.lower():
                    raise flight.FlightServerError(f"❌ Gagal Query! Pastikan Tabel '{target_table}' ada.. Detail: {error_str}")
                else:
                    raise flight.FlightServerError(f"❌ Database Error: {error_str}")
            finally:
                if con: con.close()

            # Fitur Arsip: Simpan hasil query jadi CSV di folder Downloads
            if should_save:
                file_base_name = target_file.replace(".duckdb", "")
                # Nama file beda tergantung jenis report
                if action == 'get_full_clean':
                    report_name = f"{file_base_name}_full_export.csv"
                else:
                    report_name = f"{file_base_name}_summary.csv"
                df_save = arrow_table.to_pandas()
                if not df_save.empty: 
                    df_save.to_csv(os.path.join(download_dir, report_name), index=False)
                    logging.info(f"💾 Export file saved: {report_name}")
            
            # Kirim data balik ke client
            return flight.RecordBatchStream(arrow_table)

        except Exception as e:
            logging.error(f"❌ Error Server: {e}")
            raise flight.FlightServerError(str(e))
        
    # Fungsi Helper untuk aksi-aksi kecil (seperti list files di awal)
    def do_action(self, context, action):
        try:
            if action.type == "list_files":
                # Baca parameter dari client
                body_bytes = action.body.to_pybytes()
                info = json.loads(body_bytes.decode('utf-8'))
                client_id = info.get('client_id')
                password = info.get('password')
                
                # Cek kelengkapan login
                if not client_id or not password:
                    yield flight.Result(json.dumps({"error": "Kredensial tidak lengkap", "success": False}).encode('utf-8'))
                    return
                # Verifikasi login
                if not self._verify_credentials(client_id, password):
                    yield flight.Result(json.dumps({"error": "Invalid credentials", "success": False}).encode('utf-8'))
                    return
                
                # Cek folder Storage user (Raw dan Clean)
                base_dir = os.path.join("storage", client_id)
                raw_files = os.listdir(os.path.join(base_dir, "Raw")) if os.path.exists(os.path.join(base_dir, "Raw")) else []
                clean_files = os.listdir(os.path.join(base_dir, "Clean")) if os.path.exists(os.path.join(base_dir, "Clean")) else []
                
                # Kirim daftar file ke client
                yield flight.Result(json.dumps({"success": True, "raw": raw_files, "clean": clean_files}).encode('utf-8'))
            else:
                raise flight.FlightServerError("Action not implemented!")
        except Exception as e:
            logging.error(f"❌ Error do_action: {e}")
            raise flight.FlightServerError(f"Error processing action: {str(e)}")

def main():
    # Setup Server di Port 9999 (Listen ke semua IP)
    server = BusinessSolutionServer("grpc://0.0.0.0:9999")
    logging.info("🚀 Business Server Ready (Filename Check Mode)")
    logging.info("🔐 Password Hashing: ENABLED (SHA-256)")
    # Jalankan server (looping forever)
    server.serve()

if __name__ == '__main__':
    main()
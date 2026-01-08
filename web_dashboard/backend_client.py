import pyarrow.flight as flight
import pyarrow as pa
import json
import pandas as pd

# ============================================================================
# Client Payroll gRPC
# ============================================================================
# Kelas ini tugasnya jadi perantara (wrapper) antara Streamlit dan Server.
# Dia ngebungkus semua kerumitan protokol Arrow Flight biar gampang dipanggil.

class PayrollClient:
    
    # --- 1. Inisialisasi Koneksi ---
    def __init__(self, location="grpc://localhost:9999"):
        """
        Buka jalur komunikasi ke server pas object dibuat.
        """
        self.client = flight.FlightClient(location)
        self.location = location

    # --- 2. Cek Login/Autentikasi ---
    def authenticate(self, client_id, password):
        """
        Ngetes password bener apa nggak dengan cara coba minta list file.
        Kalau server ngebolehin, berarti login sukses.
        """
        try:
            success, _ = self.get_file_list(client_id, password)
            return success
        except Exception as e:
            # Kalau error, diem aja (silent fail) demi keamanan
            print(f"Authentication failed: {e}")
            return False

    # --- 3. Minta Daftar File ---
    def get_file_list(self, client_id, password):
        """
        Minta server ngirim daftar file (Raw & Clean) punya user tersebut.
        Menggunakan metode 'Action' di Arrow Flight.
        """
        try:
            # Bungkus kredensial jadi JSON
            action_info = {
                "client_id": client_id,
                "password": password
            }
            
            # Bikin request action tipe 'list_files'
            action = flight.Action(
                "list_files",
                json.dumps(action_info).encode('utf-8')
            )
            
            # Kirim ke server
            results = self.client.do_action(action)
            
            # Baca balasan dari server
            response_received = False
            for result in results:
                body_bytes = result.body.to_pybytes()
                data = json.loads(body_bytes.decode('utf-8'))
                response_received = True
                
                # Cek kalau server ngeluh error (misal password salah)
                if "error" in data:
                    print(f"❌ Server Error: {data['error']}")
                    return False, {}
                
                # Kalau sukses, balikin datanya
                if data.get("success", False):
                    return True, data
            
            if not response_received:
                print("⚠️ Server tidak mengirim response")
                return False, {}
            
            return False, {}
            
        except flight.FlightUnauthenticatedError as auth_err:
            print(f"❌ Autentikasi Ditolak: {auth_err}")
            return False, {}
            
        except flight.FlightServerError as server_err:
            print(f"❌ Server Error: {server_err}")
            return False, {}
            
        except Exception as e:
            print(f"❌ Client Error: {e}")
            return False, {}

    # --- 4. Upload File CSV (Penting buat DE!) ---
    def upload_csv(self, file_buffer, client_id, password):
        """
        Upload CSV ke server.
        Alur: CSV -> Arrow Table (Super Cepat) -> Stream ke Server.
        """
        try:
            # Baca CSV jadi text semua dulu (biar server yang mikir tipe datanya)
            df = pd.read_csv(file_buffer, dtype=str)
            
            if df.empty:
                return False, "❌ File CSV kosong atau tidak valid"
            
            # KONVERSI KE ARROW TABLE
            # Ini kuncinya: Arrow mindahin data jauh lebih cepat daripada kirim JSON biasa
            table = pa.Table.from_pandas(df)

            # Siapin metadata (nama file, user, pass) buat dikirim duluan
            filename = getattr(file_buffer, 'name', 'raw_payroll.csv')
            descriptor_info = {
                "client_id": client_id,
                "password": password,
                "filename": filename
            }
            
            # Bikin amplop (Descriptor) buat data
            descriptor = flight.FlightDescriptor.for_path(
                json.dumps(descriptor_info).encode('utf-8')
            )

            # Mulai streaming upload (do_put)
            writer, _ = self.client.do_put(descriptor, table.schema)
            
            # Tulis datanya
            writer.write_table(table)
            
            # Tutup koneksi tulis (biar server tau upload udah kelar)
            writer.close()
            
            return True, "✅ Upload & SQLMesh Pipeline Berhasil Dijalankan!"
            
        except flight.FlightUnauthenticatedError:
            return False, "❌ Kredensial tidak valid untuk upload"
            
        except pd.errors.EmptyDataError:
            return False, "❌ File CSV kosong atau format tidak valid"
            
        except pa.ArrowInvalid as arrow_err:
            return False, f"❌ Error konversi data: {str(arrow_err)}"
            
        except Exception as e:
            return False, f"❌ Gagal Upload: {str(e)}"

    # --- 5. Ambil Data Summary (Report) ---
    def get_summary_report(self, client_id, password, target_file):
        """
        Minta server jalanin query agregasi (SUM, COUNT) dan balikin hasilnya.
        """
        try:
            # Siapin tiket request
            request_info = {
                "action": "get_budget_report",
                "client_id": client_id,
                "password": password,
                "target_file": target_file,
                "save_copy": False  # Gak usah simpen file di server, cukup kirim data aja
            }
            
            ticket = flight.Ticket(
                json.dumps(request_info).encode('utf-8')
            )

            # Minta data (do_get) -> Server bakal streaming balik Arrow Table
            reader = self.client.do_get(ticket)
            
            # Baca semua data dari stream
            result_table = reader.read_all()
            
            # Convert balik dari Arrow ke Pandas buat dipakai di Streamlit
            df = result_table.to_pandas()
            
            if df.empty:
                return True, pd.DataFrame() 
            
            return True, df
            
        except flight.FlightUnauthenticatedError:
            return False, "❌ Kredensial tidak valid untuk mengakses laporan"
        
        except flight.FlightServerError as server_err:
            error_msg = str(server_err)
            if "Catalog Error" in error_msg or "Binder Error" in error_msg:
                return False, "❌ Tabel tidak ditemukan. Pastikan file sudah diproses."
            else:
                return False, f"❌ Server Error: {error_msg}"
            
        except Exception as e:
            return False, f"❌ Gagal Ambil Data: {str(e)}"

    # --- 6. Ambil Data Lengkap (Tanpa Agregasi) ---
    def get_full_data(self, client_id, password, target_file):
        """
        Sama kayak di atas, tapi ini minta seluruh data mentah (Select *)
        buat fitur download CSV full.
        """
        try:
            request_info = {
                "action": "get_full_clean",
                "client_id": client_id,
                "password": password,
                "target_file": target_file,
                "save_copy": False
            }
            
            ticket = flight.Ticket(json.dumps(request_info).encode('utf-8'))
            reader = self.client.do_get(ticket)
            result_table = reader.read_all()
            
            df = result_table.to_pandas()
            return True, df
            
        except flight.FlightUnauthenticatedError:
            return False, "❌ Kredensial tidak valid untuk mengakses data"
        
        except flight.FlightServerError as server_err:
            error_msg = str(server_err)
            if "Catalog Error" in error_msg or "Binder Error" in error_msg:
                return False, "❌ Tabel tidak ditemukan. Pastikan file sudah diproses."
            else:
                return False, f"❌ Server Error: {error_msg}"
            
        except Exception as e:
            return False, f"❌ Gagal Ambil Data: {str(e)}"
    
    # --- 7. Tutup Koneksi ---
    def close(self):
        try:
            self.client.close()
        except:
            pass
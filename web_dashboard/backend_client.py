import pyarrow.flight as flight
import pyarrow as pa
import json
import pandas as pd

# ============================================================================
# PayrollClient: gRPC Client untuk Komunikasi dengan Business Solution Server
# ============================================================================
# Purpose: Handle semua komunikasi antara Streamlit app dengan gRPC server
# Architecture: Client-side wrapper untuk Apache Arrow Flight protocol

class PayrollClient:
    """
    Client untuk berkomunikasi dengan Business Solution Server via gRPC
    
    Features:
    - Authentication & Authorization
    - File listing & management
    - CSV upload & processing
    - Data retrieval & reporting
    """
    
    def __init__(self, location="grpc://localhost:9999"):
        """
        ====================================================================
        [POINT 1: CONNECTION INITIALIZATION]
        ====================================================================
        Inisialisasi koneksi ke gRPC Server
        
        Args:
            location (str): gRPC server address (format: grpc://host:port)
        
        Note:
            - Connection dibuat saat object di-initialize
            - Connection persistent sepanjang object lifecycle
            - Default port 9999 sesuai dengan server configuration
        """
        self.client = flight.FlightClient(location)
        self.location = location

    def authenticate(self, client_id, password):
        """
        ====================================================================
        [POINT 2: AUTHENTICATION WRAPPER]
        ====================================================================
        Fungsi utility untuk validasi kredensial user
        
        Strategy:
            Menggunakan get_file_list() sebagai authentication probe
            Jika server return success, artinya kredensial valid
        
        Args:
            client_id (str): Unique tenant identifier
            password (str): User password (akan di-hash di server)
        
        Returns:
            bool: True jika autentikasi berhasil, False jika gagal
        
        Design Pattern:
            - Reuse existing endpoint (get_file_list) daripada bikin endpoint baru
            - Efficient: Sekalian ambil data file list kalau auth sukses
        """
        try:
            success, _ = self.get_file_list(client_id, password)
            return success
        except Exception as e:
            # Silent fail untuk authentication
            # Security best practice: Jangan expose detail error
            print(f"Authentication failed: {e}")
            return False

    def get_file_list(self, client_id, password):
        """
        ====================================================================
        [POINT 3: FILE MANAGEMENT - LIST FILES]
        ====================================================================
        Retrieve daftar file Raw dan Clean dari server
        
        Flow:
        1. Buat Action request dengan kredensial
        2. Send via do_action() ke server
        3. Parse JSON response dari server
        4. Return success status + data
        
        Args:
            client_id (str): Tenant ID
            password (str): User password
        
        Returns:
            tuple: (success: bool, data: dict)
                   data format: {"raw": [...], "clean": [...]}
        
        Error Handling:
            - FlightUnauthenticatedError: Kredensial salah
            - FlightServerError: Server error (timeout, crash, etc)
            - Generic Exception: Network issues, parsing error
        """
        try:
            # ================================================================
            # STEP 1: Prepare Action Request
            # ================================================================
            action_info = {
                "client_id": client_id,
                "password": password
            }
            
            # Create Flight Action dengan type "list_files"
            action = flight.Action(
                "list_files",
                json.dumps(action_info).encode('utf-8')
            )
            
            # ================================================================
            # STEP 2: Send Request to Server
            # ================================================================
            results = self.client.do_action(action)
            
            # ================================================================
            # STEP 3: Parse Response
            # ================================================================
            # Iterator pattern: do_action returns generator
            response_received = False
            
            for result in results:
                # Convert Arrow Buffer to Python bytes
                body_bytes = result.body.to_pybytes()
                data = json.loads(body_bytes.decode('utf-8'))
                response_received = True
                
                # ============================================================
                # CRITICAL: Check for Server Error Message
                # ============================================================
                # Server bisa kirim {"error": "...", "success": False}
                # Ini bukan exception, tapi soft error via response body
                if "error" in data:
                    print(f"❌ Server Error: {data['error']}")
                    return False, {}
                
                # Success case: Return data
                if data.get("success", False):
                    return True, data
            
            # ================================================================
            # EDGE CASE: No Response Received
            # ================================================================
            if not response_received:
                print("⚠️ Server tidak mengirim response")
                return False, {}
            
            # Default fallback
            return False, {}
            
        except flight.FlightUnauthenticatedError as auth_err:
            # ================================================================
            # ERROR TYPE 1: Authentication Failed
            # ================================================================
            # Thrown oleh server kalau kredensial invalid
            print(f"❌ Autentikasi Ditolak: {auth_err}")
            return False, {}
            
        except flight.FlightServerError as server_err:
            # ================================================================
            # ERROR TYPE 2: Server Error
            # ================================================================
            # Generic server error (500, timeout, crash)
            print(f"❌ Server Error: {server_err}")
            return False, {}
            
        except Exception as e:
            # ================================================================
            # ERROR TYPE 3: Unexpected Error
            # ================================================================
            # Network issues, parsing error, etc
            print(f"❌ Client Error: {e}")
            return False, {}

    def upload_csv(self, file_buffer, client_id, password):
        """
        ====================================================================
        [POINT 4: DATA INGESTION - UPLOAD CSV]
        ====================================================================
        Upload CSV file ke server untuk processing via SQLMesh
        
        Flow:
        1. Read CSV dengan dtype=str (preserve raw data)
        2. Convert ke Arrow Table (zero-copy serialization)
        3. Create Flight Descriptor dengan metadata
        4. Stream data via do_put()
        5. Server trigger SQLMesh transformation
        
        Args:
            file_buffer: File object (dari st.file_uploader atau open())
            client_id (str): Tenant ID
            password (str): User password
        
        Returns:
            tuple: (success: bool, message: str)
        
        Design Decisions:
        ────────────────────────────────────────────────────────────────
        Q: Kenapa dtype=str?
        A: Preserve raw data integrity. SQLMesh yang handle type casting.
           Kalau pandas auto-detect type, data "kotor" bisa error.
        
        Q: Kenapa pakai Arrow Table?
        A: Efficient binary serialization. 10x faster than JSON/CSV stream.
           Zero-copy transfer via shared memory.
        
        Q: Kenapa metadata di descriptor?
        A: gRPC Flight pattern. Metadata travel ahead of data stream.
           Server bisa validate sebelum terima full payload.
        """
        try:
            # ================================================================
            # STEP 1: READ & VALIDATE CSV
            # ================================================================
            # dtype=str: Preserve all data as text (raw zone philosophy)
            # Server yang bertanggung jawab untuk type conversion
            df = pd.read_csv(file_buffer, dtype=str)
            
            # Validation: Check if dataframe is empty
            if df.empty:
                return False, "❌ File CSV kosong atau tidak valid"
            
            # ================================================================
            # STEP 2: CONVERT TO ARROW FORMAT
            # ================================================================
            # Arrow Table = Columnar in-memory format
            # Benefit: Fast serialization, schema validation
            table = pa.Table.from_pandas(df)

            # ================================================================
            # STEP 3: PREPARE METADATA
            # ================================================================
            # Extract filename (handle both file object & path string)
            filename = getattr(file_buffer, 'name', 'raw_payroll.csv')
            
            # Metadata yang dikirim ke server
            descriptor_info = {
                "client_id": client_id,
                "password": password,
                "filename": filename
            }
            
            # Create Flight Descriptor
            # Pattern: for_path() untuk metadata serialization
            descriptor = flight.FlightDescriptor.for_path(
                json.dumps(descriptor_info).encode('utf-8')
            )

            # ================================================================
            # STEP 4: STREAM UPLOAD (do_put)
            # ================================================================
            # do_put() = Streaming write dari client ke server
            # Returns: (writer, metadata_reader)
            writer, _ = self.client.do_put(descriptor, table.schema)
            
            # Write actual data
            writer.write_table(table)
            
            # IMPORTANT: Always close writer untuk finalize transfer
            writer.close()
            
            # ================================================================
            # SUCCESS
            # ================================================================
            return True, "✅ Upload & SQLMesh Pipeline Berhasil Dijalankan!"
            
        except flight.FlightUnauthenticatedError:
            # ================================================================
            # ERROR: Authentication Failed during Upload
            # ================================================================
            return False, "❌ Kredensial tidak valid untuk upload"
            
        except pd.errors.EmptyDataError:
            # ================================================================
            # ERROR: CSV Empty atau Corrupt
            # ================================================================
            return False, "❌ File CSV kosong atau format tidak valid"
            
        except pa.ArrowInvalid as arrow_err:
            # ================================================================
            # ERROR: Arrow Conversion Failed
            # ================================================================
            # Biasanya karena data type issue atau schema problem
            return False, f"❌ Error konversi data: {str(arrow_err)}"
            
        except Exception as e:
            # ================================================================
            # ERROR: Generic Error
            # ================================================================
            return False, f"❌ Gagal Upload: {str(e)}"

    def get_summary_report(self, client_id, password, target_file):
        """
        ====================================================================
        [POINT 5: DATA RETRIEVAL - GET REPORT]
        ====================================================================
        Retrieve aggregated summary report dari DuckDB
        
        Flow:
        1. Create Ticket dengan query parameters
        2. Send via do_get() ke server
        3. Server execute SQL query di DuckDB
        4. Stream Arrow Table back ke client
        5. Convert ke Pandas DataFrame
        
        Args:
            client_id (str): Tenant ID
            password (str): User password
            target_file (str): DuckDB filename (e.g., "client01_corporate_file.duckdb")
        
        Returns:
            tuple: (success: bool, data: pd.DataFrame or error_message: str)
        
        Server-side Behavior:
        ────────────────────────────────────────────────────────────────
        - Query: SELECT job_title, COUNT(*), SUM(total_amount) GROUP BY job_title
        - Output: Aggregated budget report per job position
        - Optional: save_copy=False → Tidak save ke Downloads folder
        
        Design Note:
        ────────────────────────────────────────────────────────────────
        save_copy parameter:
        - True: Server save hasil query ke CSV (archiving)
        - False: No-save mode (menghindari disk clutter)
        
        Untuk production dashboard → False (real-time query)
        Untuk scheduled reporting → True (need audit trail)
        """
        try:
            # ================================================================
            # STEP 1: PREPARE TICKET (Query Parameters)
            # ================================================================
            request_info = {
                "action": "get_budget_report",
                "client_id": client_id,
                "password": password,
                "target_file": target_file,
                "save_copy": False  # No-save mode: Prevent disk clutter
            }
            
            # Serialize ticket data
            ticket = flight.Ticket(
                json.dumps(request_info).encode('utf-8')
            )

            # ================================================================
            # STEP 2: SEND REQUEST & RECEIVE STREAM (do_get)
            # ================================================================
            # do_get() = Streaming read dari server ke client
            # Returns: RecordBatchStreamReader
            reader = self.client.do_get(ticket)
            
            # Read entire Arrow Table from stream
            result_table = reader.read_all()
            
            # ================================================================
            # STEP 3: CONVERT TO PANDAS
            # ================================================================
            # Arrow → Pandas conversion (efficient, maintains types)
            df = result_table.to_pandas()
            
            # Validation: Check if result is empty
            if df.empty:
                return True, pd.DataFrame()  # Return empty but valid
            
            return True, df
            
        except flight.FlightUnauthenticatedError:
            # ================================================================
            # ERROR: Authentication Failed
            # ================================================================
            return False, "❌ Kredensial tidak valid untuk mengakses laporan"
        
        except flight.FlightServerError as server_err:
            # ================================================================
            # ERROR: Server Error (Query Failed, Table Not Found, etc)
            # ================================================================
            error_msg = str(server_err)
            
            # Provide user-friendly error message
            if "Catalog Error" in error_msg or "Binder Error" in error_msg:
                return False, "❌ Tabel tidak ditemukan. Pastikan file sudah diproses."
            else:
                return False, f"❌ Server Error: {error_msg}"
            
        except Exception as e:
            # ================================================================
            # ERROR: Generic Error
            # ================================================================
            return False, f"❌ Gagal Ambil Data: {str(e)}"

    def get_full_data(self, client_id, password, target_file):
        """
        Retrieve FULL data dari fact table (tanpa aggregation)
        
        Use case: Export seluruh data untuk analisis eksternal
        
        Returns:
            tuple: (success: bool, data: pd.DataFrame or error_message: str)
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
    
    def close(self):
        """
        Close gRPC connection
        
        Note: Flight client auto-cleanup on garbage collection,
              tapi explicit close adalah best practice
        """
        try:
            self.client.close()
        except:
            pass  # Ignore close errors


# ============================================================================
# [USAGE EXAMPLE]
# ============================================================================
"""
# Initialize client
client = PayrollClient("grpc://localhost:9999")

# Authenticate
if client.authenticate("client01", "password123"):
    print("Login success!")
    
    # Upload CSV
    with open("payroll.csv", "rb") as f:
        success, msg = client.upload_csv(f, "client01", "password123")
        print(msg)
    
    # Get report
    success, data = client.get_summary_report("client01", "password123", "file.duckdb")
    if success:
        print(data.head())

# Cleanup
client.close()
"""
import pyarrow.flight as flight
import pyarrow as pa
import duckdb
import json
import logging
import pathlib
import os
import pyarrow.csv as pacsv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SERVER] - %(message)s')

class BusinessSolutionServer(flight.FlightServerBase):
    def __init__(self, location):
        super(BusinessSolutionServer, self).__init__(location)
        
        # [PENTING] Kita HAPUS self.conn di sini.
        # Kita cuma simpan nama path-nya saja.
        # Pastikan nama file ini SAMA PERSIS dengan di config.yaml SQLMesh kamu!
        self.db_file = 'payroll_db.duckdb' 
        
        # Cek database ada atau tidak (sekadar info)
        if not os.path.exists(self.db_file):
            logging.warning(f"⚠️ File Database {self.db_file} belum ada. Jalankan SQLMesh dulu!")
        else:
            logging.info(f"Connected target: {self.db_file}")

    # Fungsi pembantu untuk konek sebentar lalu tutup
    def get_connection(self):
        # read_only=False supaya bisa nulis kalau perlu
        return duckdb.connect(database=self.db_file, read_only=False)

    def do_get(self, context, ticket):
        con = None
        try:
            command = json.loads(ticket.ticket.decode('utf-8'))
            action = command.get('action')
            logging.info(f"Menerima Request Action: {action}")
            
            # BUKA KONEKSI (Hanya saat dibutuhkan)
            con = self.get_connection()
            
            query = ""
            
            if action == 'get_full_clean':
                # FIX: Tambahkan schema 'payroll.' di depan nama tabel
                query = "SELECT * FROM payroll.fct_payroll" 
                
            elif action == 'get_budget_report':
                # FIX: Tambahkan schema 'payroll.' di sini juga
                query = """
                SELECT 
                    job_title AS "Posisi Pekerjaan",
                    COUNT(*) AS "Jumlah Karyawan",
                    SUM(base_pay) AS "Total Anggaran Gaji",
                    AVG(base_pay) AS "Rata-Rata Gaji"
                FROM payroll.fct_payroll
                GROUP BY job_title
                ORDER BY "Total Anggaran Gaji" DESC
                """
            else:
                raise ValueError("Menu tidak tersedia.")
            
            logging.info(f"Executing Query...")
            result_rel = con.sql(query)
            
            return flight.RecordBatchStream(result_rel.arrow())
            
        except Exception as e:
            logging.error(f"Error serving data: {e}")
            raise
        finally:
            if con:
                con.close()

    # =========================================================================
    # FITUR 2: INGESTION (Terima Data Mentah)
    # =========================================================================
    def do_put(self, context, descriptor, reader, writer):
        try:
            logging.info("[INGESTION] Menerima Data Mentah Baru...")
            incoming_table = reader.read_all() # Data Arrow murni
            
            save_path = "seeds/raw_payroll.csv"
            
            # --- CARA KUNO (PANDAS) - HAPUS INI ---
            # df = incoming_table.to_pandas()  <-- MEMORY SPIKE DISINI
            # df.to_csv(save_path, index=False) <-- CPU SPIKE DISINI
            
            # --- CARA MODERN (PYARROW NATIVE) ---
            # Menulis langsung dari Arrow ke CSV. Jauh lebih efisien.
            pacsv.write_csv(incoming_table, save_path)
            
            logging.info(f"[SUCCESS] File Seeds Updated: {save_path}")
            logging.info(">> Database TIDAK DI-LOCK. Silakan jalankan 'sqlmesh plan' sekarang! <<")
            
        except Exception as e:
            logging.error(f"Gagal update seed: {e}")
            raise

def main():
    server = BusinessSolutionServer("grpc://0.0.0.0:9999")
    logging.info("🚀 Business Server Ready on Port 9999 (Lock-Free Mode)")
    server.serve()

if __name__ == '__main__':
    main()
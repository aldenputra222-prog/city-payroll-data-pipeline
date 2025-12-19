import pyarrow.flight as flight
import pyarrow as pa
import duckdb
import json
import logging
import pathlib

# Logging System
# Ini standar industri. Jangan pakai 'print()', pakai logging biar ada timestamp-nya.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SERVER] - %(message)s')

class AdvancedPayrollServer(flight.FlightServerBase):
    def __init__(self, location, parquet_path):
        super(AdvancedPayrollServer, self).__init__(location)
        self.parquet_path = str(pathlib.Path(parquet_path).absolute())
        
        # In-Memory Engine
        # Kita nyalakan DuckDB di RAM. Ini teknik 'Compute Layer' yang terpisah dari Storage.
        self.conn = duckdb.connect(database=':memory:')
        
        # Virtual View
        # Kita tidak load data fisik. Kita cuma kasih tau DuckDB: "Eh, file-nya di situ ya".
        logging.info(f"Loading data from: {self.parquet_path}")
        self.conn.execute(f"CREATE OR REPLACE VIEW payroll_data AS SELECT * FROM read_parquet('{self.parquet_path}')")
        
    def do_get(self, context, ticket):
        try:
            # Ticket Handling
            # Menerima pesanan dari Client (JSON yang di-encode jadi bytes)
            command_str = ticket.ticket.decode('utf-8')
            logging.info(f"Menerima Command: {command_str}")
            command = json.loads(command_str)
            
            query = ""
            
            # --- ROUTING LOGIC (Otak Server) ---
            
            if command['action'] == 'get_all':
                query = "SELECT * FROM payroll_data"
                
            elif command['action'] == 'filter_dept':
                # [UPDATE PENTING DISINI BRO] 
                # Dulu: SELECT * (Ambil semua sampah)
                # Sekarang: SELECT job_title (Ambil yang penting aja)
                # Kita taruh 'job_title' paling depan biar User langsung liat itu.
                dept = command.get('department', '').replace("'", "")
                
                query = f"""
                SELECT 
                    job_title,      -- Teks Nama Pekerjaan (WAJIB ADA)
                    base_pay,       -- Gaji Pokok
                    year            -- Tahun Data (Opsional)
                FROM payroll_data 
                WHERE job_title ILIKE '%{dept}%'
                ORDER BY base_pay DESC -- Kita urutkan dari gaji tertinggi biar enak dilihat
                """
                
            elif command['action'] == 'summary_stats':
                # Aggregation
                # Server yang menghitung (SUM/COUNT), bukan Client. Hemat bandwidth.
                query = """
                SELECT 
                    job_title, 
                    COUNT(*) as total_emp, 
                    SUM(base_pay) as total_spend 
                FROM payroll_data 
                GROUP BY job_title 
                ORDER BY total_spend DESC
                """
            
            else:
                raise ValueError("Unknown action")
            
            # Execution
            # Jalankan query di Memory Server
            logging.info(f"Executing SQL: {query}")
            result_rel = self.conn.sql(query)
            
            # Streaming
            # Kirim hasil balik ke Client via Arrow Protocol
            return flight.RecordBatchStream(result_rel.arrow())
            
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            raise

def main():
    DATA_PATH = "./file_parquet/clean_data.parquet"
    if not pathlib.Path(DATA_PATH).exists():
        print("⚠️ File Parquet tidak ditemukan! Jalankan ETL dulu.")
        return

    server = AdvancedPayrollServer("grpc://0.0.0.0:8815", DATA_PATH)
    logging.info("🚀 Advanced Flight Server Listening on Port 8815")
    server.serve()

if __name__ == '__main__':
    main()
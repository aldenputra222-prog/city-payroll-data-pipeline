# 🚀 End-to-End City Payroll Data Pipeline

A high-performance data engineering pipeline demonstrating **Cross-Platform Architecture** (Windows/Linux) using **Apache Arrow Flight**, **SQLMesh**, and **C++**.

![Status](https://img.shields.io/badge/Status-Completed-success)
![Stack](https://img.shields.io/badge/Tech-Arrow%20Flight%20%7C%20DuckDB%20%7C%20SQLMesh%20%7C%20C%2B%2B-blue)

## 📖 Project Overview

Project ini mensimulasikan sistem pengolahan data gaji pegawai pemerintahan secara terpusat. Sistem ini memisahkan **Client** (User Interface) dan **Server** (Data Processing) di dua lingkungan sistem operasi yang berbeda, meniru arsitektur *Hybrid Cloud* di dunia nyata.

**Key Features:**
* **High-Performance Transfer:** Menggunakan protokol Apache Arrow Flight (gRPC) untuk mengirim data besar dalam hitungan detik tanpa *serialization overhead*.
* **Cross-Platform:** Client berjalan di **Ubuntu (WSL)**, Server & Database berjalan di **Windows**.
* **Modern ETL:** Transformasi data menggunakan **SQLMesh** dengan arsitektur Medallion (Bronze -> Silver -> Gold).
* **Robust Data Cleaning:** Penanganan format mata uang dan anomali data menggunakan Regex tingkat lanjut di DuckDB.

---

## 🏗️ Architecture

```mermaid
graph LR
    A[Client C++ (Ubuntu/WSL)] -- Arrow Flight (gRPC) --> B[Server Python (Windows)]
    B -- Write CSV --> C[Raw Storage (Seeds)]
    C -- Ingest --> D[DuckDB Database]
    D -- Transform (SQLMesh) --> D
    B -- Read Gold Data --> A
    A -- Save CSV --> E[Excel Report]
```

1. Client (C++): Aplikasi CLI interaktif untuk upload raw data dan request laporan.

2. Server (Python): Menangani request Flight, menyimpan file raw, dan melayani data matang.

3. Transformation (SQLMesh):

 - Bronze: Raw CSV ingestion.

 - Silver: Data Cleaning (Regex removal of non-numeric chars, Type Casting).

 - Gold: Business Logic & Aggregation (Budget Reports).

# 🛠️ Tech Stack
Language: C++ (Client), Python (Server), SQL (Transformation).

Transport: Apache Arrow Flight (High-performance RPC).

Database: DuckDB (OLAP Engine).

Transformation Tool: SQLMesh.

Environment: WSL 2 (Ubuntu 22.04) & Windows 11.

# 📂 Project Structure
```Bash

├── cpp_client/            # Source code Client C++
│   ├── gov_app.cpp        # Main application logic
│   └── seeds/             # Local staging for upload
├── models/                # SQLMesh Models (Transformation Logic)
│   ├── stg_payroll.sql    # Silver Layer (Cleaning)
│   └── fct_payroll.sql    # Gold Layer (Final Product)
├── seeds/                 # Server-side Raw Data Storage
├── serve_flight.py        # Python Arrow Flight Server
├── config.yaml            # SQLMesh Configuration
└── README.md              # Project Documentation
```

# 🚀 How to Run
1. Server Side (Windows)
Pastikan Python dan library Arrow terinstall.

```Powershell
# Jalankan Server
python serve_flight.py
# Output: Server listening on grpc://0.0.0.0:9999
```

2. ETL Process (Windows)
Setelah data diterima dari client, jalankan transformasi data:

```Powershell
# Validasi dan Apply Model
sqlmesh plan --auto-apply
```

3. Client Side (Ubuntu / WSL)
Compile dan jalankan aplikasi C++.

```Bash
# Masuk ke folder client
cd cpp_client

# Compile
g++ gov_app.cpp -o gov_app -larrow -larrow_flight -O3

# Run Application
./gov_app
```

# 📸 Usage Demo
1. Upload Raw Data User memilih menu upload di C++ Client. Data dikirim via jaringan ke Windows Server.

2. Download Report User meminta laporan anggaran. Server mengirim stream data Arrow yang sudah bersih.

Note: Output file CSV menggunakan standar Internasional (Dot Decimal). Jika membuka di Excel dengan Region Indonesia, gunakan fitur Get Data -> From Text/CSV dan set Locale ke English (US).

# 🧠 Key Learnings & Challenges
Cross-Platform Networking: Mengatur firewall Windows dan IP forwarding agar WSL bisa berkomunikasi dengan Host Windows.

Data Consistency: Menangani isu "Garbage Data" (nilai kuadriliun) akibat kesalahan format locale (Titik vs Koma) menggunakan Regex [^0-9.] di DuckDB.

Memory Management: Implementasi std::unique_ptr dan std::move di C++ untuk menangani objek Arrow Flight secara efisien.

Created by [Alden] Vocational High School Student | Aspiring Data Engineer

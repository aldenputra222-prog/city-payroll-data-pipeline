# 🚀 ArrowFlow: Multi-Tenant gRPC Payroll Integration

A high-performance, secure, and multi-tenant data engineering pipeline designed to handle cross-industry payroll analytics using **Apache Arrow Flight**, **SQLMesh**, and **Streamlit**.

![Status](https://img.shields.io/badge/Status-Completed-success)
![Stack](https://img.shields.io/badge/Tech-Arrow%20Flight%20%7C%20DuckDB%20%7C%20SQLMesh%20%7C%20Streamlit-blue)
![Security](https://img.shields.io/badge/Security-SHA256%20Auth-red)

## 📖 Project Overview

Project ini mensimulasikan sistem manajemen data payroll terpusat yang melayani berbagai sektor industri (**Corporate, Education, Hospital**) dalam satu infrastruktur. Sistem ini memisahkan **Frontend** (Streamlit Dashboard) dan **Backend** (gRPC Server) untuk meniru arsitektur *Distributed System* yang aman dan scalable.

**Key Features:**
* **High-Performance Transfer:** Menggunakan protokol **Apache Arrow Flight (gRPC)** untuk streaming data CSV besar dan retrieval report tanpa *serialization overhead*.
* **Secure Multi-Tenant:** Isolasi database DuckDB per tenant dan penyimpanan kredensial terenkripsi (SHA-256) dalam `users.json`.
* **Smart Validation Gate:** Fitur keamanan yang menolak upload file jika nama file tidak sesuai dengan tipe industri akun (mencegah *Schema Mismatch*).
* **Automated ETL:** Transformasi data otomatis menggunakan **SQLMesh** dengan pemisahan layer Staging (Cleaning) dan Fact (Business Logic).

---

## 🏗️ Architecture

```bash
    A[User / Streamlit Dashboard] -- Arrow Flight (gRPC) --> B[Server Engine (Python)]
    B -- Validate Filename & Auth --> B
    B -- Write Raw CSV --> C[Raw Storage]
    C -- Ingest --> D[DuckDB per Tenant]
    D -- SQLMesh Transform (STG -> FCT) --> D
    B -- Read Gold Data --> A
    A -- Visualisation (Altair) --> E[Executive Report]
```

Frontend (Streamlit): Interface interaktif untuk Login, Upload Data, dan Visualisasi Laporan Keuangan.

Backend (Python Server): Menangani autentikasi, validasi nama file, thread-safe upload, dan trigger SQLMesh.

Transformation (SQLMesh):

Staging (STG): Data Cleaning (Regex removal currency, Type Casting, Null Handling).

Fact (FCT): Penerapan Business Logic spesifik (e.g., Overtime Risk untuk Corporate, Seniority Bonus untuk Education).

## 🛠️ Tech Stack
Language: Python 3.10+

Transport: Apache Arrow Flight (High-performance RPC)

Database: DuckDB (OLAP Engine)

Transformation: SQLMesh

Frontend: Streamlit & Altair

Security: Hashlib (SHA-256), Threading Locks

## 📂 Project Structure

```Bash
EDU_PAYROLL_TRANSFORM/
├── models/                     # SQLMesh Models (Transformation Logic)
│   ├── corporate/              # Corporate Sector Logic
│   ├── education/              # Education Sector Logic
│   └── hospital/               # Healthcare Sector Logic
├── web_dashboard/              # Frontend Application
│   ├── app.py                  # Main Streamlit Dashboard
│   └── backend_client.py       # Arrow Flight Client Wrapper
├── serve_flight.py             # Main Backend Server
├── users.json                  # Encrypted User Database
├── config.yaml                 # SQLMesh Configuration
└── README.md                   # Project Documentation
```

## 🚀 How to Run
1. Server Side (Backend)
Server bertugas menangani koneksi gRPC dan proses ETL.

```Bash

# Install Dependencies
pip install -r requirements.txt

# Initialize SQLMesh (First run only)
sqlmesh init duckdb

# Jalankan Server
python serve_flight.py
# Output: Server listening on grpc://0.0.0.0:9999
2. Client Side (Frontend)
Buka terminal baru untuk menjalankan dashboard.
```

```Bash

cd web_dashboard
streamlit run app.py
```

3. Usage Workflow
Login: Gunakan Client ID yang terdaftar (misal: NJ_Department_of_Education).

Ingest Data: Upload file CSV. PENTING: Nama file harus mengandung kata kunci industri (contoh: data_education_2024.csv).

Transform & Report: Klik "Tarik Laporan". Sistem akan memproses data via SQLMesh dan menampilkan grafik analitik.

## 🧠 Key Learnings & Challenges
Data Integrity vs Flexibility: Tantangan terbesar adalah menangani user yang salah upload file. Solusinya adalah implementasi Filename Keyword Validation di level server sebelum data menyentuh database.

Thread-Safety: Mengelola concurrent upload dari beberapa user sekaligus menggunakan threading.Lock() agar proses SQLMesh tidak bertabrakan.

Business Logic Complexity: Menerjemahkan kebutuhan bisnis yang berbeda (Gaji Guru vs Klaim RS) menjadi kode SQL yang modular dan maintainable menggunakan arsitektur Medallion.

Secure Storage: Belajar tidak menyimpan password dalam plain-text, melainkan menggunakan Hashing (SHA-256) untuk simulasi standar keamanan enterprise.
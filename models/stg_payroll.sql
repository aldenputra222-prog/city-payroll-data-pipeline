-- Konfigurasi Model: Menggunakan 'FULL' refresh.
-- Artinya tabel akan dihapus dan dibuat ulang setiap kali pipeline dijalankan
-- untuk memastikan data selalu konsisten dengan sumber terbaru.
MODEL (
  name payroll.stg_payroll,
  kind FULL
);

SELECT
  -- Mengambil kolom identitas untuk pelacakan data (Lineage)
  "Row ID" AS row_id,
  "Year" AS year,
  "Department Title" AS department,
  "Job Class Title" AS job_title,
  "Employment Type" AS employment_type,

  -- Cleaning: Menghapus simbol mata uang ($) dan pemisah ribuan (,) menggunakan Regex.
  -- Casting: Mengubah tipe data dari String ke Double untuk keperluan kalkulasi.
  CAST(REGEXP_REPLACE("Base Pay", '[$,]', '', 'g') AS DOUBLE) AS base_pay,

  -- Handling Null: Mengisi nilai kosong (NULL) dengan 0.0 menggunakan COALESCE.
  -- Ini mencegah error saat melakukan operasi matematika (penjumlahan/pembagian).
  COALESCE(CAST(REGEXP_REPLACE("Overtime Pay", '[$,]', '', 'g') AS DOUBLE), 0.0) AS overtime_pay,

  -- Normalisasi data Bonus dan Benefit
  COALESCE(CAST(REGEXP_REPLACE("Longevity Bonus Pay", '[$,]', '', 'g') AS DOUBLE), 0.0) AS longevity_bonus,
  CAST(REGEXP_REPLACE("Average Benefit Cost", '[$,]', '', 'g') AS DOUBLE) AS benefit_cost,
  
  -- Metadata: Menambahkan timestamp untuk audit kapan data diproses.
  CURRENT_TIMESTAMP AS processed_at

-- Ingestion Strategy: Membaca langsung file CSV fisik (Direct Read).
-- Metode ini memotong proses loading katalog untuk performa yang lebih cepat.
FROM read_csv('seeds/raw_payroll.csv', header=True, auto_detect=True, decimal_separator=',');
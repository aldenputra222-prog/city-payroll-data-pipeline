-- Konfigurasi Model: Menggunakan 'FULL' refresh.
-- Strategi ini sangat cocok untuk produk SaaS karena kita ingin
-- setiap upload baru menggantikan data lama di database "Clean" PT tersebut.
MODEL (
  name corporate.stg_corporate,
  kind FULL
);

SELECT
  -- 1. IDENTITAS & LINEAGE
  -- Mengambil kolom identitas agar kita bisa melacak data dari sumber aslinya.
  "Row ID" AS row_id,
  "Year" AS year,
  "Department Title" AS department,
  "Job Class Title" AS job_title,
  "Employment Type" AS employment_type,

  -- 2. DATA CLEANING & TYPE CASTING
  -- Kita gunakan REGEXP_REPLACE untuk membersihkan simbol mata uang.
  -- Penting: CAST ke DOUBLE dilakukan agar data bisa diolah secara matematis (Agregasi).
  CAST(REGEXP_REPLACE("Base Pay", '[$,]', '', 'g') AS DOUBLE) AS base_pay,

  -- 3. NULL HANDLING (Zero-Fill Policy)
  -- COALESCE sangat penting di produk finansial/payroll.
  -- Nilai NULL diubah jadi 0.0 agar tidak merusak perhitungan total budget.
  COALESCE(CAST(REGEXP_REPLACE("Overtime Pay", '[$,]', '', 'g') AS DOUBLE), 0.0) AS overtime_pay,

  -- Normalisasi data Bonus dan Benefit
  COALESCE(CAST(REGEXP_REPLACE("Longevity Bonus Pay", '[$,]', '', 'g') AS DOUBLE), 0.0) AS longevity_bonus,
  COALESCE(CAST(REGEXP_REPLACE("Average Benefit Cost", '[$,]', '', 'g') AS DOUBLE), 0.0) AS benefit_cost,

  -- 4. METADATA AUDIT
  -- Menambahkan timestamp saat data ini masuk ke zona "Clean".
  CURRENT_TIMESTAMP AS processed_at

-- 5. THE MAGIC VARIABLE: @client_raw_path
-- [LOGIC UTAMA]: Kita tidak lagi menulis 'seeds/raw_payroll.csv'.
-- Kita menggunakan Macro Variable '@client_raw_path' yang disuntikkan oleh server.py.
-- Ini memungkinkan SATU script SQL ini memproses file berbeda untuk PT yang berbeda.
FROM read_csv(@client_raw_path, 
    header=True, 
    auto_detect=True, 
    quote='"', 
    all_varchar=True  -- Ini kuncinya! Paksa baca teks dulu semua biar gak kepotong
);
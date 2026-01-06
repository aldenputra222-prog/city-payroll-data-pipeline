MODEL (
  name education.stg_education, -- Namespace 'education' sesuai folder
  kind FULL
);

SELECT
  -- 1. IDENTITAS (Penting buat laporan sekolah)
  "last_name" AS last_name,
  "first_name" AS first_name,
  "district" AS district_name,
  "school" AS school_name,
  "primary_job" AS job_title, -- Contoh: "Math Teacher Gr 5-8"
  
  -- 2. METRIK GURU (FTE & Experience)
  -- FTE (Full Time Equivalent): 1.0 = Full Time, 0.5 = Setengah hari.
  -- Kalau Null, kita anggap 1.0 (Default Full Time) biar aman perhitungannya.
  COALESCE(CAST("fte" AS DOUBLE), 1.0) AS fte_ratio,
  
  -- Pengalaman: Kalau kosong dianggap 0 tahun (Fresh Grad)
  COALESCE(CAST("experience_total" AS DOUBLE), 0.0) AS experience_years,
  
  -- Sertifikasi: Buat filter guru bersertifikat vs belum
  "certificate" AS certificate_status,
  
  -- 3. KEUANGAN (Salary)
  -- Data ini udah integer bersih, tapi kita cast ke DOUBLE buat kalkulasi.
  -- Kita anggap 0 kalau datanya kosong (Safety Net).
  COALESCE(CAST("salary" AS DOUBLE), 0.0) AS base_salary,
  
  -- 4. AUDIT TRAIL
  CURRENT_TIMESTAMP AS processed_at

-- Variable @client_raw_path disuntik server.py
FROM read_csv(@client_raw_path, header=True, auto_detect=True, quote='"', all_varchar=True);
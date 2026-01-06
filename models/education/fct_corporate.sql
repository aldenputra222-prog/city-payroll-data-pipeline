MODEL (
  name education.fct_education,
  kind FULL
);

SELECT
  -- [1] DIMENSI
  district_name,
  school_name,
  job_title,
  
  -- [2] METRIK UTAMA
  base_salary,
  fte_ratio,
  experience_years,

  -- [3] BUSINESS LOGIC: Employment Status
  -- Mendeteksi guru honorer/part-time vs guru tetap.
  CASE 
    WHEN fte_ratio >= 1.0 THEN 'Full Time'
    ELSE 'Part Time / Adjunct'
  END AS employment_status,

  -- [4] BUSINESS LOGIC: Seniority Bonus Simulation
  -- Asumsi: Sistem payroll ini memberikan bonus otomatis 5% untuk guru > 15 tahun.
  -- (Ini mensimulasikan logic tunjangan sertifikasi/senioritas).
  CASE 
    WHEN experience_years > 15 THEN base_salary * 0.05 
    ELSE 0 
  END AS seniority_bonus,

  -- [5] AGGREGATE METRIC: Total Cost to District
  -- Total uang yang keluar = Gaji Pokok + Bonus Senioritas
  (base_salary + (CASE WHEN experience_years > 15 THEN base_salary * 0.05 ELSE 0 END)) AS total_amount,

  -- [6] ANALYTICAL METRIC: Cost Efficiency
  -- Menghitung "Mahal" nya seorang guru per tahun pengalaman.
  -- Berguna untuk melihat apakah guru senior overpaid atau underpaid.
  ROUND(base_salary / NULLIF(experience_years, 0), 2) AS cost_per_experience_year,

  processed_at

FROM education.stg_education;
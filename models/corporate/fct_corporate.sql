MODEL (
  name corporate.fct_corporate,
  kind FULL
);

SELECT
  -- [1] DIMENSI (Konteks Data)
  row_id,
  year,
  department,
  job_title,

  -- [2] RAW METRICS (Data Dasar)
  base_pay,       -- Gaji Pokok
  overtime_pay,   -- Lembur

  -- [3] BUSINESS LOGIC: Total Cost to Organization (TCO)
  -- Logic: "Berapa total uang yang keluar dari kas perusahaan untuk karyawan ini?"
  -- Ini menjumlahkan semua komponen: Gaji + Lembur + Bonus + Beban Benefit.
  (base_pay + overtime_pay + longevity_bonus + benefit_cost) AS total_amount,

  -- [4] CATEGORIZATION: Overtime Risk Detection (Risk Management)
  -- Logic: Menandai karyawan dengan lembur berlebih (Red Flag).
  -- Jika lembur > 25% gaji pokok, ini indikasi kelelahan kerja atau inefisiensi.
  CASE 
    WHEN overtime_pay > (0.25 * base_pay) THEN 'High Overtime'
    WHEN overtime_pay > 0 THEN 'Regular Overtime'
    ELSE 'No Overtime'
  END AS overtime_status,

  -- [5] ANALYTICAL METRIC: Loyalty Bonus Ratio
  -- Logic: Menghitung bobot bonus loyalitas terhadap gaji pokok.
  -- NULLIF(base_pay, 0) adalah 'Safety Net' untuk mencegah error Division by Zero.
  ROUND((longevity_bonus / NULLIF(base_pay, 0)) * 100, 2) AS loyalty_bonus_percentage,

  -- [6] AUDIT TRAIL
  processed_at

-- Data diambil dari layer Staging yang sudah bersih
FROM corporate.stg_corporate;
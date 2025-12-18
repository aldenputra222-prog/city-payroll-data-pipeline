MODEL (
  name payroll.fct_payroll,
  kind FULL
);

SELECT
  row_id,
  year,
  department,
  job_title,

  -- [1] RAW METRICS (Data Mentah)
  -- Kita panggil kolom ini supaya bisa dianalisis secara terpisah di Power BI.
  base_pay,       -- Gaji Pokok (Penting untuk pembanding)
  overtime_pay,   -- Lembur (WAJIB ADA untuk Chart "True Overtime Cost")

  -- [2] BUSINESS LOGIC: Total Cost of Ownership (TCO)
  -- Menjawab: "Berapa total uang yang benar-benar keluar dari dompet perusahaan?"
  -- Kita jumlahkan semua komponen pendapatan + biaya benefit.
  -- COALESCE digunakan untuk jaga-jaga kalau ada nilai NULL, dianggap 0.
  (base_pay + overtime_pay + longevity_bonus + benefit_cost) AS total_cost_to_org,

  -- [3] CATEGORIZATION: Overtime Risk Detection
  -- Menjawab: "Siapa yang kerjanya tidak sehat atau indikasi fraud?"
  -- Logic: Jika uang lembur tembus 25% dari gaji pokok, itu 'High Overtime' (Red Flag).
  -- Jika cuma > 0, itu lembur wajar. Sisanya tidak lembur.
  CASE 
    WHEN overtime_pay > (0.25 * base_pay) THEN 'High Overtime'
    WHEN overtime_pay > 0 THEN 'Regular Overtime'
    ELSE 'No Overtime'
  END AS overtime_status,

  -- [4] METRIC CALCULATION: Loyalty/Bonus Ratio
  -- Menjawab: "Seberapa besar bonus loyalitas dibanding gaji pokok?"
  -- Logic: Pembagian Matematika. NULLIF(base_pay, 0) adalah safety net.
  -- Artinya: "Kalau base_pay nol, jangan dibagi (biar gak error Division by Zero), tapi hasilkan NULL".
  (longevity_bonus / NULLIF(base_pay, 0)) * 100 AS loyalty_bonus_percentage

FROM payroll.stg_payroll;
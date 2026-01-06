MODEL (
  name hospital.fct_hospital,
  kind FULL
);

SELECT
  hospital_name,
  service_description AS job_title, 
  city,
  state,
  total_patients,
  avg_payment_per_patient,

  -- Gunakan 'total_amount' sesuai kontrak kita
  (total_patients * avg_payment_per_patient) AS total_amount,

  processed_at
FROM hospital.stg_hospital;
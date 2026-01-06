MODEL (
  name hospital.stg_hospital,
  kind FULL
);

SELECT
  -- Karena pake normalize_names=True:
  -- 'Provider Name' -> provider_name
  -- 'DRG Definition' -> drg_definition
  -- ' Total Discharges ' -> total_discharges
  -- ' Average Total Payments ' -> average_total_payments
  
  provider_name AS hospital_name,
  provider_city AS city,
  provider_state AS state,
  drg_definition AS service_description,
  
  CAST(total_discharges AS INTEGER) AS total_patients,
  CAST(average_total_payments AS DOUBLE) AS avg_payment_per_patient,
  CAST(average_medicare_payments AS DOUBLE) AS medicare_coverage,
  
  CURRENT_TIMESTAMP AS processed_at

-- JURUS SAKTI: normalize_names=True
FROM read_csv(@client_raw_path, 
    header=True, 
    auto_detect=True, 
    normalize_names=True, 
    quote='"', 
    all_varchar=True);
%sql

SELECT
  DATE_TRUNC('MONTH', date_service) AS Month,
  ROUND(sum(line_charge), 2) AS "Total Paid",
  ROUND(sum(line_allowed), 2) AS "Total Allowed",
  ROUND(SUM(line_charge)/approx_count_distinct(patient_id), 2) AS "PMPM"
FROM health_claims
GROUP BY "MONTH"
ORDER BY "MONTH" DESC
LIMIT 12;


SELECT ROUND(SUM(line_charge)/approx_count_distinct(patient_id), 2) AS "Paid PMPM",
ROUND(sum(line_allowed)/approx_count_distinct(patient_id), 2) AS "Allowed PMPM"

FROM health_claims;

WITH base AS (
  SELECT
    LOCATION_OF_CARE,
    line_charge,
    claim_id,
    AVG(line_charge) OVER ()     AS avg_charge,
    STDDEV(line_charge) OVER ()  AS stddev_charge
  FROM health_claims
)
SELECT
  LOCATION_OF_CARE AS "Location of Care",
  ROUND(SUM(line_charge), 2) AS "Paid Amount",
  APPROX_COUNT_DISTINCT(claim_id) AS "Claims"
FROM base
WHERE line_charge > avg_charge + 3 * stddev_charge
GROUP BY LOCATION_OF_CARE
ORDER BY "Paid Amount" DESC;

WITH base AS (
  SELECT
    DATE_TRUNC('MONTH', date_service) AS month,
    icd10cm_code_description          AS condition,
    patient_id,
    line_charge
  FROM health_claims
  WHERE icd10cm_code_description IS NOT NULL
)
SELECT
  month AS "Month",
  condition AS "Condition",
  APPROX_COUNT_DISTINCT(patient_id) AS "Patients",
  ROUND(SUM(line_charge), 2) AS "Total Paid",
  ROUND(SUM(line_charge) / NULLIF(APPROX_COUNT_DISTINCT(patient_id), 0), 2) AS "Paid per Patient"
FROM base
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY month ORDER BY SUM(line_charge) DESC) <= 10
ORDER BY "Month" DESC, "Total Paid" DESC;

WITH patient_spend AS (
  SELECT
    patient_id,
    ROUND(SUM(line_charge), 2) AS total_paid
  FROM health_claims
  GROUP BY 1
),
ranked AS (
  SELECT
    patient_id,
    total_paid,
    SUM(total_paid) OVER () AS overall_paid,
    SUM(total_paid) OVER (ORDER BY total_paid DESC) AS running_paid,
    ROW_NUMBER() OVER (ORDER BY total_paid DESC) AS spend_rank
  FROM patient_spend
)
SELECT
  spend_rank AS "Rank",
  patient_id AS "Patient ID",
  total_paid AS "Paid (12M)",
  ROUND(100 * total_paid / NULLIF(overall_paid, 0), 4) AS "Pct of Total Paid",
  ROUND(100 * running_paid / NULLIF(overall_paid, 0), 2) AS "Cumulative Pct of Total Paid"
FROM ranked
ORDER BY "Paid (12M)" DESC
LIMIT 100;

WITH preventive_cpts AS (
  SELECT column1 AS cpt
  FROM VALUES
    ('99381'), ('99382'), ('99383'), ('99384'), ('99385'),
    ('99386'), ('99387'), ('99391'), ('99392'), ('99393'),
    ('99394'), ('99395'), ('99396'), ('99397')  -- preventive medicine E/M
),
base AS (
  SELECT
    DATE_TRUNC('MONTH', date_service) AS month,
    patient_id,
    line_charge,
    CASE WHEN cpt IN (SELECT cpt FROM preventive_cpts)
         THEN 'Preventive'
         ELSE 'Non-Preventive'
    END AS category
  FROM health_claims
)
SELECT
  month AS "Month",
  category AS "Category",
  APPROX_COUNT_DISTINCT(patient_id) AS "Patients",
  ROUND(SUM(line_charge), 2) AS "Paid",
  ROUND(100 * SUM(line_charge) / NULLIF(SUM(SUM(line_charge)) OVER (PARTITION BY month), 0), 2) AS "Pct of Month Paid"
FROM base
GROUP BY 1, 2
ORDER BY "Month" DESC, "Paid" DESC;

WITH base AS (
  SELECT
    patient_zip3,
    patient_id,
    line_charge
  FROM health_claims
  WHERE date_service >= DATEADD(MONTH, -12, CURRENT_DATE())
    AND patient_zip3 IS NOT NULL
),
zip_rollup AS (
  SELECT
    patient_zip3,
    APPROX_COUNT_DISTINCT(patient_id) AS patients,
    ROUND(SUM(line_charge), 2) AS total_paid,
    ROUND(SUM(line_charge) / NULLIF(APPROX_COUNT_DISTINCT(patient_id), 0), 2) AS paid_per_patient
  FROM base
  GROUP BY 1
)
SELECT
  patient_zip3 AS "ZIP",
  patients AS "Patients",
  total_paid AS "Total Paid (12M)",
  paid_per_patient AS "Paid per Patient (12M)"
FROM zip_rollup
WHERE patients >= 50
ORDER BY "Paid per Patient (12M)" DESC
LIMIT 50;

WITH base AS (
  SELECT
    billing_npi,
    line_charge,
    patient_id,
    claim_id
  FROM health_claims
  WHERE billing_npi IS NOT NULL
),
npi_stats AS (
  SELECT
    billing_npi,
    APPROX_COUNT_DISTINCT(patient_id) AS patients,
    APPROX_COUNT_DISTINCT(claim_id)   AS claims,
    ROUND(AVG(line_charge), 2)        AS avg_paid_per_claim,
    ROUND(MEDIAN(line_charge), 2)     AS median_paid_per_claim,
    ROUND(SUM(line_charge), 2)        AS total_paid
  FROM base
  GROUP BY 1
)
SELECT
  billing_npi AS "Billing NPI",
  patients AS "Patients",
  claims AS "Claims",
  total_paid AS "Total Paid",
  avg_paid_per_claim AS "Avg Paid/Claim",
  median_paid_per_claim AS "Median Paid/Claim",
  ROUND(avg_paid_per_claim / NULLIF(median_paid_per_claim, 0), 2) AS "Mean-to-Median Ratio"
FROM npi_stats
WHERE claims >= 100
ORDER BY "Mean-to-Median Ratio" DESC
LIMIT 50;

WITH base AS (
  SELECT
    patient_id,
    diagnosis_code,
    icd10cm_code_description,
    date_service,
    line_charge
  FROM health_claims
  WHERE diagnosis_code IS NOT NULL
),
patient_dx AS (
  SELECT
    patient_id,
    APPROX_COUNT_DISTINCT(diagnosis_code) AS distinct_dx_codes,
    ROUND(SUM(line_charge), 2)            AS total_paid
  FROM base
  GROUP BY 1
),
bucketed AS (
  SELECT
    CASE
      WHEN distinct_dx_codes <= 2 THEN '0-2 DX'
      WHEN distinct_dx_codes <= 5 THEN '3-5 DX'
      WHEN distinct_dx_codes <= 10 THEN '6-10 DX'
      ELSE '11+ DX'
    END AS dx_burden_bucket,
    patient_id,
    total_paid
  FROM patient_dx
)
SELECT
  dx_burden_bucket AS "DX Burden Bucket",
  APPROX_COUNT_DISTINCT(patient_id) AS "Patients",
  ROUND(SUM(total_paid), 2) AS "Total Paid (12M)",
  ROUND(AVG(total_paid), 2) AS "Avg Paid per Patient (12M)"
FROM bucketed
GROUP BY 1
ORDER BY
  CASE dx_burden_bucket
    WHEN '0-2 DX' THEN 1
    WHEN '3-5 DX' THEN 2
    WHEN '6-10 DX' THEN 3
    ELSE 4
  END;

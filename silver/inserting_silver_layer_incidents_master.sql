

--2026-02-22T20:05:09.183+00:00
--Creation of the Silver layer 34columns on the bronze layer
--User:Rafael Monterrosa
--Detected this value unknown on attribution_confidence replaced it with null 
--2025-12-31	2021-01-01 acceptable bounderies  max(incident_date) min(incident_date)
--Validate discovery_date >= incident_date (if applicable) NO CASES 
--company_revenue_usd < 0 no values with this filter
--employee_count <= 0 no values with this filter
-- num_affected_rows	num_inserted_rows
-- 850	850

WITH base AS (
  SELECT
    trim(t.incident_id) as incident_id,
    upper(trim(t.company_name)) as company_name,
    cast(t.company_revenue_usd as double) as company_revenue_usd,
    upper(trim(t.country_hq)) as country_hq,
    trim(t.industry_primary) as industry_primary,
    trim(t.industry_secondary) as industry_secondary,
    cast(t.employee_count as int) as employee_count,
    cast(t.is_public_company as boolean) as is_public_company,
    upper(trim(t.stock_ticker)) as stock_ticker,

    cast(t.incident_date as date) as incident_date,
    cast(t.incident_date_estimated as boolean) as incident_date_estimated,
    cast(t.discovery_date as date) as discovery_date,
    cast(t.disclosure_date as date) as disclosure_date,

    lower(trim(t.attack_vector_primary)) as attack_vector_primary,
    lower(trim(t.attack_vector_secondary)) as attack_vector_secondary,
    trim(t.attack_chain) as attack_chain,
    upper(trim(t.attributed_group)) as attributed_group,

    CASE
      WHEN t.attribution_confidence IS NULL THEN NULL
      WHEN lower(trim(t.attribution_confidence)) = 'unknown' THEN NULL
      ELSE lower(trim(t.attribution_confidence))
    END AS attribution_confidence,

    cast(t.data_compromised_records as bigint) as data_compromised_records,
    upper(trim(t.data_type)) as data_type,
    lower(trim(t.systems_affected)) as systems_affected,
    cast(t.downtime_hours as double) as downtime_hours,

    trim(t.data_source_primary) as data_source_primary,
    trim(t.data_source_secondary) as data_source_secondary,
    lower(trim(t.data_source_type)) as data_source_type,

    cast(t.confidence_tier as int) as confidence_tier,
    cast(t.quality_score as double) as quality_score,
    upper(trim(t.quality_grade)) as quality_grade,
    lower(trim(t.review_flag)) as review_flag,
    trim(t.notes) as notes,

    cast(t.created_at as timestamp) as created_at_bronze,
    cast(t.updated_at as timestamp) as updated_at_bronze,
    cast(t.ingestion_timestamp as timestamp) as ingestion_timestamp_bronze,
    lower(trim(t.source_file)) as source_file
  FROM workspace.bronze.incidents_master_raw t
),

silver_layer AS (
  SELECT
    b.*,

    -- Date logic DQ
    CASE
      WHEN b.incident_date IS NULL THEN 'INVALID_INCIDENT_DATE'
      WHEN b.discovery_date IS NOT NULL AND b.discovery_date < b.incident_date THEN 'DISCOVERY_BEFORE_INCIDENT'
      WHEN b.disclosure_date IS NOT NULL AND b.discovery_date IS NOT NULL AND b.disclosure_date < b.discovery_date THEN 'DISCLOSURE_BEFORE_DISCOVERY'
      WHEN b.incident_date > CURRENT_DATE() THEN 'INCIDENT_IN_FUTURE'
      ELSE 'OK'
    END AS date_quality_flag,

    -- DQ score: count of issues across key rules
    (
      -- incident_id format
      CASE WHEN b.incident_id RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{3}$' THEN 0 ELSE 1 END

      -- country format (ISO2 or known mappable)
      + CASE
          WHEN b.country_hq IS NULL THEN 1
          WHEN b.country_hq RLIKE '^[A-Z]{2}$' THEN 0
          WHEN b.country_hq IN ('USA','UNITED STATES') THEN 0
          ELSE 1
        END

      -- industry_primary format (NN or NN-NN)
      + CASE
          WHEN b.industry_primary IS NULL THEN 1
          WHEN b.industry_primary RLIKE '^[0-9]{2}(-[0-9]{2})?$' THEN 0
          ELSE 1
        END

      -- revenue sanity
      + CASE
          WHEN b.company_revenue_usd IS NULL THEN 1
          WHEN b.company_revenue_usd < 0 THEN 1
          WHEN b.company_revenue_usd > 1e13 THEN 1
          ELSE 0
        END

      -- employee_count sanity
      + CASE
          WHEN b.employee_count IS NULL THEN 1
          WHEN b.employee_count < 0 THEN 1
          WHEN b.employee_count > 10000000 THEN 1
          ELSE 0
        END

      -- downtime sanity (NULL allowed)
      + CASE
          WHEN b.downtime_hours IS NULL THEN 0
          WHEN b.downtime_hours < 0 THEN 1
          WHEN b.downtime_hours > 8760 THEN 1
          ELSE 0
        END

      -- attribution_confidence domain
      + CASE
          WHEN b.attribution_confidence IS NULL THEN 0
          WHEN b.attribution_confidence IN ('confirmed','probable','suspected') THEN 0
          ELSE 1
        END

      -- confidence_tier domain
      + CASE
          WHEN b.confidence_tier IS NULL THEN 1
          WHEN b.confidence_tier IN (1,2,3,4,5) THEN 0
          ELSE 1
        END

      -- quality_grade domain (optional but useful)
      + CASE
          WHEN b.quality_grade IS NULL THEN 1
          WHEN b.quality_grade IN ('GOLD','SILVER','BRONZE') THEN 0
          ELSE 1
        END

      -- is_public_company sanity (optional)
      + CASE
          WHEN b.is_public_company IS NULL THEN 1
          ELSE 0
        END
    ) AS dq_issue_count,

    current_timestamp() as ingestion_timestamp_silver,

    -- Deterministic dedupe winner
    row_number() OVER (
      PARTITION BY b.incident_id
      ORDER BY
        b.quality_score DESC,
        b.updated_at_bronze DESC,
        b.ingestion_timestamp_bronze DESC
    ) AS dedupe_rank

  FROM base b
),

winners AS (
  SELECT
    -- you can exclude dedupe_rank if you don't want to store it
    incident_id, company_name, company_revenue_usd, country_hq, industry_primary, industry_secondary,
    employee_count, is_public_company, stock_ticker, incident_date, incident_date_estimated,
    discovery_date, disclosure_date, attack_vector_primary, attack_vector_secondary, attack_chain,
    attributed_group, attribution_confidence, data_compromised_records, data_type, systems_affected,
    downtime_hours, data_source_primary, data_source_secondary, data_source_type, confidence_tier,
    quality_score, quality_grade, review_flag, notes, created_at_bronze, updated_at_bronze,
    ingestion_timestamp_bronze, source_file, date_quality_flag, dq_issue_count, ingestion_timestamp_silver
  FROM silver_layer
  WHERE dedupe_rank = 1
)

MERGE INTO workspace.silver.incidents_master s
USING winners w
ON s.incident_id = w.incident_id

WHEN MATCHED THEN UPDATE SET
  s.company_name = w.company_name,
  s.company_revenue_usd = w.company_revenue_usd,
  s.country_hq = w.country_hq,
  s.industry_primary = w.industry_primary,
  s.industry_secondary = w.industry_secondary,
  s.employee_count = w.employee_count,
  s.is_public_company = w.is_public_company,
  s.stock_ticker = w.stock_ticker,
  s.incident_date = w.incident_date,
  s.incident_date_estimated = w.incident_date_estimated,
  s.discovery_date = w.discovery_date,
  s.disclosure_date = w.disclosure_date,
  s.attack_vector_primary = w.attack_vector_primary,
  s.attack_vector_secondary = w.attack_vector_secondary,
  s.attack_chain = w.attack_chain,
  s.attributed_group = w.attributed_group,
  s.attribution_confidence = w.attribution_confidence,
  s.data_compromised_records = w.data_compromised_records,
  s.data_type = w.data_type,
  s.systems_affected = w.systems_affected,
  s.downtime_hours = w.downtime_hours,
  s.data_source_primary = w.data_source_primary,
  s.data_source_secondary = w.data_source_secondary,
  s.data_source_type = w.data_source_type,
  s.confidence_tier = w.confidence_tier,
  s.quality_score = w.quality_score,
  s.quality_grade = w.quality_grade,
  s.review_flag = w.review_flag,
  s.notes = w.notes,
  s.created_at_bronze = w.created_at_bronze,
  s.updated_at_bronze = w.updated_at_bronze,
  s.ingestion_timestamp_bronze = w.ingestion_timestamp_bronze,
  s.source_file = w.source_file,
  s.date_quality_flag = w.date_quality_flag,
  s.dq_issue_count = w.dq_issue_count,
  s.ingestion_timestamp_silver = w.ingestion_timestamp_silver

WHEN NOT MATCHED THEN INSERT (
  incident_id, company_name, company_revenue_usd, country_hq, industry_primary, industry_secondary,
  employee_count, is_public_company, stock_ticker, incident_date, incident_date_estimated,
  discovery_date, disclosure_date, attack_vector_primary, attack_vector_secondary, attack_chain,
  attributed_group, attribution_confidence, data_compromised_records, data_type, systems_affected,
  downtime_hours, data_source_primary, data_source_secondary, data_source_type, confidence_tier,
  quality_score, quality_grade, review_flag, notes, created_at_bronze, updated_at_bronze,
  ingestion_timestamp_bronze, source_file, date_quality_flag, dq_issue_count, ingestion_timestamp_silver
) VALUES (
  w.incident_id, w.company_name, w.company_revenue_usd, w.country_hq, w.industry_primary, w.industry_secondary,
  w.employee_count, w.is_public_company, w.stock_ticker, w.incident_date, w.incident_date_estimated,
  w.discovery_date, w.disclosure_date, w.attack_vector_primary, w.attack_vector_secondary, w.attack_chain,
  w.attributed_group, w.attribution_confidence, w.data_compromised_records, w.data_type, w.systems_affected,
  w.downtime_hours, w.data_source_primary, w.data_source_secondary, w.data_source_type, w.confidence_tier,
  w.quality_score, w.quality_grade, w.review_flag, w.notes, w.created_at_bronze, w.updated_at_bronze,
  w.ingestion_timestamp_bronze, w.source_file, w.date_quality_flag, w.dq_issue_count, w.ingestion_timestamp_silver
);

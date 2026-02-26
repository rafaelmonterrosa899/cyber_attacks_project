--2026-02-26T00:00:00.000+00:00
--Creation of the Silver layer (financial impact)
--User: Rafael Monterrosa
--DQ rules: incident_id not null, CPI parsing, loss range sanity
--Dedup rule: winner per incident_id by ingestion_timestamp_bronze, updated_at_bronze, created_at_bronze, completeness_score, source_file
--num_affected_rows  num_inserted_rows -> validate after run

WITH base AS (
  SELECT
    -- ===============================
    -- Keys / Strings
    -- ===============================
    CASE
      WHEN UPPER(TRIM(t.incident_id)) IN ('', 'NULL', 'N/A') THEN NULL
      ELSE UPPER(TRIM(t.incident_id))
    END AS incident_id,

    CASE
      WHEN UPPER(TRIM(t.direct_loss_method)) IN ('', 'NULL', 'N/A') THEN NULL
      ELSE UPPER(TRIM(t.direct_loss_method))
    END AS direct_loss_method,

    CASE
      WHEN UPPER(TRIM(t.total_loss_method)) IN ('', 'NULL', 'N/A') THEN NULL
      ELSE UPPER(TRIM(t.total_loss_method))
    END AS total_loss_method,

    NULLIF(TRIM(t.ransom_source), '') AS ransom_source,
    NULLIF(TRIM(t.notes), '') AS notes,
    LOWER(TRIM(t.source_file)) AS source_file,

    -- ===============================
    -- Monetary casts
    -- ===============================
    TRY_CAST(TRIM(t.direct_loss_usd) AS DECIMAL(18,2)) AS direct_loss_usd,
    TRY_CAST(TRIM(t.ransom_demanded_usd) AS DECIMAL(18,2)) AS ransom_demanded_usd,
    TRY_CAST(TRIM(t.ransom_paid_usd) AS DECIMAL(18,2)) AS ransom_paid_usd,
    TRY_CAST(TRIM(t.recovery_cost_usd) AS DECIMAL(18,2)) AS recovery_cost_usd,
    TRY_CAST(TRIM(t.legal_fees_usd) AS DECIMAL(18,2)) AS legal_fees_usd,
    TRY_CAST(TRIM(t.regulatory_fine_usd) AS DECIMAL(18,2)) AS regulatory_fine_usd,
    TRY_CAST(TRIM(t.insurance_payout_usd) AS DECIMAL(18,2)) AS insurance_payout_usd,

    TRY_CAST(TRIM(t.total_loss_usd) AS DECIMAL(18,2)) AS total_loss_usd,
    TRY_CAST(TRIM(t.total_loss_lower_bound) AS DECIMAL(18,2)) AS total_loss_lower_bound,
    TRY_CAST(TRIM(t.total_loss_upper_bound) AS DECIMAL(18,2)) AS total_loss_upper_bound,
    TRY_CAST(TRIM(t.inflation_adjusted_usd) AS DECIMAL(18,2)) AS inflation_adjusted_usd,

    -- ===============================
    -- CPI parsing (Spark)
    -- ===============================
    TRY_CAST(REGEXP_EXTRACT(t.cpi_index_used, '(\\d{4})', 1) AS INT) AS cpi_year,
    TRY_CAST(REGEXP_EXTRACT(t.cpi_index_used, '\\(([^)]+)\\)', 1) AS DECIMAL(10,2)) AS cpi_value,

    -- ===============================
    -- Bronze lineage timestamps
    -- ===============================
    CAST(t.created_at AS TIMESTAMP) AS created_at_bronze,
    CAST(t.updated_at AS TIMESTAMP) AS updated_at_bronze,
    CAST(t.ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp_bronze

  FROM workspace.bronze.financial_impact_raw t
),

silver_layer AS (
  SELECT
    b.*,

    -- ===============================
    -- Financial derived fields
    -- ===============================
    CASE WHEN b.ransom_paid_usd > 0 THEN 1 ELSE 0 END AS ransom_paid_flag,
    CASE WHEN b.insurance_payout_usd > 0 THEN 1 ELSE 0 END AS insured_flag,
    CASE WHEN b.regulatory_fine_usd > 0 THEN 1 ELSE 0 END AS regulatory_penalty_flag,

    b.total_loss_usd - COALESCE(b.insurance_payout_usd, 0) AS net_loss_after_insurance,

    -- ===============================
    -- DQ flags (similar style)
    -- ===============================
    CASE
      WHEN b.incident_id IS NULL THEN 'MISSING_INCIDENT_ID'
      WHEN b.total_loss_lower_bound IS NOT NULL AND b.total_loss_upper_bound IS NOT NULL
           AND b.total_loss_lower_bound > b.total_loss_upper_bound THEN 'INVALID_LOSS_RANGE'
      WHEN b.cpi_year IS NULL AND b.cpi_value IS NOT NULL THEN 'CPI_YEAR_MISSING'
      WHEN b.cpi_value IS NULL AND b.cpi_year IS NOT NULL THEN 'CPI_VALUE_MISSING'
      ELSE 'OK'
    END AS financial_quality_flag,

    -- DQ score: count of issues across key rules
    (
      -- incident_id format (optional strictness)
      CASE WHEN b.incident_id RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{3}$' THEN 0 ELSE 1 END

      -- loss range sanity (NULL allowed)
      + CASE
          WHEN b.total_loss_lower_bound IS NULL OR b.total_loss_upper_bound IS NULL THEN 0
          WHEN b.total_loss_lower_bound <= b.total_loss_upper_bound THEN 0
          ELSE 1
        END

      -- monetary sanity checks (NULL allowed)
      + CASE WHEN b.direct_loss_usd        IS NULL THEN 0 WHEN b.direct_loss_usd        < 0 THEN 1 ELSE 0 END
      + CASE WHEN b.recovery_cost_usd      IS NULL THEN 0 WHEN b.recovery_cost_usd      < 0 THEN 1 ELSE 0 END
      + CASE WHEN b.legal_fees_usd         IS NULL THEN 0 WHEN b.legal_fees_usd         < 0 THEN 1 ELSE 0 END
      + CASE WHEN b.regulatory_fine_usd    IS NULL THEN 0 WHEN b.regulatory_fine_usd    < 0 THEN 1 ELSE 0 END
      + CASE WHEN b.insurance_payout_usd   IS NULL THEN 0 WHEN b.insurance_payout_usd   < 0 THEN 1 ELSE 0 END
      + CASE WHEN b.total_loss_usd         IS NULL THEN 1 WHEN b.total_loss_usd         < 0 THEN 1 ELSE 0 END

      -- CPI parsing sanity (NULL allowed but if one present, both should be present)
      + CASE
          WHEN b.cpi_year IS NULL AND b.cpi_value IS NULL THEN 0
          WHEN b.cpi_year IS NOT NULL AND b.cpi_value IS NOT NULL THEN 0
          ELSE 1
        END
    ) AS dq_issue_count,

    current_timestamp() AS ingestion_timestamp_silver,

    -- ===============================
    -- Completeness score (tie-breaker)
    -- ===============================
    (
      CASE WHEN b.direct_loss_usd        IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.ransom_demanded_usd    IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.ransom_paid_usd        IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.recovery_cost_usd      IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.legal_fees_usd         IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.regulatory_fine_usd    IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.insurance_payout_usd   IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.total_loss_usd         IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.total_loss_lower_bound IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.total_loss_upper_bound IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN b.inflation_adjusted_usd IS NOT NULL THEN 1 ELSE 0 END
    ) AS completeness_score,

    -- Deterministic dedupe winner (NO lateral alias usage)
    row_number() OVER (
      PARTITION BY b.incident_id
      ORDER BY
        b.ingestion_timestamp_bronze DESC,
        b.updated_at_bronze DESC,
        b.created_at_bronze DESC,
        (
          CASE WHEN b.direct_loss_usd        IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.ransom_demanded_usd    IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.ransom_paid_usd        IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.recovery_cost_usd      IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.legal_fees_usd         IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.regulatory_fine_usd    IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.insurance_payout_usd   IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.total_loss_usd         IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.total_loss_lower_bound IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.total_loss_upper_bound IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN b.inflation_adjusted_usd IS NOT NULL THEN 1 ELSE 0 END
        ) DESC,
        b.source_file ASC
    ) AS dedupe_rank

  FROM base b
  WHERE b.incident_id IS NOT NULL
),

winners AS (
  SELECT
    incident_id,
    direct_loss_method,
    total_loss_method,
    ransom_source,
    notes,
    source_file,

    direct_loss_usd,
    ransom_demanded_usd,
    ransom_paid_usd,
    recovery_cost_usd,
    legal_fees_usd,
    regulatory_fine_usd,
    insurance_payout_usd,

    total_loss_usd,
    total_loss_lower_bound,
    total_loss_upper_bound,
    inflation_adjusted_usd,

    cpi_year,
    cpi_value,

    created_at_bronze,
    updated_at_bronze,
    ingestion_timestamp_bronze,

    ransom_paid_flag,
    insured_flag,
    regulatory_penalty_flag,

    net_loss_after_insurance,
    financial_quality_flag,
    dq_issue_count,
    ingestion_timestamp_silver
  FROM silver_layer
  WHERE dedupe_rank = 1
)

MERGE INTO workspace.silver.financial_impact s
USING winners w
ON s.incident_id = w.incident_id

WHEN MATCHED THEN UPDATE SET
  s.direct_loss_method          = w.direct_loss_method,
  s.total_loss_method           = w.total_loss_method,
  s.ransom_source               = w.ransom_source,
  s.notes                       = w.notes,
  s.source_file                 = w.source_file,

  s.direct_loss_usd             = w.direct_loss_usd,
  s.ransom_demanded_usd         = w.ransom_demanded_usd,
  s.ransom_paid_usd             = w.ransom_paid_usd,
  s.recovery_cost_usd           = w.recovery_cost_usd,
  s.legal_fees_usd              = w.legal_fees_usd,
  s.regulatory_fine_usd         = w.regulatory_fine_usd,
  s.insurance_payout_usd        = w.insurance_payout_usd,

  s.total_loss_usd              = w.total_loss_usd,
  s.total_loss_lower_bound      = w.total_loss_lower_bound,
  s.total_loss_upper_bound      = w.total_loss_upper_bound,
  s.inflation_adjusted_usd      = w.inflation_adjusted_usd,

  s.cpi_year                    = w.cpi_year,
  s.cpi_value                   = w.cpi_value,

  s.created_at_bronze           = w.created_at_bronze,
  s.updated_at_bronze           = w.updated_at_bronze,
  s.ingestion_timestamp_bronze  = w.ingestion_timestamp_bronze,

  s.ransom_paid_flag            = w.ransom_paid_flag,
  s.insured_flag                = w.insured_flag,
  s.regulatory_penalty_flag     = w.regulatory_penalty_flag,

  s.net_loss_after_insurance    = w.net_loss_after_insurance,

  s.financial_quality_flag      = w.financial_quality_flag,
  s.dq_issue_count              = w.dq_issue_count,
  s.ingestion_timestamp_silver  = w.ingestion_timestamp_silver

WHEN NOT MATCHED THEN INSERT (
  incident_id,
  direct_loss_method,
  total_loss_method,
  ransom_source,
  notes,
  source_file,

  direct_loss_usd,
  ransom_demanded_usd,
  ransom_paid_usd,
  recovery_cost_usd,
  legal_fees_usd,
  regulatory_fine_usd,
  insurance_payout_usd,

  total_loss_usd,
  total_loss_lower_bound,
  total_loss_upper_bound,
  inflation_adjusted_usd,

  cpi_year,
  cpi_value,

  created_at_bronze,
  updated_at_bronze,
  ingestion_timestamp_bronze,

  ransom_paid_flag,
  insured_flag,
  regulatory_penalty_flag,

  net_loss_after_insurance,

  financial_quality_flag,
  dq_issue_count,
  ingestion_timestamp_silver
) VALUES (
  w.incident_id,
  w.direct_loss_method,
  w.total_loss_method,
  w.ransom_source,
  w.notes,
  w.source_file,

  w.direct_loss_usd,
  w.ransom_demanded_usd,
  w.ransom_paid_usd,
  w.recovery_cost_usd,
  w.legal_fees_usd,
  w.regulatory_fine_usd,
  w.insurance_payout_usd,

  w.total_loss_usd,
  w.total_loss_lower_bound,
  w.total_loss_upper_bound,
  w.inflation_adjusted_usd,

  w.cpi_year,
  w.cpi_value,

  w.created_at_bronze,
  w.updated_at_bronze,
  w.ingestion_timestamp_bronze,

  w.ransom_paid_flag,
  w.insured_flag,
  w.regulatory_penalty_flag,

  w.net_loss_after_insurance,

  w.financial_quality_flag,
  w.dq_issue_count,
  w.ingestion_timestamp_silver
);

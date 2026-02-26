DROP TABLE IF EXISTS workspace.silver.financial_impact;

CREATE TABLE workspace.silver.financial_impact (
  incident_id               STRING,
  direct_loss_method        STRING,
  total_loss_method         STRING,
  ransom_source             STRING,
  notes                     STRING,
  source_file               STRING,

  direct_loss_usd           DECIMAL(18,2),
  ransom_demanded_usd       DECIMAL(18,2),
  ransom_paid_usd           DECIMAL(18,2),
  recovery_cost_usd         DECIMAL(18,2),
  legal_fees_usd            DECIMAL(18,2),
  regulatory_fine_usd       DECIMAL(18,2),
  insurance_payout_usd      DECIMAL(18,2),

  total_loss_usd            DECIMAL(18,2),
  total_loss_lower_bound    DECIMAL(18,2),
  total_loss_upper_bound    DECIMAL(18,2),
  inflation_adjusted_usd    DECIMAL(18,2),

  cpi_year                  INT,
  cpi_value                 DECIMAL(10,2),

  -- ✅ Bronze lineage (same naming convention as previous silver)
  created_at_bronze                TIMESTAMP,
  updated_at_bronze                TIMESTAMP,
  ingestion_timestamp_bronze        TIMESTAMP,

  ransom_paid_flag          INT,
  insured_flag              INT,
  regulatory_penalty_flag   INT,

  net_loss_after_insurance  DECIMAL(18,2),
  loss_range_valid_flag     INT,
  data_quality_flag         INT,

  -- ✅ Silver processed timestamp
  ingestion_timestamp_silver       TIMESTAMP
)
USING DELTA;

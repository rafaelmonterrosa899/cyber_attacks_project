
CREATE TABLE IF NOT EXISTS workspace.silver.market_impact
(
  -- ================= BUSINESS KEYS =================
  incident_id STRING,
  stock_ticker STRING,

  -- ================= PRICES =================
  price_7d_before DECIMAL(18,2),
  price_disclosure_day DECIMAL(18,2),
  price_1d_after DECIMAL(18,2),
  price_7d_after DECIMAL(18,2),
  price_30d_after DECIMAL(18,2),

  -- ================= VOLUMES =================
  volume_avg_30d_baseline BIGINT,
  volume_disclosure_day BIGINT,

  -- ================= SECTOR =================
  sector_index STRING,
  sector_return_same_period DECIMAL(18,6),

  -- ================= ABNORMAL RETURNS =================
  abnormal_return_1d DECIMAL(18,6),
  abnormal_return_7d DECIMAL(18,6),
  abnormal_return_30d DECIMAL(18,6),

  -- ================= CAR =================
  car_neg1_to_pos1 DECIMAL(18,6),
  car_0_to_7 DECIMAL(18,6),
  car_0_to_30 DECIMAL(18,6),
  car_0_to_90 DECIMAL(18,6),

  -- ================= STATS =================
  t_statistic_1d DECIMAL(18,6),
  p_value_1d DECIMAL(18,6),
  t_statistic_30d DECIMAL(18,6),
  p_value_30d DECIMAL(18,6),

  -- ================= BOOLEAN =================
  earnings_announcement_within_7d BOOLEAN,

  -- ================= MARKET =================
  market_cap_at_disclosure DECIMAL(20,2),
  volume_ratio_disclosure DECIMAL(18,6),
  pre_incident_volatility_30d DECIMAL(18,6),
  post_incident_volatility_30d DECIMAL(18,6),

  -- ================= RECOVERY =================
  days_to_price_recovery INT,

  -- ================= NOTES =================
  notes STRING,

  -- ================= TIMESTAMPS =================
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  bronze_ingestion_timestamp TIMESTAMP,
  silver_ingestion_timestamp TIMESTAMP,

  -- ================= SOURCE =================
  source_file STRING,

  -- ================= DQ FLAGS =================
  dq_invalid_incident_id INT,
  dq_invalid_stock_ticker INT,
  dq_invalid_price_7d_before INT,
  dq_invalid_price_disclosure_day INT,
  dq_invalid_price_1d_after INT,
  dq_invalid_price_7d_after INT,
  dq_invalid_price_30d_after INT,
  dq_invalid_volume_avg_30d_baseline INT,
  dq_invalid_volume_disclosure_day INT,
  dq_invalid_p_value_1d INT,
  dq_invalid_p_value_30d INT,
  dq_invalid_volume_ratio_disclosure INT,
  dq_invalid_pre_volatility_30d INT,
  dq_invalid_post_volatility_30d INT,
  dq_invalid_days_to_price_recovery INT,
  dq_invalid_updated_before_created INT,
  dq_invalid_boolean_token INT,
  dq_missing_bronze_ingestion_timestamp INT,
  dq_has_issues INT
)
USING DELTA;

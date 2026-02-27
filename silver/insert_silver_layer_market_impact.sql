/* ============================================================
   DATABRICKS / DELTA - IDPOTENT MERGE (UPSERT)
   Rule:
     - Match key: incident_id  (1 row per incident_id in Silver)
     - UPDATE only when src.bronze_ingestion_timestamp > tgt.bronze_ingestion_timestamp
     - INSERT when not matched
     - Always sets tgt.silver_ingestion_timestamp = current_timestamp() on insert/update
   ============================================================ */

MERGE INTO workspace.silver.market_impact AS tgt
USING (

  /* ===== Source = typed + dq + deterministic dedup (1 per incident_id) ===== */
  WITH typed AS (
    SELECT
      NULLIF(REGEXP_REPLACE(LOWER(TRIM(CAST(incident_id  AS STRING))), '^(null|n/a|na)$', ''), '') AS incident_id,
      NULLIF(REGEXP_REPLACE(LOWER(TRIM(CAST(stock_ticker AS STRING))), '^(null|n/a|na)$', ''), '') AS stock_ticker,

      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(price_7d_before AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,2)) AS price_7d_before,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(price_disclosure_day AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,2)) AS price_disclosure_day,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(price_1d_after AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,2)) AS price_1d_after,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(price_7d_after AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,2)) AS price_7d_after,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(price_30d_after AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,2)) AS price_30d_after,

      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(volume_avg_30d_baseline AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS BIGINT) AS volume_avg_30d_baseline,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(volume_disclosure_day AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS BIGINT) AS volume_disclosure_day,

      NULLIF(REGEXP_REPLACE(TRIM(CAST(sector_index AS STRING)), '^(?i)(null|n/a|na)$', ''), '') AS sector_index,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(sector_return_same_period AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS sector_return_same_period,

      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(abnormal_return_1d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS abnormal_return_1d,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(abnormal_return_7d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS abnormal_return_7d,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(abnormal_return_30d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS abnormal_return_30d,

      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(car_neg1_to_pos1 AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS car_neg1_to_pos1,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(car_0_to_7 AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS car_0_to_7,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(car_0_to_30 AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS car_0_to_30,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(car_0_to_90 AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS car_0_to_90,

      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(t_statistic_1d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS t_statistic_1d,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(p_value_1d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS p_value_1d,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(t_statistic_30d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS t_statistic_30d,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(p_value_30d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS p_value_30d,

      CASE
        WHEN NULLIF(REGEXP_REPLACE(LOWER(TRIM(CAST(earnings_announcement_within_7d AS STRING))), '^(null|n/a|na)$', ''), '') IN ('true','1','yes','y','t')  THEN TRUE
        WHEN NULLIF(REGEXP_REPLACE(LOWER(TRIM(CAST(earnings_announcement_within_7d AS STRING))), '^(null|n/a|na)$', ''), '') IN ('false','0','no','n','f') THEN FALSE
        ELSE NULL
      END AS earnings_announcement_within_7d,

      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(market_cap_at_disclosure AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(20,2)) AS market_cap_at_disclosure,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(volume_ratio_disclosure AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS volume_ratio_disclosure,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(pre_incident_volatility_30d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS pre_incident_volatility_30d,
      TRY_CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(CAST(post_incident_volatility_30d AS STRING))), '^(null|n/a|na)$', ''), ',', ''), '') AS DECIMAL(18,6)) AS post_incident_volatility_30d,

      TRY_CAST(NULLIF(REGEXP_REPLACE(LOWER(TRIM(CAST(days_to_price_recovery AS STRING))), '^(null|n/a|na)$', ''), '') AS INT) AS days_to_price_recovery,
      NULLIF(REGEXP_REPLACE(TRIM(CAST(notes AS STRING)), '^(?i)(null|n/a|na)$', ''), '') AS notes,

      TRY_TO_TIMESTAMP(NULLIF(REGEXP_REPLACE(LOWER(TRIM(REPLACE(CAST(created_at AS STRING), 'T', ' '))), '^(null|n/a|na)$', ''), '')) AS created_at,
      TRY_TO_TIMESTAMP(NULLIF(REGEXP_REPLACE(LOWER(TRIM(REPLACE(CAST(updated_at AS STRING), 'T', ' '))), '^(null|n/a|na)$', ''), '')) AS updated_at,
      TRY_TO_TIMESTAMP(NULLIF(REGEXP_REPLACE(LOWER(TRIM(REPLACE(CAST(ingestion_timestamp AS STRING), 'T', ' '))), '^(null|n/a|na)$', ''), '')) AS bronze_ingestion_timestamp,

      NULLIF(REGEXP_REPLACE(TRIM(CAST(source_file AS STRING)), '^(?i)(null|n/a|na)$', ''), '') AS source_file,

      TRIM(CAST(earnings_announcement_within_7d AS STRING)) AS _raw_earnings_announcement_within_7d
    FROM workspace.bronze.market_impact_raw
  ),

  dq AS (
    SELECT
      t.*,

      CASE WHEN incident_id IS NULL THEN 1 ELSE 0 END AS dq_invalid_incident_id,

      CASE
        WHEN stock_ticker IS NULL THEN 1
        WHEN NOT (UPPER(stock_ticker) RLIKE '^[A-Z0-9\\.-]{1,10}$') THEN 1
        ELSE 0
      END AS dq_invalid_stock_ticker,

      CASE WHEN price_7d_before      IS NOT NULL AND price_7d_before      < 0 THEN 1 ELSE 0 END AS dq_invalid_price_7d_before,
      CASE WHEN price_disclosure_day IS NOT NULL AND price_disclosure_day < 0 THEN 1 ELSE 0 END AS dq_invalid_price_disclosure_day,
      CASE WHEN price_1d_after       IS NOT NULL AND price_1d_after       < 0 THEN 1 ELSE 0 END AS dq_invalid_price_1d_after,
      CASE WHEN price_7d_after       IS NOT NULL AND price_7d_after       < 0 THEN 1 ELSE 0 END AS dq_invalid_price_7d_after,
      CASE WHEN price_30d_after      IS NOT NULL AND price_30d_after      < 0 THEN 1 ELSE 0 END AS dq_invalid_price_30d_after,

      CASE WHEN volume_avg_30d_baseline IS NOT NULL AND volume_avg_30d_baseline < 0 THEN 1 ELSE 0 END AS dq_invalid_volume_avg_30d_baseline,
      CASE WHEN volume_disclosure_day   IS NOT NULL AND volume_disclosure_day   < 0 THEN 1 ELSE 0 END AS dq_invalid_volume_disclosure_day,

      CASE WHEN p_value_1d  IS NOT NULL AND (p_value_1d  < 0 OR p_value_1d  > 1) THEN 1 ELSE 0 END AS dq_invalid_p_value_1d,
      CASE WHEN p_value_30d IS NOT NULL AND (p_value_30d < 0 OR p_value_30d > 1) THEN 1 ELSE 0 END AS dq_invalid_p_value_30d,

      CASE WHEN volume_ratio_disclosure IS NOT NULL AND volume_ratio_disclosure < 0 THEN 1 ELSE 0 END AS dq_invalid_volume_ratio_disclosure,

      CASE WHEN pre_incident_volatility_30d  IS NOT NULL AND pre_incident_volatility_30d  < 0 THEN 1 ELSE 0 END AS dq_invalid_pre_volatility_30d,
      CASE WHEN post_incident_volatility_30d IS NOT NULL AND post_incident_volatility_30d < 0 THEN 1 ELSE 0 END AS dq_invalid_post_volatility_30d,

      CASE WHEN days_to_price_recovery IS NOT NULL AND days_to_price_recovery < 0 THEN 1 ELSE 0 END AS dq_invalid_days_to_price_recovery,

      CASE WHEN created_at IS NOT NULL AND updated_at IS NOT NULL AND updated_at < created_at THEN 1 ELSE 0 END AS dq_invalid_updated_before_created,

      CASE
        WHEN _raw_earnings_announcement_within_7d IS NULL THEN 0
        WHEN LOWER(TRIM(_raw_earnings_announcement_within_7d)) IN ('', 'null', 'n/a', 'na') THEN 0
        WHEN earnings_announcement_within_7d IS NULL THEN 1
        ELSE 0
      END AS dq_invalid_boolean_token,

      CASE WHEN bronze_ingestion_timestamp IS NULL THEN 1 ELSE 0 END AS dq_missing_bronze_ingestion_timestamp,

      CASE
        WHEN
          (CASE WHEN incident_id IS NULL THEN 1 ELSE 0 END) = 1
          OR (CASE
                WHEN stock_ticker IS NULL THEN 1
                WHEN NOT (UPPER(stock_ticker) RLIKE '^[A-Z0-9\\.-]{1,10}$') THEN 1
                ELSE 0
              END) = 1
          OR (CASE WHEN p_value_1d  IS NOT NULL AND (p_value_1d  < 0 OR p_value_1d  > 1) THEN 1 ELSE 0 END) = 1
          OR (CASE WHEN p_value_30d IS NOT NULL AND (p_value_30d < 0 OR p_value_30d > 1) THEN 1 ELSE 0 END) = 1
          OR (CASE WHEN created_at IS NOT NULL AND updated_at IS NOT NULL AND updated_at < created_at THEN 1 ELSE 0 END) = 1
          OR (CASE
                WHEN _raw_earnings_announcement_within_7d IS NULL THEN 0
                WHEN LOWER(TRIM(_raw_earnings_announcement_within_7d)) IN ('', 'null', 'n/a', 'na') THEN 0
                WHEN earnings_announcement_within_7d IS NULL THEN 1
                ELSE 0
              END) = 1
        THEN 1 ELSE 0
      END AS dq_has_issues
    FROM typed t
  ),

  dedup AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY incident_id
        ORDER BY
          bronze_ingestion_timestamp DESC NULLS LAST,
          updated_at                 DESC NULLS LAST,
          created_at                 DESC NULLS LAST,
          CASE WHEN stock_ticker IS NOT NULL THEN 1 ELSE 0 END DESC,
          source_file                DESC NULLS LAST
      ) AS rn
    FROM dq
    WHERE incident_id IS NOT NULL
  )

  SELECT
    incident_id,
    stock_ticker,
    price_7d_before,
    price_disclosure_day,
    price_1d_after,
    price_7d_after,
    price_30d_after,
    volume_avg_30d_baseline,
    volume_disclosure_day,
    sector_index,
    sector_return_same_period,
    abnormal_return_1d,
    abnormal_return_7d,
    abnormal_return_30d,
    car_neg1_to_pos1,
    car_0_to_7,
    car_0_to_30,
    car_0_to_90,
    t_statistic_1d,
    p_value_1d,
    t_statistic_30d,
    p_value_30d,
    earnings_announcement_within_7d,
    market_cap_at_disclosure,
    volume_ratio_disclosure,
    pre_incident_volatility_30d,
    post_incident_volatility_30d,
    days_to_price_recovery,
    notes,
    created_at,
    updated_at,
    bronze_ingestion_timestamp,
    source_file,

    dq_invalid_incident_id,
    dq_invalid_stock_ticker,
    dq_invalid_price_7d_before,
    dq_invalid_price_disclosure_day,
    dq_invalid_price_1d_after,
    dq_invalid_price_7d_after,
    dq_invalid_price_30d_after,
    dq_invalid_volume_avg_30d_baseline,
    dq_invalid_volume_disclosure_day,
    dq_invalid_p_value_1d,
    dq_invalid_p_value_30d,
    dq_invalid_volume_ratio_disclosure,
    dq_invalid_pre_volatility_30d,
    dq_invalid_post_volatility_30d,
    dq_invalid_days_to_price_recovery,
    dq_invalid_updated_before_created,
    dq_invalid_boolean_token,
    dq_missing_bronze_ingestion_timestamp,
    dq_has_issues

  FROM dedup
  WHERE rn = 1

) AS src
ON tgt.incident_id = src.incident_id

WHEN MATCHED AND
     (
       tgt.bronze_ingestion_timestamp IS NULL
       OR src.bronze_ingestion_timestamp > tgt.bronze_ingestion_timestamp
     )
THEN UPDATE SET
  tgt.stock_ticker = src.stock_ticker,
  tgt.price_7d_before = src.price_7d_before,
  tgt.price_disclosure_day = src.price_disclosure_day,
  tgt.price_1d_after = src.price_1d_after,
  tgt.price_7d_after = src.price_7d_after,
  tgt.price_30d_after = src.price_30d_after,
  tgt.volume_avg_30d_baseline = src.volume_avg_30d_baseline,
  tgt.volume_disclosure_day = src.volume_disclosure_day,
  tgt.sector_index = src.sector_index,
  tgt.sector_return_same_period = src.sector_return_same_period,
  tgt.abnormal_return_1d = src.abnormal_return_1d,
  tgt.abnormal_return_7d = src.abnormal_return_7d,
  tgt.abnormal_return_30d = src.abnormal_return_30d,
  tgt.car_neg1_to_pos1 = src.car_neg1_to_pos1,
  tgt.car_0_to_7 = src.car_0_to_7,
  tgt.car_0_to_30 = src.car_0_to_30,
  tgt.car_0_to_90 = src.car_0_to_90,
  tgt.t_statistic_1d = src.t_statistic_1d,
  tgt.p_value_1d = src.p_value_1d,
  tgt.t_statistic_30d = src.t_statistic_30d,
  tgt.p_value_30d = src.p_value_30d,
  tgt.earnings_announcement_within_7d = src.earnings_announcement_within_7d,
  tgt.market_cap_at_disclosure = src.market_cap_at_disclosure,
  tgt.volume_ratio_disclosure = src.volume_ratio_disclosure,
  tgt.pre_incident_volatility_30d = src.pre_incident_volatility_30d,
  tgt.post_incident_volatility_30d = src.post_incident_volatility_30d,
  tgt.days_to_price_recovery = src.days_to_price_recovery,
  tgt.notes = src.notes,
  tgt.created_at = src.created_at,
  tgt.updated_at = src.updated_at,
  tgt.bronze_ingestion_timestamp = src.bronze_ingestion_timestamp,
  tgt.source_file = src.source_file,

  tgt.dq_invalid_incident_id = src.dq_invalid_incident_id,
  tgt.dq_invalid_stock_ticker = src.dq_invalid_stock_ticker,
  tgt.dq_invalid_price_7d_before = src.dq_invalid_price_7d_before,
  tgt.dq_invalid_price_disclosure_day = src.dq_invalid_price_disclosure_day,
  tgt.dq_invalid_price_1d_after = src.dq_invalid_price_1d_after,
  tgt.dq_invalid_price_7d_after = src.dq_invalid_price_7d_after,
  tgt.dq_invalid_price_30d_after = src.dq_invalid_price_30d_after,
  tgt.dq_invalid_volume_avg_30d_baseline = src.dq_invalid_volume_avg_30d_baseline,
  tgt.dq_invalid_volume_disclosure_day = src.dq_invalid_volume_disclosure_day,
  tgt.dq_invalid_p_value_1d = src.dq_invalid_p_value_1d,
  tgt.dq_invalid_p_value_30d = src.dq_invalid_p_value_30d,
  tgt.dq_invalid_volume_ratio_disclosure = src.dq_invalid_volume_ratio_disclosure,
  tgt.dq_invalid_pre_volatility_30d = src.dq_invalid_pre_volatility_30d,
  tgt.dq_invalid_post_volatility_30d = src.dq_invalid_post_volatility_30d,
  tgt.dq_invalid_days_to_price_recovery = src.dq_invalid_days_to_price_recovery,
  tgt.dq_invalid_updated_before_created = src.dq_invalid_updated_before_created,
  tgt.dq_invalid_boolean_token = src.dq_invalid_boolean_token,
  tgt.dq_missing_bronze_ingestion_timestamp = src.dq_missing_bronze_ingestion_timestamp,
  tgt.dq_has_issues = src.dq_has_issues,

  tgt.silver_ingestion_timestamp = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
  incident_id,
  stock_ticker,
  price_7d_before,
  price_disclosure_day,
  price_1d_after,
  price_7d_after,
  price_30d_after,
  volume_avg_30d_baseline,
  volume_disclosure_day,
  sector_index,
  sector_return_same_period,
  abnormal_return_1d,
  abnormal_return_7d,
  abnormal_return_30d,
  car_neg1_to_pos1,
  car_0_to_7,
  car_0_to_30,
  car_0_to_90,
  t_statistic_1d,
  p_value_1d,
  t_statistic_30d,
  p_value_30d,
  earnings_announcement_within_7d,
  market_cap_at_disclosure,
  volume_ratio_disclosure,
  pre_incident_volatility_30d,
  post_incident_volatility_30d,
  days_to_price_recovery,
  notes,
  created_at,
  updated_at,
  bronze_ingestion_timestamp,
  source_file,
  dq_invalid_incident_id,
  dq_invalid_stock_ticker,
  dq_invalid_price_7d_before,
  dq_invalid_price_disclosure_day,
  dq_invalid_price_1d_after,
  dq_invalid_price_7d_after,
  dq_invalid_price_30d_after,
  dq_invalid_volume_avg_30d_baseline,
  dq_invalid_volume_disclosure_day,
  dq_invalid_p_value_1d,
  dq_invalid_p_value_30d,
  dq_invalid_volume_ratio_disclosure,
  dq_invalid_pre_volatility_30d,
  dq_invalid_post_volatility_30d,
  dq_invalid_days_to_price_recovery,
  dq_invalid_updated_before_created,
  dq_invalid_boolean_token,
  dq_missing_bronze_ingestion_timestamp,
  dq_has_issues,
  silver_ingestion_timestamp
) VALUES (
  src.incident_id,
  src.stock_ticker,
  src.price_7d_before,
  src.price_disclosure_day,
  src.price_1d_after,
  src.price_7d_after,
  src.price_30d_after,
  src.volume_avg_30d_baseline,
  src.volume_disclosure_day,
  src.sector_index,
  src.sector_return_same_period,
  src.abnormal_return_1d,
  src.abnormal_return_7d,
  src.abnormal_return_30d,
  src.car_neg1_to_pos1,
  src.car_0_to_7,
  src.car_0_to_30,
  src.car_0_to_90,
  src.t_statistic_1d,
  src.p_value_1d,
  src.t_statistic_30d,
  src.p_value_30d,
  src.earnings_announcement_within_7d,
  src.market_cap_at_disclosure,
  src.volume_ratio_disclosure,
  src.pre_incident_volatility_30d,
  src.post_incident_volatility_30d,
  src.days_to_price_recovery,
  src.notes,
  src.created_at,
  src.updated_at,
  src.bronze_ingestion_timestamp,
  src.source_file,
  src.dq_invalid_incident_id,
  src.dq_invalid_stock_ticker,
  src.dq_invalid_price_7d_before,
  src.dq_invalid_price_disclosure_day,
  src.dq_invalid_price_1d_after,
  src.dq_invalid_price_7d_after,
  src.dq_invalid_price_30d_after,
  src.dq_invalid_volume_avg_30d_baseline,
  src.dq_invalid_volume_disclosure_day,
  src.dq_invalid_p_value_1d,
  src.dq_invalid_p_value_30d,
  src.dq_invalid_volume_ratio_disclosure,
  src.dq_invalid_pre_volatility_30d,
  src.dq_invalid_post_volatility_30d,
  src.dq_invalid_days_to_price_recovery,
  src.dq_invalid_updated_before_created,
  src.dq_invalid_boolean_token,
  src.dq_missing_bronze_ingestion_timestamp,
  src.dq_has_issues,
  current_timestamp()
);

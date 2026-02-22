CREATE TABLE IF NOT EXISTS workspace.audit.data_quality_metrics (
  layer STRING,
  table_name STRING,
  column_name STRING,
  null_count BIGINT,
  null_pct DOUBLE,
  total_rows BIGINT,
  profiled_at TIMESTAMP
)
USING DELTA;

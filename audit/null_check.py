from pyspark.sql import functions as F

table_fqn = "workspace.bronze.incidents_master_raw"
df = spark.table(table_fqn)

total_rows = df.count()

# null counts
exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
null_counts = df.agg(*exprs).collect()[0].asDict()

result = [(col, int(null_counts[col]), round(null_counts[col] * 100 / total_rows, 2))
          for col in df.columns]

result_df = (
    spark.createDataFrame(result, ["column_name", "null_count", "null_pct"])
    .orderBy(F.desc("null_pct"), F.desc("null_count"))
)

display(result_df)

layer_name = "bronze"
table_short = "incidents_master_raw"

dq_df = (
    result_df
    .withColumn("layer", F.lit(layer_name))
    .withColumn("table_name", F.lit(table_short))
    .withColumn("total_rows", F.lit(total_rows).cast("bigint"))  # <-- CLAVE
    .withColumn("profiled_at", F.current_timestamp().cast("timestamp"))
    .withColumn("null_count", F.col("null_count").cast("bigint"))
    .withColumn("null_pct", F.col("null_pct").cast("double"))
    .select("layer","table_name","column_name","null_count","null_pct","total_rows","profiled_at")
)

dq_df.write.mode("append").saveAsTable("workspace.audit.data_quality_metrics")

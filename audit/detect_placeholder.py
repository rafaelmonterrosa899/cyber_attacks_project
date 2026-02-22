# ==========================================
# SILVER LAYER - incidents (profiling placeholders)
# Source: workspace.bronze.incidents_master_raw
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, StringType as ST, LongType

# ----------------------------
# CONFIG
# ----------------------------
table_fqn = "workspace.bronze.incidents_master_raw"

placeholders = [
    "null", "n/a", "na", "",
    "unknown", "none", "not reported",
    "not available", "tbd", "--", "?"
]

# Column to inspect in detail
focus_col = "attribution_confidence"

# ----------------------------
# LOAD
# ----------------------------
df = spark.table(table_fqn)

# ----------------------------
# 1) Find columns with placeholders (string cols only)
# ----------------------------
results = []

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        col_name = field.name

        condition = F.lower(F.trim(F.col(col_name))).isin(placeholders)
        count_placeholder = df.filter(condition).count()

        if count_placeholder > 0:
            results.append((col_name, int(count_placeholder)))

schema = StructType([
    StructField("column_name", ST(), True),
    StructField("placeholder_count", LongType(), True),
])

result_df = spark.createDataFrame(results, schema=schema) \
                 .orderBy(F.desc("placeholder_count"))

display(result_df)

# ----------------------------
# 2) Inspect the placeholders found in ONE specific column
#    (shows raw_value + normalized + count)
# ----------------------------
col = focus_col

detail_df = (
    df.select(F.col(col).alias("raw_value"))
      .withColumn("normalized", F.lower(F.trim(F.col("raw_value"))))
      .where(F.col("normalized").isin(placeholders))
      .groupBy("raw_value", "normalized")
      .count()
      .orderBy(F.desc("count"))
)

display(detail_df)

# ----------------------------
# 3) (Optional) Create a "silver-ready" DF replacing placeholders with NULL
#    NOTE: This does NOT write anything yet; it just builds the transformed DF
# ----------------------------
silver_df = df

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        c = field.name
        silver_df = silver_df.withColumn(
            c,
            F.when(F.lower(F.trim(F.col(c))).isin(placeholders), F.lit(None)).otherwise(F.col(c))
        )

# Quick check: after cleaning, show NULL count for the focus column
silver_df.select(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col(focus_col).isNull(), 1).otherwise(0)).alias("nulls_after_clean")
).show()

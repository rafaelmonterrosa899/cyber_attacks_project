spark.sql("USE CATALOG workspace")

#Define Paths
raw_base_path = "/Volumes/workspace/default/raw_cyber_attacks/archive/"

incidents_path = raw_base_path + "incidents_master.csv"
financial_path = raw_base_path + "financial_impact.csv"
market_path    = raw_base_path + "market_impact.csv"


#Read plus metadata
from pyspark.sql.functions import current_timestamp, lit

def read_csv(path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def add_metadata(df, source_name):
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(source_name))
    )

df_incidents = add_metadata(read_csv(incidents_path), "incidents_master.csv")
df_financial = add_metadata(read_csv(financial_path), "financial_impact.csv")
df_market    = add_metadata(read_csv(market_path), "market_impact.csv")


#Bronze tables
(df_incidents.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("workspace.bronze.incidents_master_raw"))

(df_financial.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("workspace.bronze.financial_impact_raw"))

(df_market.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("workspace.bronze.market_impact_raw"))


spark.sql("DESCRIBE workspace.bronze.incidents_master_raw").show(truncate=False)
spark.sql("DESCRIBE workspace.bronze.financial_impact_raw").show(truncate=False)
spark.sql("DESCRIBE workspace.bronze.market_impact_raw").show(truncate=False)

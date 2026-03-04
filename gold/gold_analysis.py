
#Insights

# The analysis shows a clear inverse relationship between company size and the relative financial impact of cyber incidents. Smaller companies experience significantly higher loss-to-revenue ratios, with median impacts above 10%, while large enterprises typically experience losses below 1% of revenue.


#Script to review the correlation between variables 

from pyspark.sql import functions as F
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns

#Read the gold table 

df = spark.table("workspace.gold.incidents_master_gold")  

#Select only numberic columns 

from pyspark.sql import functions as F

num_cols = [c for (c,t) in df.dtypes if t in ("int","bigint","double","float","decimal","long","short")]

df_num = df.select([F.col(c).cast("double").alias(c) for c in num_cols])

# Correlation matrix (Pearson)
corr = pd.DataFrame(index=num_cols, columns=num_cols, dtype=float)

for c1 in num_cols:
    for c2 in num_cols:
        corr.loc[c1, c2] = df_num.stat.corr(c1, c2)  # Pearson
corr

#Correlation Matrix
plt.figure(figsize=(10,8))
sns.heatmap(corr, annot=True, cmap="coolwarm")
plt.show()

#graph to check if little companies suffers more in terms of total loss

# --- scatterplot (FIX) ---
pdf = (
    df.select(
        F.col("company_revenue_usd").cast("double").alias("company_revenue_usd"),
        F.col("total_loss_revenue_ratio").cast("double").alias("total_loss_revenue_ratio"),
    )
    .where(F.col("company_revenue_usd").isNotNull() & F.col("total_loss_revenue_ratio").isNotNull())
    .limit(20000)   # evita reventar memoria si hay muchos rows
    .toPandas()
)

plt.figure(figsize=(10,6))

sns.scatterplot(
    data=pdf,
    x="company_revenue_usd",
    y="total_loss_revenue_ratio"
)

plt.xscale("log")

plt.xlabel("Company Revenue (log scale)")
plt.ylabel("Total Loss / Revenue")
plt.title("Cyber Incident Impact vs Company Size")

plt.show()

#Group by company size

pdf["size_bucket"] = pd.qcut(pdf["company_revenue_usd"], 5, labels=["Q1 (smallest)", "Q2", "Q3", "Q4", "Q5 (largest)"])

plt.figure(figsize=(10,6))
sns.boxplot(data=pdf, x="size_bucket", y="total_loss_revenue_ratio")
plt.xlabel("Company Size (Revenue Quantiles)")
plt.ylabel("Total Loss / Revenue")
plt.title("Relative Cyber Incident Impact by Company Size")
plt.show()


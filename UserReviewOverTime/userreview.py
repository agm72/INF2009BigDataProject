from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, substring, round, avg as spark_avg, countDistinct
)
import matplotlib.pyplot as plt

# 1) Spark session (only needed if you haven't started PySpark with an existing SparkSession)
spark = SparkSession.builder \
    .appName("SparkUserReviewAnalysisWithPlots") \
    .getOrCreate()

# 2) Read data, define schema
schema = """
    user_id STRING,
    year STRING,
    yelpingSince STRING,
    review_count INT,
    avg_stars DOUBLE,
    avg_review_len DOUBLE
"""
df = spark.read.csv(
    "hdfs://localhost:9000/user/gm/output/user_review_join/part-*",
    sep="\t",
    schema=schema
)

# 3) Compute yearsActive
df_enriched = df \
    .withColumn("joinedYear", substring(col("yelpingSince"), 1, 4).cast("int")) \
    .withColumn("reviewYear", col("year").cast("int")) \
    .withColumn("yearsActive", col("reviewYear") - col("joinedYear"))

# Filter out negative or null yearsActive
df_enriched = df_enriched.filter(col("yearsActive") >= 0)

# 4) Aggregations
trend_df = df_enriched.groupBy("yearsActive") \
    .agg(
        round(spark_avg("avg_stars"), 2).alias("avg_stars_evolution"),
        countDistinct("user_id").alias("user_count")
    ) \
    .orderBy("yearsActive")

trend_len_df = df_enriched.groupBy("yearsActive") \
    .agg(
        round(spark_avg("avg_review_len"), 2).alias("avg_len_evolution"),
        countDistinct("user_id").alias("user_count")
    ) \
    .orderBy("yearsActive")

trend_pd = trend_df.toPandas()
trend_len_pd = trend_len_df.toPandas()

# 5) Plot 1: Average Star Rating vs. Years Active
plt.figure()
plt.plot(
    trend_pd["yearsActive"],
    trend_pd["avg_stars_evolution"],
    marker="o"
)
plt.title("Average Star Rating vs. Years Active")
plt.xlabel("Years Active")
plt.ylabel("Average Star Rating")
plt.show()

# 6) Plot 2: Average Review Length vs. Years Active
plt.figure()
plt.plot(
    trend_len_pd["yearsActive"],
    trend_len_pd["avg_len_evolution"],
    marker="o"
)
plt.title("Average Review Length vs. Years Active")
plt.xlabel("Years Active")
plt.ylabel("Average Review Length (Characters)")
plt.show()

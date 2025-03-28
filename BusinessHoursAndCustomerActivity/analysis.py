# -------------------------------------------------------
# 1. Initialize Spark Session
# -------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("BusinessHoursReviewAnalysis") \
    .getOrCreate()

# -------------------------------------------------------
# 2. Define a schema for reading the output
# -------------------------------------------------------
# The MapReduce output has 4 columns in tab-separated format:
#   business_id, total_hours, review_count, avg_stars

schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("total_hours", DoubleType(), True),
    StructField("review_count", LongType(), True),
    StructField("avg_stars", DoubleType(), True)
])

# -------------------------------------------------------
# 3. Load the data from HDFS
# -------------------------------------------------------
input_path = "hdfs://localhost:9000/user/gm/output_yelp_hrs_reviews/part-*"

df = spark.read \
    .option("sep", "\t") \
    .schema(schema) \
    .csv(input_path)

df.show(10, truncate=False)

# -------------------------------------------------------
# 4. Basic Exploratory Analysis
# -------------------------------------------------------
# A) Record count
record_count = df.count()
print(f"Record count: {record_count}")

# B) Summary stats
df.describe(["total_hours", "review_count", "avg_stars"]).show()

# -------------------------------------------------------
# 5. Correlation Analysis
# -------------------------------------------------------
corr_hours_reviews = df.stat.corr("total_hours", "review_count")
corr_hours_avgstars = df.stat.corr("total_hours", "avg_stars")

print("Correlation between total_hours and review_count:", corr_hours_reviews)
print("Correlation between total_hours and avg_stars:", corr_hours_avgstars)

# -------------------------------------------------------
# 6. Bucket total_hours for grouping
# -------------------------------------------------------
df_buckets = df.withColumn(
    "hours_bucket",
    F.when(df["total_hours"] < 40, "low_hours")
     .when((df["total_hours"] >= 40) & (df["total_hours"] < 80), "medium_hours")
     .otherwise("high_hours")
)

grouped_df = df_buckets.groupBy("hours_bucket") \
    .agg(
        F.count("*").alias("business_count"),
        F.avg("review_count").alias("avg_review_count"),
        F.avg("avg_stars").alias("avg_star_rating")
    ) \
    .orderBy("hours_bucket")

grouped_df.show()

# -------------------------------------------------------
# 7. Visualization with Matplotlib
# -------------------------------------------------------
# We'll collect sample data for plotting. 
# For large datasets, be aware of potential memory issues.

import matplotlib.pyplot as plt

# A) Histogram of total_hours
# -------------------------------------------------------
# Each chart should be in its own figure

hours_data = df.select("total_hours").dropna().sample(fraction=0.05).collect()
hours_values = [row.total_hours for row in hours_data]

plt.figure()  # new figure
plt.hist(hours_values, bins=20)
plt.xlabel("Total Weekly Hours")
plt.ylabel("Frequency")
plt.title("Distribution of Business Weekly Hours")
plt.show()


# B) Scatter plot: total_hours vs. review_count
# -------------------------------------------------------
scatter_data = df.select("total_hours", "review_count").dropna().sample(fraction=0.01).collect()
x_hours = [row.total_hours for row in scatter_data]
y_reviews = [row.review_count for row in scatter_data]

plt.figure()  # new figure
plt.scatter(x_hours, y_reviews)
plt.xlabel("Total Weekly Hours")
plt.ylabel("Review Count")
plt.title("Hours vs. Review Count")
plt.show()

# -------------------------------------------------------
# Optional Additional Visualization:
#   e.g., a scatter plot of total_hours vs. avg_stars
# -------------------------------------------------------
scatter_data_2 = df.select("total_hours", "avg_stars").dropna().sample(fraction=0.01).collect()
x_hours_2 = [row.total_hours for row in scatter_data_2]
y_stars = [row.avg_stars for row in scatter_data_2]

plt.figure()  # new figure
plt.scatter(x_hours_2, y_stars)
plt.xlabel("Total Weekly Hours")
plt.ylabel("Average Star Rating")
plt.title("Hours vs. Average Star Rating")
plt.show()

# -------------------------------------------------------
# 8. End / Cleanup
# -------------------------------------------------------
spark.stop()

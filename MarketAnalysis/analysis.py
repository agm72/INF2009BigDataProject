from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession (if not already created)
spark = SparkSession.builder.appName("YelpCityAnalysis").getOrCreate()

# Adjust the HDFS path as needed. Here we assume the Job2 output is at:
# hdfs:///user/gm/yelp_output/job2
# Each line is expected to be in the format:
# "city,state<TAB>totalBusinesses,totalReviews,avgStars,totalCheckins"
df = spark.read.option("delimiter", "\t").csv("hdfs://localhost:9000/user/gm/yelp_output/job2", header=False)

# The file is loaded into two columns: _c0 (city,state) and _c1 (the aggregated string)
# Split the aggregated string into separate columns:
split_cols = split(col("_c1"), ",")
df2 = df.withColumn("city_state", col("_c0")) \
        .withColumn("totalBusinesses", split_cols.getItem(0).cast("int")) \
        .withColumn("totalReviews", split_cols.getItem(1).cast("int")) \
        .withColumn("avgStars", split_cols.getItem(2).cast("double")) \
        .withColumn("totalCheckins", split_cols.getItem(3).cast("int"))

# Select the columns we need
city_df = df2.select("city_state", "totalBusinesses", "totalReviews", "avgStars", "totalCheckins")

# Show the first few rows in Spark to verify the schema
print("City-level aggregated data:")
city_df.show(10, truncate=False)

# Example Query 1: Top 10 cities by total reviews
top_reviews = city_df.orderBy(col("totalReviews").desc()).limit(10)
print("Top 10 cities by total reviews:")
top_reviews.show(truncate=False)

# Example Query 2: Top 10 cities by total check-ins
top_checkins = city_df.orderBy(col("totalCheckins").desc()).limit(10)
print("Top 10 cities by total check-ins:")
top_checkins.show(truncate=False)

# Example Query 3: Top 10 cities by average stars (with at least 100 reviews)
top_avgStars = city_df.filter(col("totalReviews") > 100).orderBy(col("avgStars").desc()).limit(10)
print("Top 10 cities by average stars (min 100 reviews):")
top_avgStars.show(truncate=False)

# Convert the Spark DataFrame to a Pandas DataFrame for visualization
pdf = city_df.toPandas()

# Optionally, separate city and state for better labeling
pdf[['city', 'state']] = pdf['city_state'].str.split(",", expand=True)

# Set seaborn style for plots
sns.set(style="whitegrid")

# Visualization 1: Bar chart for top 10 cities by total reviews
top_reviews_pd = pdf.sort_values(by="totalReviews", ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="totalReviews", y="city_state", data=top_reviews_pd, palette="viridis")
plt.title("Top 10 Cities by Total Reviews")
plt.xlabel("Total Reviews")
plt.ylabel("City, State")
plt.tight_layout()
plt.show()

# Visualization 2: Scatter plot of average stars vs. total businesses, colored by state
plt.figure(figsize=(10, 6))
sns.scatterplot(x="totalBusinesses", y="avgStars", data=pdf, hue="state", palette="deep")
plt.title("Average Stars vs. Total Businesses by City")
plt.xlabel("Total Businesses")
plt.ylabel("Average Stars")
plt.tight_layout()
plt.show()

# Visualization 3: Bar chart for top 10 cities by total check-ins
top_checkins_pd = pdf.sort_values(by="totalCheckins", ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x="totalCheckins", y="city_state", data=top_checkins_pd, palette="coolwarm")
plt.title("Top 10 Cities by Total Check-ins")
plt.xlabel("Total Check-ins")
plt.ylabel("City, State")
plt.tight_layout()
plt.show()

# Stop the Spark session when finished (if running as a script)
# spark.stop()

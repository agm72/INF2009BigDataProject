from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Data visualization imports
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = SparkSession.builder.appName("YelpJoinedAnalysis").getOrCreate()

# Define schema for the joined output
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("total_reviews", IntegerType(), True),
    StructField("compliment_hot", IntegerType(), True),
    StructField("compliment_more", IntegerType(), True),
    StructField("compliment_profile", IntegerType(), True)
])

# Read the output of the MapReduce job from HDFS
df_joined = spark.read \
    .option("delimiter", "\t") \
    .schema(schema) \
    .csv("hdfs://localhost:9000/user/gm/yelp_out/user_review_join")

# Show first 10 records
df_joined.show(10)

# Compute and display the average number of reviews per user
df_joined.agg({"total_reviews": "avg"}).show()

# Show the top 10 users by total_reviews in descending order
df_joined.orderBy(df_joined["total_reviews"].desc()).show(10)

# --------------------
# DATA VISUALIZATION
# --------------------

# 1. Create a Spark DataFrame of the top 10 users by total_reviews.
top_10_df = df_joined.orderBy(df_joined["total_reviews"].desc()).limit(10)

# 2. Convert the Spark DataFrame to a Pandas DataFrame for plotting.
top_10_pdf = top_10_df.toPandas()

# 3. Plot a bar chart of the top 10 users by their total reviews.
plt.figure(figsize=(10, 6))  # set a reasonable figure size

# If user_name might be large or contain special chars, you may prefer a horizontal bar:
sns.barplot(x="total_reviews", y="user_name", data=top_10_pdf)

plt.title("Top 10 Users by Number of Reviews")
plt.xlabel("Number of Reviews")
plt.ylabel("User Name")

# 4. Display the plot
plt.tight_layout()
plt.show()

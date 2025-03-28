from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Data visualization imports
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------------
# Spark Session & Data Loading
# -------------------------------
spark = SparkSession.builder.appName("YelpJoinedAnalysis").getOrCreate()

# Define the schema for the joined output produced by MapReduce
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("total_reviews", IntegerType(), True),
    StructField("compliment_hot", IntegerType(), True),
    StructField("compliment_more", IntegerType(), True),
    StructField("compliment_profile", IntegerType(), True)
])

# Read the output from HDFS (adjust the path as necessary)
df_joined = spark.read \
    .option("delimiter", "\t") \
    .schema(schema) \
    .csv("hdfs://localhost:9000/user/gm/yelp_out/user_review_join")

# Show the first 10 records
df_joined.show(10)

# Compute and display the average number of reviews per user
df_joined.agg({"total_reviews": "avg"}).show()

# Show the top 10 users by total reviews in descending order
df_joined.orderBy(df_joined["total_reviews"].desc()).show(10)

# -------------------------------
# Data Visualization using matplotlib/seaborn
# -------------------------------

# Convert the Spark DataFrame to a Pandas DataFrame for visualization
df_pdf = df_joined.toPandas()

# Compute total compliments per user as the sum of compliment_hot, compliment_more, and compliment_profile
df_pdf['total_compliments'] = df_pdf['compliment_hot'] + df_pdf['compliment_more'] + df_pdf['compliment_profile']

# 1. Top 10 Users by Total Reviews (Bar Chart)
top_10_reviews_df = df_joined.orderBy(df_joined["total_reviews"].desc()).limit(10)
top_10_reviews_pdf = top_10_reviews_df.toPandas()

plt.figure(figsize=(10, 6))
sns.barplot(x="total_reviews", y="user_name", data=top_10_reviews_pdf)
plt.title("Top 10 Users by Number of Reviews")
plt.xlabel("Number of Reviews")
plt.ylabel("User Name")
plt.tight_layout()
plt.show()

# 2. Top 10 Users by Total Compliments (Bar Chart)
top_10_compliments_pdf = df_pdf.sort_values(by='total_compliments', ascending=False).head(10)

plt.figure(figsize=(10, 6))
sns.barplot(x="total_compliments", y="user_name", data=top_10_compliments_pdf)
plt.title("Top 10 Users by Total Compliments")
plt.xlabel("Total Compliments")
plt.ylabel("User Name")
plt.tight_layout()
plt.show()

# Stop the Spark session when done
spark.stop()

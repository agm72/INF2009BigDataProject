from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

# Read the output of the MapReduce job
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

import sys
import setuptools._distutils
sys.modules['distutils'] = setuptools._distutils

# Contributor: Carol
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("YelpDataAnalysis").getOrCreate()

# --- Analyze Sentiment Scores ---
# Read the sentiment join output (format: business_id, "Categories: ... | Sentiment: ...")
sentiment_rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/gm/yelp_dataset/sentiment/part-*")

def parse_sentiment(line):
    """
    Splits each line from the sentiment output into:
      - business_id
      - categories (canonicalized: sorted alphabetically)
      - sentiment score (integer)
    """
    try:
        parts = line.split("\t")
        business_id = parts[0]
        info = parts[1]
        
        # Extract sentiment value
        sentiment = int(info.split("Sentiment: ")[1])
        
        # Extract categories string (e.g. "Mexican, Restaurants")
        categories_raw = info.split("Categories: ")[1].split(" | ")[0]
        
        # Normalize categories by splitting, stripping, sorting, and rejoining
        cat_list = [c.strip() for c in categories_raw.split(",")]
        cat_list.sort()  # Alphabetical order
        categories_canonical = ", ".join(cat_list)
        
        return (business_id, categories_canonical, sentiment)
    except Exception as e:
        # Return default if parsing fails
        return ("unknown", "unknown", 0)

sentiment_data = sentiment_rdd.map(parse_sentiment)
sentiment_df = sentiment_data.toDF(["business_id", "categories", "sentiment"])

# Aggregate sentiment scores by categories
category_sentiment = sentiment_df.groupBy("categories").agg(_sum("sentiment").alias("total_sentiment"))
category_sentiment_pd = category_sentiment.orderBy(col("total_sentiment").desc()).toPandas()

# Stop Spark session
spark.stop()

# --- Visualization with Matplotlib/Seaborn ---
# Plot Top 20 Categories by Total Sentiment
plt.figure(figsize=(10,6))
category_sentiment_pd_top = category_sentiment_pd.head(20)
sns.barplot(
    x="total_sentiment",
    y="categories",
    data=category_sentiment_pd_top,
    palette="magma"
)
plt.title("Top 20 Categories by Total Sentiment")
plt.xlabel("Total Sentiment Score")
plt.ylabel("Categories")
plt.tight_layout()
plt.savefig("category_sentiment.png")
plt.show()


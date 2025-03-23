import sys
import setuptools._distutils
sys.modules['distutils'] = setuptools._distutils

# Contributor: Carol
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# 1. Adjust global font scale for Seaborn (optional)
sns.set(font_scale=0.9)

# 2. Initialize Spark Session
spark = SparkSession.builder.appName("YelpDataAnalysis").getOrCreate()

# 3. Read the sentiment join output from HDFS
# Expected format per line: business_id, "Categories: ... | Sentiment: ..."
sentiment_rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/gm/yelp_dataset/sentiment/part-*")

def parse_sentiment(line):
    """
    Splits each line from the sentiment output into:
      - business_id
      - categories (canonicalized: sorted alphabetically)
      - sentiment score (integer)
    Example line format:
        <business_id> <tab> Categories: <categories> | Sentiment: <score>
    """
    try:
        parts = line.split("\t")
        business_id = parts[0]
        info = parts[1]
        
        # Extract sentiment value
        sentiment = int(info.split("Sentiment: ")[1])
        
        # Extract categories string (e.g. "Mexican, Restaurants")
        categories_raw = info.split("Categories: ")[1].split(" | ")[0]
        
        # Normalize categories: split by comma, strip whitespace, sort, and re-join
        cat_list = [c.strip() for c in categories_raw.split(",")]
        cat_list.sort()  # Alphabetical order ensures consistent ordering
        categories_canonical = ", ".join(cat_list)
        
        return (business_id, categories_canonical, sentiment)
    except Exception as e:
        # In case of error, return default values
        return ("unknown", "unknown", 0)

# 4. Convert RDD to a DataFrame
sentiment_data = sentiment_rdd.map(parse_sentiment)
sentiment_df = sentiment_data.toDF(["business_id", "categories", "sentiment"])

# 5. Aggregate sentiment scores by canonicalized categories
category_sentiment = sentiment_df.groupBy("categories").agg(_sum("sentiment").alias("total_sentiment"))

# 6. Sort in ascending order to get the 20 worst (lowest) sentiment categories
category_sentiment_worst = category_sentiment.orderBy(col("total_sentiment").asc()).limit(20)
category_sentiment_pd_worst = category_sentiment_worst.toPandas()

# 7. Stop Spark session (no longer needed after data is collected)
spark.stop()

# --- Visualization with Matplotlib/Seaborn ---

# 8. Create a larger figure and bar plot
plt.figure(figsize=(12, 8))  # Wider and taller to fit longer labels
sns.barplot(
    x="total_sentiment",
    y="categories",
    data=category_sentiment_pd_worst,
    palette="magma"
)

# 9. Title and axis labels
plt.title("Top 20 Worst Categories by Total Sentiment", fontsize=14)
plt.xlabel("Total Sentiment Score", fontsize=12)
plt.ylabel("Categories", fontsize=12)

# 10. Tight layout ensures minimal clipping
plt.tight_layout()

# 11. Save the figure with bbox_inches='tight' to include all text
plt.savefig("category_sentiment_worst.png", bbox_inches='tight')

# 12. Display the plot
plt.show()


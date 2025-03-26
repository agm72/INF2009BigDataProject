#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("YelpAggregatedAnalysis") \
        .getOrCreate()

    # 1. Load the aggregated data from the Job 2 output
    # Each line is: city<TAB>category<TAB>avg_rating<TAB>review_count
    aggregated_rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/gm/yelp_output/job2_aggregated")

    # 2. Parse each line into a DataFrame row
    df = aggregated_rdd.map(lambda line: line.split("\t")) \
        .map(lambda fields: (
            fields[0],               # city
            fields[1],               # category
            float(fields[2]),        # avg_rating
            int(fields[3])           # review_count
        )) \
        .toDF(["city", "category", "avg_rating", "review_count"])

    # 3. Show the DataFrame schema and a few rows to verify
    df.printSchema()
    df.show(10, truncate=False)

    # 4. Top Category per City:
    # Use a window function to rank categories within each city by avg_rating in descending order.
    window_city = Window.partitionBy("city").orderBy(F.desc("avg_rating"))

    ranked_df = (
        df.withColumn("rank", F.rank().over(window_city))  # assign rank within each city
          .filter(F.col("rank") == 1)                      # keep only the top category (rank 1)
          .orderBy("city")
    )

    # 5. Show the top category per city results
    print("Top category per city:")
    ranked_df.show(100, truncate=False)

    # 6. Show all cities:
    # Get a DataFrame of all distinct cities from the aggregated data
    distinct_cities = df.select("city").distinct().orderBy("city")
    # Left join with ranked_df so that every city appears, even if no top category exists (if that's possible)
    all_cities = distinct_cities.join(ranked_df, on="city", how="left").orderBy("city")
    print("All cities with top category (if available):")
    all_cities.show(100, truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()

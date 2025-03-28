#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

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

    # 6. Randomize and limit to 10 cities
    ranked_randomized_df = ranked_df.orderBy(F.rand()).limit(10)
    pandas_ranked_df = ranked_randomized_df.toPandas()

    # 7. Visualization as a Table
    # We'll only display city and category columns in the table.
    table_data = pandas_ranked_df[['city', 'category']]

    # Increase figure size to accommodate wider text
    fig, ax = plt.subplots(figsize=(12, 4))  # width=12, height=4
    ax.axis('off')  # Hide axes for a clean table look

    # Create the table in the center of the axes
    table = ax.table(
        cellText=table_data.values,
        colLabels=table_data.columns,
        loc='center',
        cellLoc='center'
    )

    # Turn off auto font sizing so we can manually set it
    table.auto_set_font_size(False)
    table.set_fontsize(10)

    # Automatically set column widths to match content
    # (requires matplotlib 3.4+ for best results)
    table.auto_set_column_width(col=list(range(len(table_data.columns))))

    plt.title("Top Category per City (Random 10 Cities)", pad=20)
    plt.tight_layout()
    plt.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

def parse_star_range(sr):
    """
    Helper function to parse the lower bound of the star range string.
    For example, "3.0 - 3.5" -> 3.0.
    """
    try:
        return float(sr.split("-")[0].strip())
    except Exception:
        return 0.0

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("StarRatingCheckinAnalysis").getOrCreate()
    
    # Path to the MapReduce output stored in HDFS.
    # Make sure this path matches the output directory of your MapReduce job.
    hdfs_path = "hdfs:///user/gm/yelp_dataset/join_output/part-*"
    
    # Load the output as text (each line is "star_range <tab> checkin_count")
    rdd = spark.sparkContext.textFile(hdfs_path)
    
    # Split each line by tab to separate the star range and check-in count
    split_rdd = rdd.map(lambda line: line.split("\t"))
    
    # Define schema for our DataFrame: star_range as string, checkin_count as integer.
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("star_range", StringType(), True),
        StructField("checkin_count", IntegerType(), True)
    ])
    
    # Convert the RDD to a DataFrame.
    # In case a line is missing the check-in count, default to 0.
    row_rdd = split_rdd.map(lambda x: (x[0], int(x[1]) if len(x) > 1 else 0))
    df = spark.createDataFrame(row_rdd, schema)
    
    print("Initial DataFrame:")
    df.show(truncate=False)
    
    # Group by star_range to aggregate check-in counts (if multiple entries exist per range)
    grouped_df = df.groupBy("star_range").agg(F.sum("checkin_count").alias("total_checkins"))
    
    # Order by star range (sorting using the lower bound)
    grouped_df = grouped_df.orderBy(F.asc("star_range"))
    
    print("Aggregated DataFrame:")
    grouped_df.show(truncate=False)
    
    # Collect results to the driver for plotting
    results = grouped_df.collect()
    
    # Sort the results using our parse_star_range helper
    results_list = sorted(results, key=lambda row: parse_star_range(row["star_range"]))
    
    # Separate star ranges and total check-ins for plotting
    star_ranges = [row["star_range"] for row in results_list]
    total_checkins = [row["total_checkins"] for row in results_list]
    
    # Plot using matplotlib
    plt.figure(figsize=(10, 6))
    plt.bar(star_ranges, total_checkins)
    plt.xlabel("Star Rating Range")
    plt.ylabel("Total Check-ins")
    plt.title("Impact of Star Ratings on Check-ins")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

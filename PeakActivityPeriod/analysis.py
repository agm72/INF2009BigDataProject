from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, desc
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("YelpPeakActivityMRAnalysis").getOrCreate()

# Read the MapReduce output (each line is "hour<TAB>count")
df = spark.read.text("hdfs://localhost:9000/user/gm/output_peak_activity")

# Split the line into hour and count based on tab delimiter.
# The MapReduce job typically outputs each record as: hour\tcount
split_col = split(df['value'], "\t")
df_parsed = df.withColumn("hour", split_col.getItem(0)) \
              .withColumn("count", split_col.getItem(1).cast(IntegerType())) \
              .drop("value")

# Show the parsed data
print("Parsed MapReduce Output (Peak Activity by Hour):")
df_parsed.show()

# Order the results by hour for a chronological view
df_ordered = df_parsed.orderBy("hour")
print("Peak Activity by Hour (Ordered):")
df_ordered.show()

# Identify the peak hour with the maximum count
peak_hour = df_parsed.orderBy(desc("count")).limit(1)
print("Peak Hour with Maximum Checkins:")
peak_hour.show()

# Convert Spark DataFrame to Pandas DataFrame for visualization
pdf = df_ordered.toPandas()

# Plot the data using matplotlib
plt.figure(figsize=(10, 6))
plt.bar(pdf['hour'], pdf['count'])
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Check-ins")
plt.title("Peak Activity by Hour")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

spark.stop()

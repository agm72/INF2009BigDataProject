"""
PySpark + Matplotlib script to read the MapReduce output for Yelp review distributions
and perform data analysis.

Analyses:
1) Top 10 Businesses by Number of 5-Star Reviews
2) Top 10 Businesses by 5-Star Ratio (Min 100 Reviews)
3) Distribution of Polarization Scores (Grouped by 0.1 increments)

MapReduce output format (TSV + comma):
   <business_id>\t<businessName>,<count1>,<count2>,<count3>,<count4>,<count5>
Example line:
   ---kPU91CF4Lq2-WlRu9Lw    Frankie's Raw Bar,1,0,2,4,17

Parsing steps:
1. Read with tab ("\t") => 2 columns (_c0, _c1)
2. Split _c1 by "," => array of 6 items
3. Map each array item to columns (business_name, star counts)
4. Create a temp view and run SQL queries
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

def main():
    spark = SparkSession.builder \
        .appName("YelpReviewDistributionAnalysisWithViz") \
        .getOrCreate()

    # 1) Read the MapReduce output from HDFS
    hdfs_output_path = "hdfs://localhost:9000/user/gm/output/yelp_review_distribution/part-*"
    df_raw = spark.read \
        .option("sep", "\t") \
        .csv(hdfs_output_path, header=False, inferSchema=True)

    # The data: 
    #   _c0 => business_id
    #   _c1 => "businessName,count_1star,count_2star,count_3star,count_4star,count_5star"
    df_split = df_raw.select(
        F.col("_c0").alias("business_id"),
        F.split(F.col("_c1"), ",").alias("parsed")
    )

    df_final = df_split.select(
        F.col("business_id"),
        F.col("parsed").getItem(0).alias("business_name"),
        F.col("parsed").getItem(1).cast("int").alias("count_1star"),
        F.col("parsed").getItem(2).cast("int").alias("count_2star"),
        F.col("parsed").getItem(3).cast("int").alias("count_3star"),
        F.col("parsed").getItem(4).cast("int").alias("count_4star"),
        F.col("parsed").getItem(5).cast("int").alias("count_5star")
    )

    df_final.createOrReplaceTempView("business_distribution")

    # ----------------------------------------------------------------------
    # 1) Top 10 Businesses by Number of 5-Star Reviews
    # ----------------------------------------------------------------------
    top10_five_star = spark.sql("""
        SELECT
            business_id,
            business_name,
            count_5star
        FROM business_distribution
        ORDER BY count_5star DESC
        LIMIT 10
    """)

    pdf_top10_five_star = top10_five_star.toPandas()
    print("\n=== Top 10 Businesses by Number of 5-Star Reviews ===")
    print(pdf_top10_five_star)

    # Visualization: Bar chart
    plt.figure()
    plt.bar(pdf_top10_five_star["business_name"], pdf_top10_five_star["count_5star"])
    plt.title("Top 10 Businesses by Number of 5-Star Reviews")
    plt.xlabel("Business Name")
    plt.ylabel("Count of 5-Star Reviews")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

    # ----------------------------------------------------------------------
    # 2) Top 10 Businesses by 5-Star Ratio (Min 100 Reviews)
    # ----------------------------------------------------------------------
    five_star_ratio = spark.sql("""
        SELECT
            business_id,
            business_name,
            count_5star,
            (count_1star + count_2star + count_3star + count_4star + count_5star) AS total_reviews,
            (count_5star * 1.0) / 
             (count_1star + count_2star + count_3star + count_4star + count_5star) AS five_star_ratio
        FROM business_distribution
        WHERE (count_1star + count_2star + count_3star + count_4star + count_5star) >= 100
        ORDER BY five_star_ratio DESC
        LIMIT 10
    """)

    pdf_five_star_ratio = five_star_ratio.toPandas()
    print("\n=== Top 10 Businesses by 5-Star Ratio (Min 100 Reviews) ===")
    print(pdf_five_star_ratio)

    # Visualization: Bar chart of five_star_ratio
    plt.figure()
    plt.bar(pdf_five_star_ratio["business_name"], pdf_five_star_ratio["five_star_ratio"])
    plt.title("Top 10 Businesses by 5-Star Ratio (Min 100 Reviews)")
    plt.xlabel("Business Name")
    plt.ylabel("5-Star Ratio")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

    # ----------------------------------------------------------------------
    # 3) Distribution of Polarization Scores (Grouped by 0.1 increments)
    # ----------------------------------------------------------------------
    distribution_of_pol = spark.sql("""
        WITH sub AS (
          SELECT
            (count_1star + count_5star)*1.0 / 
             (count_1star + count_2star + count_3star + count_4star + count_5star) AS polscore
          FROM business_distribution
          WHERE (count_1star + count_2star + count_3star + count_4star + count_5star) > 0
        )
        SELECT 
           CAST(FLOOR(polscore * 10) AS INT)/10.0 AS pol_bin,
           COUNT(*) AS freq
        FROM sub
        GROUP BY CAST(FLOOR(polscore * 10) AS INT)/10.0
        ORDER BY pol_bin
    """)

    pdf_distribution_pol = distribution_of_pol.toPandas()
    print("\n=== Distribution of Polarization Scores (Grouped by 0.1 increments) ===")
    print(pdf_distribution_pol)

    # Visualization: Bar chart of freq by pol_bin
    plt.figure()
    plt.bar(pdf_distribution_pol["pol_bin"].astype(str), pdf_distribution_pol["freq"])
    plt.title("Distribution of Polarization Scores")
    plt.xlabel("Polarization Score Bin")
    plt.ylabel("Number of Businesses")
    plt.tight_layout()
    plt.show()

    spark.stop()

if __name__ == "__main__":
    main()

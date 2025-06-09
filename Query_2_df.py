from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, desc, col, dense_rank, year, to_timestamp, expr

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 2 - Dataframe") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the HDFS path to the Parquet file
target = "hdfs-namenode"
hdfs_path = f"hdfs://{target}:9000/user/alstylos/data/parquet"

# Read Parquet files into DataFrame
crime_data_df = spark.read.parquet(f"{hdfs_path}/crime_data.parquet", header=True, inferSchema=True)
crime_data_df = crime_data_df.withColumnRenamed("AREA ", "AREA")
police_stations_df = spark.read.parquet(f"{hdfs_path}/police_stations.parquet", header=True, inferSchema=True)

# Query 2 in DataFrame format
df_Q2 = (
    crime_data_df.select(
    col("AREA"),
    col("Status Desc"),
    col("Date Rptd"),
    when(~col("Status Desc").rlike("UNK|Invest Cont"), 1).otherwise(0).alias("closed_case"),
    year(to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a")).alias("Year")
    )
    .join(police_stations_df, col("AREA") == police_stations_df["PREC"], "inner")
    .select(
        col("Year"),
        col("DIVISION"),
        col("closed_case")
    )
    .groupBy("Year", "DIVISION")
    .agg(expr("sum(closed_case) / count(*) * 100").alias("Closed-Case Rate %"))
    .withColumn("Rank", dense_rank().over(Window.partitionBy("Year").orderBy(desc("Closed-Case Rate %"))))
    .filter(col("Rank") <= 3)
    .orderBy("Year", "Rank")
)

# Show the DataFrame
df_Q2.show(truncate=False)

# Print the schema of the DataFrame
df_Q2.printSchema()

# Stop the Spark session
spark.stop()
    
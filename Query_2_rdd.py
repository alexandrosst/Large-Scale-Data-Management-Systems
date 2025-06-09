from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import datetime

def parse_crime_data(item):
    closed_case = 0
    if "UNK" not in item["Status Desc"] and "Invest Cont" not in item["Status Desc"]:
        closed_case = 1

    year = datetime.datetime.strptime(item["Date Rptd"], "%m/%d/%Y %I:%M:%S %p").year

    return (int(item["AREA"]), (year, closed_case, 1))

def parse_police_stations(item):
    return (int(item["PREC"]), item["DIVISION"])

def rank_within_group(group):
    number_of_ranks = 3
    year, items = group
    # Each item: (year, area, closed_cases)
    sorted_items = sorted(items, key=lambda x: x[2], reverse=True)[:number_of_ranks]
    return [(year, area, rate, rank)
            for rank, (year, area, rate) in enumerate(sorted_items, start=1)]

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 2 - RDD") \
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

# Convert DataFrames to RDDs
crime_data_rdd = crime_data_df.rdd
police_stations_rdd = police_stations_df.rdd

# Query 2 in RDD format
rdd_Q2 = (
    crime_data_rdd.map(parse_crime_data)
    .join(police_stations_rdd.map(parse_police_stations))
    .map(lambda item: ((item[1][0][0], item[1][1]), (item[1][0][1], item[1][0][2])))
    # output format: ((year, area), closed_case, count)
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    # output format: ((year, area), (total_closed_case, total_count))
    .map(lambda item: (item[0][0], item[0][1], item[1][0]/item[1][1]*100 if item[1][1] != 0 else 0))
    # output format: (year, area, closed_case_rate)
    .groupBy(lambda item: item[0])
    .flatMap(rank_within_group)
    # output format: (year, area, closed_case_rate, rank)
    .sortBy(lambda item: item[0])
)

rdd_Q2.collect()
# Convert the RDD to a DataFrame
schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Precinct", StringType(), True),
    StructField("Closed-Case Rate %", StringType(), True),
    StructField("#", IntegerType(), True)
])
# Create a DataFrame from the RDD
df_Q2_from_rdd = rdd_Q2.toDF(schema=schema)

# Show the DataFrame
df_Q2_from_rdd.show(truncate=False)

# Print the schema of the DataFrame
df_Q2_from_rdd.printSchema()

# Stop the Spark session
spark.stop()
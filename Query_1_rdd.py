from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 1 - RDD") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the HDFS path to the Parquet file
target = "hdfs-namenode"
hdfs_path = f"hdfs://{target}:9000/user/alstylos/data/parquet"

# Read Parquet file into DataFrame
crime_data_df = spark.read.parquet(f"{hdfs_path}/*crime_data.parquet", header=True, inferSchema=True)

# Convert DataFrame to RDD
crime_data_rdd = crime_data_df.rdd

def find_age_group(age, for_dataframe=False):
    age = int(age)
    age_groups = [
        (18, "under 18", "Minor"),
        (25, "18 - 24", "Young Adult"),
        (65, "25 - 64", "Adult"),
        (float("inf"), "65 and older", "Senior")
    ]

    label, category = next((label, category) for threshold, label, category in age_groups if age < threshold)
    
    return (label, category) if for_dataframe else (label, (category, 1))


rdd_Q1 = (
    crime_data_rdd.filter(lambda item: "AGGRAVATED ASSAULT" in item["Crm Cd Desc"])
        .map(lambda item: find_age_group(item["Vict Age"]))
        .reduceByKey(lambda x, y: (x[0], x[1] + y[1]))
        .sortBy(lambda item: item[1][1], ascending=False)
        .map(lambda item: (item[0], item[1][0], item[1][1]))
)

# Collect the results            
rdd_Q1.collect()

# Convert the RDD to a DataFrame
schema = StructType([
    StructField("Age Group", StringType(), True),
    StructField("Age Group Description", StringType(), True),
    StructField("Number of Victims", IntegerType(), True)
])
# Create a DataFrame from the RDD
df_Q1_from_rdd = rdd_Q1.toDF(schema=schema)

# Show the DataFrame
df_Q1_from_rdd.show(truncate=False)

# Print the schema of the DataFrame
df_Q1_from_rdd.printSchema()

# Stop the Spark session
spark.stop()
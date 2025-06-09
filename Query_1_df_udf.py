from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count, desc, col
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 1 - Dataframe - UDF") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the HDFS path to the Parquet file
target = "hdfs-namenode"
hdfs_path = f"hdfs://{target}:9000/user/alstylos/data/parquet"

# Read Parquet file into DataFrame
crime_data_df = spark.read.parquet(f"{hdfs_path}/crime_data.parquet", header=True, inferSchema=True)

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

# Register the function as UDF
find_age_group_udf = udf(lambda age: find_age_group(age, for_dataframe=True), 
                            StructType([
                                StructField("Age Group", StringType(), True),
                                StructField("Age Group Description", StringType(), True)
                            ]))

df_Q1 = (
    crime_data_df.filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT"))
    .select(col("Vict Age"))
    .withColumn("Age Group and Description", find_age_group_udf(col("Vict Age")))
    .withColumn("Age Group", col("Age Group and Description.Age Group"))
    .withColumn("Age Group Description", col("Age Group and Description.Age Group Description"))
    .groupBy("Age Group", "Age Group Description")
    .agg(count("*").alias("Number of Victims"))
    .orderBy(desc("Number of Victims"))
)

# Show the DataFrame
df_Q1.show(truncate=False)

# Print the schema of the DataFrame
df_Q1.printSchema()

# Stop the Spark session
spark.stop()
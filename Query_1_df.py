from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, desc, col

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 1 - Dataframe") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the HDFS path to the Parquet file
target = "hdfs-namenode"
hdfs_path = f"hdfs://{target}:9000/user/alstylos/data/parquet"

# Read Parquet file into DataFrame
crime_data_df = spark.read.parquet(f"{hdfs_path}/crime_data.parquet", header=True, inferSchema=True)

# Reusable victim age column
vict_age = col("Vict Age")
# Define age group and description
age_group = when(vict_age < 18, "under 18") \
    .when(vict_age.between(18, 24), "18 - 24") \
    .when(vict_age.between(25, 64), "25 - 64") \
    .otherwise("65 and older")

age_group_desc = when(vict_age < 18, "Minor") \
    .when(vict_age.between(18, 24), "Young Adult") \
    .when(vict_age.between(25, 64), "Adult") \
    .otherwise("Senior")

df_Q1 = (
    crime_data_df.filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT"))
    .select(col("Vict Age"))
    .withColumns({"Age Group": age_group, "Age Group Description": age_group_desc})
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
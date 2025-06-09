from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import split, count, desc, col, expr, explode, first

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 4 - Dataframe") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the HDFS path to the Parquet file
target = "hdfs-namenode"
hdfs_path = f"hdfs://{target}:9000/user/alstylos/data/parquet"

# Read Parquet files into DataFrame
crime_data_df = spark.read.parquet(f"{hdfs_path}/crime_data.parquet", header=True, inferSchema=True)
mo_codes_df = spark.read.parquet(f"{hdfs_path}/mo_codes.parquet", header=True, inferSchema=True)
# mo_codes_df = mo_codes_df.withColumn("mo_code", split(col("value"), " ", 2)[0]).withColumn("mo_description", split(col("value"), " ", 2)[1])
police_stations_df = spark.read.parquet(f"{hdfs_path}/police_stations.parquet", header=True, inferSchema=True)

df_Q4 = (
    crime_data_df.select(
        col("DR_NO"),
        col("AREA ").cast(IntegerType()).alias("AREA"),
        col("Mocodes"),
        explode(split(col("Mocodes"), " ")).alias("Mocode"),
        col("LON").cast(FloatType()).alias("LON"),
        col("LAT").cast(FloatType()).alias("LAT")
    )
    .filter(
        (col("LON") != 0.0) & (col("LAT") != 0.0)
    )
    .join(
        mo_codes_df.select(
            col("mo_code"),
            col("mo_description"),
        ).filter(
            col("mo_description").rlike("gun|weapon|Gun|Weapon")
        ), col("Mocode") == col("mo_code"), "inner"
    )
    .groupBy("DR_NO").agg(
        first("AREA").alias("AREA"),
        first("LON").alias("LON"),
        first("LAT").alias("LAT"),
    )
    .join(
        police_stations_df.select(
            col("PREC").cast(IntegerType()).alias("PREC"),
            col("DIVISION").alias("Division"),
            col("X"),
            col("Y")
        ),
        col("AREA") == col("PREC"), "inner"
    )
    .withColumn(
        "Distance",  
        expr("sqrt(pow((LON - X), 2) + pow((LAT - Y), 2))")
    )
    .groupBy(col("Division"))
    .agg(
        expr("avg(Distance)").alias("Average Distance"),
        count("*").alias("Number of Crimes")
    )
    .orderBy(desc("Number of Crimes"))
)

# Show the DataFrame
df_Q4.show(truncate=False)

# Print the schema of the DataFrame
df_Q4.printSchema()

# Stop the Spark session
spark.stop()
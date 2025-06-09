from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, regexp_replace
import sys

def read_files(file_format):
    target = "hdfs-namenode" 

    if file_format == "parquet":
        # Define the HDFS path to the Parquet file
        hdfs_path = f"hdfs://{target}:9000/user/alstylos/data/parquet"

        # Read Parquet files into DataFrame
        household_income_df = spark.read.parquet(f"{hdfs_path}/household_income.parquet", header=True, inferSchema=True)
        census_population_df = spark.read.parquet(f"{hdfs_path}/census_population.parquet", header=True, inferSchema=True)

        print("+"*50)
        print("Parquet files read successfully.")
        print("+"*50)
    else:
        # Define the HDFS path to the CSV file
        hdfs_path = f"hdfs://{target}:9000/user/root/data"

        # Read CSV files into DataFrame
        household_income_df = spark.read.csv(f"{hdfs_path}/LA_income_2015.csv", header=True, inferSchema=True)
        census_population_df = spark.read.csv(f"{hdfs_path}/2010_Census_Populations_by_Zip_Code.csv", header=True, inferSchema=True)

        print("+"*50)
        print("CSV files read successfully.")
        print("+"*50)

    return household_income_df, census_population_df

# csv or parquet for reading the data
file_format = sys.argv[1]
if file_format not in ["csv", "parquet"]:
    print("Invalid argument. Please use 'csv' or 'parquet'.")
    sys.exit(1)

print("+"*50)
print(f"Reading data in {file_format} format...")
print("+"*50)

# Create a Spark session
spark = SparkSession.builder \
    .appName(f"Query 3 - Dataframe - {file_format} Format") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# read the files based on the format passed as argument
household_income_df, census_population_df = read_files(file_format)

df_Q3 = (
    household_income_df
    .select(
        col("Zip Code"),
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(FloatType()).alias("Estimated Median Income")
    )  # Remove currency formatting and cast to float
    .join(
        census_population_df
        .select(col("Zip Code"), col("Average Household Size")), 
        "Zip Code", "inner"
    )  # Perform an inner join on Zip Code
    .withColumn(
        "Income per Capita", 
        col("Estimated Median Income") / col("Average Household Size")
    )  # Compute per capita income
    .select("Zip Code", "Income per Capita")  # Select final columns
)

# Show the DataFrame
df_Q3.show(truncate=False)

# Print the schema of the DataFrame
df_Q3.printSchema()

# Stop the Spark session
spark.stop()
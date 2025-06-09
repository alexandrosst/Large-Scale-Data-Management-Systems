from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
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
        
    # Convert DataFrames to RDDs
    household_income_rdd = household_income_df.rdd
    census_population_rdd = census_population_df.rdd

    return household_income_rdd, census_population_rdd

def parse_household_income(item):
    return (item["Zip Code"], float(item["Estimated Median Income"][1:].replace(",", "")))

def parse_census_population(item):
    return (item["Zip Code"], item["Average Household Size"])


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
    .appName(f"Query 3 - RDD - {file_format} Format") \
    .getOrCreate()

# Set the log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# read the files based on the format passed as argument
household_income_rdd, census_population_rdd = read_files(file_format)

rdd_Q3 = (
    household_income_rdd.map(parse_household_income)
    .join(census_population_rdd.map(parse_census_population))
    # output format: (zip_code, (income, household_size))
    .mapValues(lambda x: x[0] / x[1])
)

rdd_Q3.collect()
# Convert the RDD to a DataFrame
schema = StructType([
    StructField("Zip Code", IntegerType(), True),
    StructField("Income per Capita", FloatType(), True)
])
# Create a DataFrame from the RDD
df_Q3_from_rdd = rdd_Q3.toDF(schema=schema)

# Show the DataFrame
df_Q3_from_rdd.show(truncate=False)

# Print the schema of the DataFrame
df_Q3_from_rdd.printSchema()

# Stop the Spark session
spark.stop()
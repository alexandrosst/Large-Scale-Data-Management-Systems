from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Create a Spark session
spark = SparkSession.builder \
    .appName("Parquet Files") \
    .getOrCreate()

# Define the HDFS path to the CSV files
hdfs_path_csv = "hdfs://hdfs-namenode:9000/user/root/data"

# Define the paths to the CSV files
crime_data_paths = [f"{hdfs_path_csv}/LA_Crime_Data_2010_2019.csv", f"{hdfs_path_csv}/LA_Crime_Data_2020_2025.csv"]
police_stations_path = f"{hdfs_path_csv}/LA_Police_Stations.csv"
household_income_path = f"{hdfs_path_csv}/LA_income_2015.csv"
census_population_path = f"{hdfs_path_csv}/2010_Census_Populations_by_Zip_Code.csv"
mo_codes_path = f"{hdfs_path_csv}/MO_codes.txt"

# Read CSV file into a DataFrame
crime_data_df = spark.read.csv(crime_data_paths, header=True, inferSchema=True)
police_stations_df = spark.read.csv(police_stations_path, header=True, inferSchema=True)
household_income_df = spark.read.csv(household_income_path, header=True, inferSchema=True)
census_population_df = spark.read.csv(census_population_path, header=True, inferSchema=True)
mo_codes_df = spark.read.text(mo_codes_path).withColumn("mo_code", split(col("value"), " ", 2)[0]).withColumn("mo_description", split(col("value"), " ", 2)[1])

# Define the HDFS path to the Parquet files
hdfs_path = "hdfs://hdfs-namenode:9000/user/alstylos/data/parquet"

# Write DataFrames to Parquet format
crime_data_df.write.parquet(f"{hdfs_path}/crime_data.parquet")
police_stations_df.write.parquet(f"{hdfs_path}/police_stations.parquet")
household_income_df.write.parquet(f"{hdfs_path}/household_income.parquet")
census_population_df.write.parquet(f"{hdfs_path}/census_population.parquet")
mo_codes_df.write.parquet(f"{hdfs_path}/mo_codes.parquet")

# Stop the Spark session
spark.stop()
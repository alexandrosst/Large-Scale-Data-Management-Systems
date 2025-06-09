from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 2 - SQL") \
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

# Register DataFrame as SQL table
crime_data_df.createOrReplaceTempView("crime_data")
police_stations_df.createOrReplaceTempView("police_stations")

# Execute SQL query
Q2_sql_query = """
    WITH base_data AS (
        SELECT 
            CAST(YEAR(TO_TIMESTAMP(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a')) AS INT) AS Year,
            p.DIVISION,
            CASE 
                WHEN `Status Desc` NOT LIKE '%UNK%' AND `Status Desc` NOT LIKE '%Invest Cont%' THEN 1 
                ELSE 0 
            END AS closed_case
        FROM crime_data c
        JOIN police_stations p 
        ON CAST(c.AREA AS INT) = p.PREC
    ),

    aggregated_data AS (
        SELECT 
            Year,
            DIVISION,
            SUM(closed_case) * 100.0 / COUNT(*) AS `Closed-Case Rate %`
        FROM base_data
        GROUP BY Year, DIVISION
    ),

    ranked_data AS (
        SELECT 
            *,
            DENSE_RANK() OVER (PARTITION BY Year ORDER BY `Closed-Case Rate %` DESC) AS Rank
        FROM aggregated_data
    )

    SELECT Year, DIVISION, `Closed-Case Rate %`, Rank
    FROM ranked_data
    WHERE Rank <= 3
    ORDER BY Year, Rank;
"""
df_Q2_sql = spark.sql(Q2_sql_query)


# Show the DataFrame
df_Q2_sql.show(truncate=False)

# Print the schema of the DataFrame
df_Q2_sql.printSchema()

# Stop the Spark session
spark.stop()
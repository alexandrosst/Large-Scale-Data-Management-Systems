# Semester Project
## Introduction
This repository has been created as part of the post-graduate course **Large Scale Data Management Systems**. It serves as the central repository for all code and documentation related to the project undertaken within the scope of this course.

## Description
The repository primarily consists of Python scripts designed to implement the various queries for the project.
- Query 1 ⇝ Implements three variations:
    - `Query_1_df.py`: Utilizes DataFrame API for query execution.
    - `Query_1_df_udf.py`: Incorporates User-Defined Functions (UDFs) within a DataFrame-based approach.
    - `Query_1_rdd.py`: Uses Resilient Distributed Datasets (RDDs) for query processing.
- Query 2 ⇝ Includes three implementations:
    - `Query 2_df.py`: Utilizes DataFrame API for query execution.
    - `Query_2_rdd.py`: Uses Resilient Distributed Datasets (RDDs) for query processing.
    - `Query_2_sql.py`: Executes query using SQL API.
- Query 3 ⇝ Provides two variations:
    - `Query_3_df.py`: Utilizes DataFrame API for query execution.
    - `Query_3_rdd.py`: Uses Resilient Distributed Datasets (RDDs) for query processing.
- Query 4 ⇝ Consists of a single implementation:
    - `Query_4_df.py`: Processes the query using DataFrame API.

Additionally, an auxiliary script, `save_parquet.py`, was executed once to facilitate data transformation. This script read `.csv` files from `hdfs://hdfs-namenode:9000/user/root/data`, converted them into `.parquet` format and stored the resulting files in `hdfs://hdfs-namenode:9000/user/alstylos/data/parquet`. These `.parquet` files support the execution of upcoming queries within the project.
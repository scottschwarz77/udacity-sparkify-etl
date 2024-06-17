# Sparkify ETL Pipeline 

This Udacity Nanodegree project contains the code for a data pipeline that extracts Sparkify data from S3, loads the data to staging tables, and transforms the staged data into data warehouse tables. The S3 data resides in two buckets: one containing songs, and the other containing user activity (event) logs. The data warehouse tables consist of one fact table and four dimensional tables.

## Installation

1. This program requires Python to be installed. Python can be downloaded [here](https://www.python.org/downloads/).

2. From the command line, change to the directory where you want the program to be installed.

3. Clone the [Sparkify repository](https://github.com/scottschwarz77/udacity-sparkify-etl) from Github.

## Contents of the repository

The repository contains two Python programs, `create_tables.py` and `etl.py` that the user can run. These programs read settings from `dwh.cfg` (a configuration file) and use `sql_queries.py`. More details about these four files are provided below.

## Configuration

The `dwh.cfg` file stores the configuration properties that the ETL pipeline needs to run. It is divided into three sections:

* The `CLUSTER` section contains connection details for the cluster, such as the cluster endpoint and the user name/password of the database that resides on the cluster.

* The `S3`section contains the paths to the two buckets containing the song and log data. This section also contains the path to a JSON file, which is the schema for the log data.

* The `IAM_ROLE` section contains the ARN that the Redshift cluster needs to copy the files from S3.

## Running the pipeline

To run the pipeline, follow these steps:

1. Run the script `python3 create_tables.py`. This script drops the staging and data warehouse tables if they already exist. It then creates (or recreates) the staging and data warehouse tables.

  The script references the drop and create table queries in `sql_queries.py`. The tables are:

  * Staging tables: `staging_songs`, `staging_events`

  * Data warehouse tables: `songplays` (fact table), `time`(dimensional table), `artists` (dimensional table), `songs` (dimensional table), `users` (dimensional table).

2. Run the script `python3 etl.py`. This script loads the S3 data to the staging tables, and transforms the staged data to the data warehouse tables. These tables are listed in step 1 above.

  The script references the queries in `sql_queries.py` that are used to load and transform the tables.
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, round as _round, max as _max, when, broadcast
from dotenv import load_dotenv
from pathlib import Path

def get_jdbc_config() -> tuple: 
    """
    Loads PostgreSQL database configuration from the .env file and returns 
    the JDBC URL and JDBC properties.

    This function reads environment variables defined in the .env file 
    that contain database credentials and connection details, and then 
    constructs the JDBC URL and properties required for connecting 
    to the database.

    Returns:
        tuple:
            - jdbc_url (str): The JDBC URL for the database connection.
            - jdbc_properties (dict): The JDBC properties including user, password, and driver.

    Raises:
        EnvironmentError: If one or more required environment variables are missing.
    """

    print("Loading database configuration from .env file...")
    dotenv_path = Path("/opt/app/.env") 
    load_dotenv(dotenv_path=dotenv_path)

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_container_name = os.getenv("POSTGRES_CONTAINER_NAME")
    postgres_dw_db = os.getenv("POSTGRES_DW_DB")
    
    jdbc_url = f"jdbc:postgresql://{postgres_container_name}/{postgres_dw_db}"
    jdbc_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }
    print("Database configuration loaded successfully.")
    return jdbc_url, jdbc_properties


def initialize_spark() -> SparkSession:
    """
    Initializes and returns a SparkSession for the ETL process.

    This function creates a SparkSession with the specified application name
    and sets the Spark context log level to WARN to reduce log verbosity.

    Returns:
        SparkSession: The initialized SparkSession instance that can be used
        for executing Spark jobs and transformations.
    """
   
    print("Initializing Spark Session...")
    spark = SparkSession.builder.appName("ModularCustomerAnalyticsETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized successfully.")
    return spark


def extract_data(spark: SparkSession) -> dict[str, DataFrame]:
    """
    Automatically reads all datasets from the /data/cleaned directory and returns 
    them as a dictionary of DataFrames.

    This function uses the Hadoop API provided by Spark to list all subdirectories
    under /data/cleaned. Each subdirectory is assumed to represent a dataset, and
    its contents are loaded as a Spark DataFrame (Parquet format).

    Parameters:
        spark (SparkSession): The active SparkSession used for reading the data.

    Returns:
        dict[str, DataFrame]: A dictionary where each key is the dataset name 
        (subdirectory name) and the value is the corresponding Spark DataFrame.

    Raises:
        FileNotFoundError: If no subdirectories (datasets) are found in /data/cleaned.
        Exception: If any error occurs during the reading of the Parquet files.
    """

    print("Extracting all datasets automatically from /data/cleaned...")
    base_data_path = "/data/cleaned"
    dataframes = {}
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(base_data_path))
    
    dataset_names = [f.getPath().getName() for f in list_status if f.isDirectory()]
    
    if not dataset_names:
        raise FileNotFoundError(f"No subdirectories found in {base_data_path}")

    print(f"Found datasets: {dataset_names}")

    try:
        for dataset_name in dataset_names:
            path = f"{base_data_path}/{dataset_name}"
            print(f"Reading from: {path}")
            dataframes[dataset_name] = spark.read.parquet(path)
        print("Successfully read all source Parquet files.")
        return dataframes
    except Exception as e:
        print(f"Error during dynamic Extract phase: {e}")
        raise


def transform_data_in_spark(df_loyalty: DataFrame, df_activity: DataFrame) -> DataFrame:
    """
    Performs all transformation logic and churn analysis inside Spark with optimizations.
    Returns a single summarized DataFrame containing aggregated metrics and churn status.

    This function applies:
        - Caching to optimize repeated access to the activity dataset.
        - Broadcast joins to efficiently merge customer profile and activity data.
        - Aggregations to produce lifetime metrics for each customer.
        - Churn status determination based on the last activity year.
        - Calculation of net points.

    Parameters:
        df_loyalty (DataFrame): The Spark DataFrame containing customer loyalty profile data.
        df_activity (DataFrame): The Spark DataFrame containing customer activity/transaction data.

    Returns:
        DataFrame: The transformed and summarized Spark DataFrame with churn status and metrics.

    Raises:
        Exception: Propagates any Spark-related errors during transformation.
    """

    print("Transforming data in Spark with optimizations...")

    # --- OPTIMASI 1: Caching ---
    df_activity.persist()
    print("df_activity has been cached.")

    latest_year = df_activity.agg(_max("year")).collect()[0][0]
    
    df_last_activity = df_activity.groupBy("loyalty_number").agg(_max("year").alias("last_activity_year"))
    
    # --- OPTIMASI 2: Broadcast Join ---
    print("Performing broadcast join between activity and loyalty data...")
    df_customer_profile = df_activity.join(broadcast(df_loyalty), "loyalty_number", "inner")
    
    df_customer_summary = (
        df_customer_profile.groupBy("loyalty_number", "country", "province", "city", "gender", "education", "salary", "marital_status", "loyalty_card")
        .agg(
            _sum("total_flights").alias("lifetime_total_flights"),
            _sum("distance").alias("lifetime_total_distance"),
            _sum("points_accumulated").alias("lifetime_points_accumulated"),
            _sum("points_redeemed").alias("lifetime_points_redeemed"),
            _round(_sum("dollar_cost_points_redeemed"), 2).alias("lifetime_dollar_cost_redeemed")
        )
    )

    print("Performing broadcast join to add last activity year...")
    df_customer_summary = df_customer_summary.join(broadcast(df_last_activity), "loyalty_number", "inner")
    
    df_customer_summary = (
        df_customer_summary.withColumn("status", when(col("last_activity_year") < latest_year, "Churn").otherwise("Retained"))
        .withColumn("net_points", col("lifetime_points_accumulated") - col("lifetime_points_redeemed"))
        .orderBy(col("loyalty_number"))
    )
    
    print("Transformation in Spark complete.")
    df_customer_summary.show(5)
    
    df_activity.unpersist()
    print("df_activity has been unpersisted.")
    
    return df_customer_summary


def load_data_to_postgres(df: DataFrame, table_name: str, jdbc_url: str, jdbc_properties: dict) -> None:
    """
    Saves the provided DataFrame into a PostgreSQL table using JDBC.

    This function writes the DataFrame to the specified PostgreSQL table using
    the provided JDBC URL and connection properties. The write mode is set to "overwrite",
    so the target table will be replaced if it already exists.

    Parameters:
        df (DataFrame): The Spark DataFrame to be written to the database.
        table_name (str): The name of the target table in PostgreSQL.
        jdbc_url (str): The JDBC URL for connecting to the PostgreSQL database.
        jdbc_properties (dict): A dictionary containing JDBC connection properties 
                                such as user, password, and driver.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the write process.
    """

    print(f"Loading data into PostgreSQL table: {table_name}...")
    try:
        df.write.mode("overwrite").jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
        print(f"Successfully loaded data into {table_name}.")
    except Exception as e:
        print(f"Error during Load phase: {e}")
        raise


def transform_data_in_postgres(spark: SparkSession, df_loyalty: DataFrame, df_activity: DataFrame, jdbc_url: str, jdbc_properties: dict) -> DataFrame:
    """
    Executes a pushdown workflow: loads data into staging tables, 
    then performs transformation and aggregation directly within PostgreSQL 
    using a SQL query, and finally loads the transformed data as a Spark DataFrame.

    This function:
        - Loads the provided loyalty and activity DataFrames into PostgreSQL staging tables.
        - Runs a SQL query in PostgreSQL to aggregate and transform the data (pushdown).
        - Reads the transformed result back into Spark as a DataFrame.

    Parameters:
        spark (SparkSession): The active SparkSession used to read the transformed data.
        df_loyalty (DataFrame): The Spark DataFrame containing customer loyalty profile data.
        df_activity (DataFrame): The Spark DataFrame containing customer activity/transaction data.
        jdbc_url (str): The JDBC URL for connecting to the PostgreSQL database.
        jdbc_properties (dict): A dictionary of JDBC connection properties (user, password, driver).

    Returns:
        DataFrame: A Spark DataFrame containing the transformed and aggregated data 
                   queried from PostgreSQL.

    Raises:
        Exception: If any error occurs during the load or SQL transformation process.
    """

    print("Loading data to staging tables...")
    load_data_to_postgres(df_loyalty, "public.staging_loyalty_history", jdbc_url, jdbc_properties)
    load_data_to_postgres(df_activity, "public.staging_flight_activity", jdbc_url, jdbc_properties)

    print("Transforming data using PostgreSQL query...")
    transform_query = """
    (
        WITH latest_year_cte AS (
            SELECT MAX(year) as max_year FROM public.staging_flight_activity
        ),
        last_activity_cte AS (
            SELECT loyalty_number, MAX(year) as last_activity_year
            FROM public.staging_flight_activity GROUP BY loyalty_number
        )
        SELECT
            lh.loyalty_number, lh.country, lh.province, lh.city, lh.gender,
            lh.education, lh.salary, lh.marital_status, lh.loyalty_card,
            SUM(fa.total_flights) AS lifetime_total_flights,
            SUM(fa.distance) AS lifetime_total_distance,
            SUM(fa.points_accumulated) AS lifetime_points_accumulated,
            SUM(fa.points_redeemed) AS lifetime_points_redeemed,
            ROUND(SUM(fa.dollar_cost_points_redeemed)::decimal, 2) AS lifetime_dollar_cost_redeemed,
            la.last_activity_year,
            CASE WHEN la.last_activity_year < (SELECT max_year FROM latest_year_cte) THEN 'Churn' ELSE 'Retained' END AS status,
            (SUM(fa.points_accumulated) - SUM(fa.points_redeemed)) AS net_points
        FROM public.staging_flight_activity fa
        JOIN public.staging_loyalty_history lh ON fa.loyalty_number = lh.loyalty_number
        JOIN last_activity_cte la ON lh.loyalty_number = la.loyalty_number
        GROUP BY 
            lh.loyalty_number, lh.country, lh.province, lh.city, lh.gender,
            lh.education, lh.salary, lh.marital_status, lh.loyalty_card,
            la.last_activity_year
    ) AS transformed_data
    """
    df_transformed_from_db = spark.read.jdbc(url=jdbc_url, table=transform_query, properties=jdbc_properties)
    print("Transformation via SQL successful.")

    return df_transformed_from_db


def verify_data(spark: SparkSession, table_name: str, jdbc_url: str, jdbc_properties: dict) -> None:
    """
    Reads data from the final PostgreSQL table for verification purposes.

    This function:
        - Loads the specified table from PostgreSQL using JDBC.
        - Displays the top 10 rows for visual verification.
        - Displays a summary count of churn vs retained status.
        - Prints the total number of rows in the table.

    Parameters:
        spark (SparkSession): The active SparkSession used to read the data.
        table_name (str): The name of the final table in PostgreSQL to verify.
        jdbc_url (str): The JDBC URL for connecting to the PostgreSQL database.
        jdbc_properties (dict): A dictionary of JDBC connection properties (user, password, driver).

    Returns:
        None

    Raises:
        Exception: If an error occurs during the verification process.
    """

    print(f"\nVerifying data in final table: {table_name}...")
    try:
        df_read_from_pg = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
        
        print("Verification successful. Showing top 10 rows:")
        df_read_from_pg.show(10)

        print("Churn vs. Retained status summary:")
        df_read_from_pg.groupBy("status").count().show()
        
        count = df_read_from_pg.count()
        print(f"Total rows verified in '{table_name}': {count}")

    except Exception as e:
        print(f"Error during Verify phase: {e}")


def main(load_method: str) -> None:
    """
    The main entry point for executing the end-to-end ETL (Extract, Transform, Load) workflow 
    using either Spark-based transformations or PostgreSQL SQL pushdown transformations.

    This function executes the full ETL process, including:
        - Initializing SparkSession.
        - Loading JDBC configuration.
        - Extracting raw data from cleaned directories.
        - Applying transformations (either Spark-based or PostgreSQL pushdown, depending on the method).
        - Loading the final summarized data into PostgreSQL.
        - Verifying the loaded data.

    The load method determines the transformation approach:
        - 'spark_jdbc': Transformation is done in Spark, and results are loaded to PostgreSQL.
        - 'postgres_pushdown': Transformation is pushed down to PostgreSQL using SQL.

    Parameters:
        load_method (str): The ETL load method to use. 
                           Must be either 'spark_jdbc' or 'postgres_pushdown'.

    Returns:
        None

    Raises:
        ValueError: If an invalid load method is provided.
        Exception: If any error occurs during the ETL process.
    """

    workflows = {
        "spark_jdbc": {
            "transform_function": transform_data_in_spark,
            "final_table_name": "public.customer_summary_with_churn_spark"
        },
        "postgres_pushdown": {
            "transform_function": transform_data_in_postgres,
            "final_table_name": "public.customer_summary_with_churn_sql"
        }
    }

    if load_method not in workflows:
        raise ValueError(f"Invalid load method '{load_method}'. Choose from {list(workflows.keys())}.")

    spark = None
    try:
        spark = initialize_spark()
        jdbc_url, jdbc_properties = get_jdbc_config()
        
        extracted_dfs = extract_data(spark)
        
        df_loyalty = extracted_dfs["customer_loyalty_history"]
        df_activity = extracted_dfs["customer_flight_activity"]

        workflow_config = workflows[load_method]
        transform_function = workflow_config["transform_function"]
        table_to_verify = workflow_config["final_table_name"]
        
        if load_method == 'spark_jdbc':
            df_summary = transform_function(df_loyalty, df_activity)
            load_data_to_postgres(df_summary, table_to_verify, jdbc_url, jdbc_properties)
        else:
            df_summary = transform_function(spark, df_loyalty, df_activity, jdbc_url, jdbc_properties)
            load_data_to_postgres(df_summary, table_to_verify, jdbc_url, jdbc_properties)
        
        verify_data(spark, table_to_verify, jdbc_url, jdbc_properties)
        
        print("\nETL job finished successfully.")

    except Exception as e:
        print(f"An error occurred in the main ETL job: {e}")
        exit(1)
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        load_method_arg = sys.argv[1]
        main(load_method_arg)
    else:
        print("No load method provided. Defaulting to 'spark_jdbc'.")
        main('spark_jdbc')

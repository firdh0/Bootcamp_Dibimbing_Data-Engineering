from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, round as _round, max as _max, when, broadcast

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


def transform_data_in_postgres(spark: SparkSession, jdbc_url: str, jdbc_properties: dict) -> DataFrame:
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
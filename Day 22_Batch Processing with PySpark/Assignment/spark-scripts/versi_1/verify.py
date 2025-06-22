from pyspark.sql import SparkSession

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
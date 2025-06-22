from pyspark.sql import DataFrame

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
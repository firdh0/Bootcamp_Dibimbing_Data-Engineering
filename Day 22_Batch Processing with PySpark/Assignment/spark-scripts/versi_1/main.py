import sys
from config import get_jdbc_config
from spark_utils import initialize_spark
from extract import extract_data
from transform import transform_data_in_spark, transform_data_in_postgres
from load import load_data_to_postgres
from verify import verify_data

def main(load_method: str) -> None:
    """
    Main entry point for executing the ETL workflow.

    Depending on the selected load method, this function:
        - Initializes Spark and loads JDBC config.
        - Extracts input datasets (loyalty and activity).
        - Transforms the data using either Spark or PostgreSQL.
        - Loads the transformed summary to a PostgreSQL table.
        - Verifies the output by reading the final table.

    Supported load methods:
        - 'spark_jdbc': Transformation is performed in Spark.
        - 'postgres_pushdown': Raw data is loaded into staging tables, and transformation
          is executed via SQL query in PostgreSQL.

    Parameters:
        load_method (str): The transformation method to use ('spark_jdbc' or 'postgres_pushdown').

    Returns:
        None

    Raises:
        ValueError: If an invalid load method is passed.
        Exception: For any failure during the ETL process.
    """

    spark = None
    try:
        spark = initialize_spark()
        jdbc_url, jdbc_properties = get_jdbc_config()

        extracted_dfs = extract_data(spark)
        df_loyalty = extracted_dfs["customer_loyalty_history"]
        df_activity = extracted_dfs["customer_flight_activity"]

        if load_method == 'spark_jdbc':
            final_table_name = "public.customer_summary_with_churn_spark"
            df_summary = transform_data_in_spark(df_loyalty, df_activity)
            load_data_to_postgres(df_summary, final_table_name, jdbc_url, jdbc_properties)

        elif load_method == 'postgres_pushdown':
            final_table_name = "public.customer_summary_with_churn_sql"
            print("Loading data to staging tables for SQL pushdown...")
            load_data_to_postgres(df_loyalty, "public.staging_loyalty_history", jdbc_url, jdbc_properties)
            load_data_to_postgres(df_activity, "public.staging_flight_activity", jdbc_url, jdbc_properties)
            df_summary = transform_data_in_postgres(spark, jdbc_url, jdbc_properties)
            load_data_to_postgres(df_summary, final_table_name, jdbc_url, jdbc_properties)

        else:
            raise ValueError(f"Invalid load method '{load_method}'. Choose 'spark_jdbc' or 'postgres_pushdown'.")

        verify_data(spark, final_table_name, jdbc_url, jdbc_properties)
        print("\nETL job completed successfully.")

    except Exception as e:
        print(f"An error occurred during the main ETL job: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")


if __name__ == '__main__':
    """
    CLI entry point for running the ETL pipeline.

    Accepts an optional command-line argument for the load method:
        - 'spark_jdbc' (default)
        - 'postgres_pushdown'

    Example:
        python main.py spark_jdbc
    """
    if len(sys.argv) > 1:
        load_method_arg = sys.argv[1]
    else:
        print("No load method provided. Defaulting to 'spark_jdbc'.")
        load_method_arg = 'spark_jdbc'

    main(load_method_arg)
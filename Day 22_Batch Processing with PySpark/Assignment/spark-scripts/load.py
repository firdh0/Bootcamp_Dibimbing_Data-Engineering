import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pathlib import Path

def main():
    """
    Tahap Load: Membaca data yang telah ditransformasi dan menyimpannya ke PostgreSQL.
    """
    spark = SparkSession.builder.appName("ETL_Load").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Load job started.")

    dotenv_path = Path("/opt/app/.env") 
    load_dotenv(dotenv_path=dotenv_path)

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_container_name = os.getenv("POSTGRES_CONTAINER_NAME")
    postgres_dw_db = os.getenv("POSTGRES_DW_DB")

    input_path = "/data/processed/transformed/customer_summary"
    table_name = "public.customer_summary_with_churn"
    
    try:
        df_to_load = spark.read.parquet(input_path)
        print("Successfully read transformed data.")

        jdbc_url = f"jdbc:postgresql://{postgres_container_name}/{postgres_dw_db}"
        jdbc_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver",
        }

        print(f"Writing data to PostgreSQL table: {table_name}")
        df_to_load.write.mode("overwrite").jdbc(
            url=jdbc_url, table=table_name, properties=jdbc_properties
        )
        print("Successfully loaded data into PostgreSQL.")

    except Exception as e:
        print(f"Error during load phase: {e}")
        spark.stop()
        exit(1)
        
    spark.stop()
    print("Load job finished.")

if __name__ == '__main__':
    main()
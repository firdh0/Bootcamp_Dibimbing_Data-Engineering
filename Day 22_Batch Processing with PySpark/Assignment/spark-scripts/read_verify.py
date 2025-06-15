from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pathlib import Path

def main():
    """
    Tahap Verifikasi: Membaca data yang telah dimuat ke PostgreSQL.
    """
    spark = SparkSession.builder.appName("ETL_Verify").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Verify job started.")

    dotenv_path = Path("/opt/app/.env") 
    load_dotenv(dotenv_path=dotenv_path)

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_container_name = os.getenv("POSTGRES_CONTAINER_NAME")
    postgres_dw_db = os.getenv("POSTGRES_DW_DB")

    table_name = "public.customer_summary_with_churn"
    
    try:
        jdbc_url = f"jdbc:postgresql://{postgres_container_name}/{postgres_dw_db}"
        jdbc_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver",
        }

        print(f"Reading data back from PostgreSQL table: {table_name}")
        df_read_from_pg = spark.read.jdbc(
            url=jdbc_url, table=table_name, properties=jdbc_properties
        )
        
        count = df_read_from_pg.count()
        print(f"Verification successful. Found {count} rows in '{table_name}'.")
        df_read_from_pg.show(10)

    except Exception as e:
        print(f"Error during verify phase: {e}")
        spark.stop()
        exit(1)

    spark.stop()
    print("Verify job finished.")

if __name__ == '__main__':
    main()

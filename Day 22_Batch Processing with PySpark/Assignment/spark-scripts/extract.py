from pyspark.sql import SparkSession

def main():
    """
    Tahap Ekstrak: Membaca data sumber dari Parquet dan menyimpannya ke lokasi sementara.
    """
    spark = SparkSession.builder.appName("ETL_Extract").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Extract job started.")

    base_data_path = "/data/cleaned"
    output_path = "/data/processed/extracted"

    try:
        df_loyalty = spark.read.parquet(f"{base_data_path}/customer_loyalty_history")
        df_activity = spark.read.parquet(f"{base_data_path}/customer_flight_activity")
        print("Successfully read source Parquet files.")
        
        df_loyalty.write.mode("overwrite").parquet(f"{output_path}/loyalty")
        df_activity.write.mode("overwrite").parquet(f"{output_path}/activity")
        print(f"Extracted data successfully written to {output_path}")

    except Exception as e:
        print(f"Error during extract phase: {e}")
        spark.stop()
        exit(1)
    
    spark.stop()
    print("Extract job finished.")

if __name__ == '__main__':
    main()
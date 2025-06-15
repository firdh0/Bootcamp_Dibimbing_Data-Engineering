from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round, max as _max, when

def main():
    """
    Tahap Transform: Membaca data dari lokasi sementara, melakukan transformasi,
    dan menyimpan hasilnya ke lokasi sementara lainnya.
    """
    spark = SparkSession.builder.appName("ETL_Transform").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Transform job started.")

    input_path = "/data/processed/extracted"
    output_path = "/data/processed/transformed/customer_summary"

    try:
        df_loyalty = spark.read.parquet(f"{input_path}/loyalty")
        df_activity = spark.read.parquet(f"{input_path}/activity")
        print("Successfully read extracted data from temporary storage.")

        latest_year = df_activity.agg(_max("year")).collect()[0][0]
        print(f"Latest year in dataset is: {latest_year}")

        df_last_activity = df_activity.groupBy("loyalty_number").agg(_max("year").alias("last_activity_year"))
        
        df_customer_profile = df_activity.join(df_loyalty, "loyalty_number", "inner")
        
        df_customer_summary = (
            df_customer_profile.groupBy(
                "loyalty_number", "country", "province", "city", 
                "gender", "education", "salary", "marital_status", "loyalty_card"
            ).agg(
                _sum("total_flights").alias("lifetime_total_flights"),
                _sum("distance").alias("lifetime_total_distance"),
                _sum("points_accumulated").alias("lifetime_points_accumulated"),
                _sum("points_redeemed").alias("lifetime_points_redeemed"),
                _round(_sum("dollar_cost_points_redeemed"), 2).alias("lifetime_dollar_cost_redeemed")
            )
        )

        df_customer_summary = df_customer_summary.join(df_last_activity, "loyalty_number", "inner")
        
        df_customer_summary = (
            df_customer_summary.withColumn(
                "status",
                when(col("last_activity_year") < latest_year, "Churn").otherwise("Retained")
            )
            .withColumn(
                "net_points",
                col("lifetime_points_accumulated") - col("lifetime_points_redeemed")
            )
            .orderBy(col("loyalty_number"))
        )
        
        print("Transformation and aggregation complete.")
        
        df_customer_summary.write.mode("overwrite").parquet(output_path)
        print(f"Transformed data successfully written to {output_path}")

    except Exception as e:
        print(f"Error during transform phase: {e}")
        spark.stop()
        exit(1)
        
    spark.stop()
    print("Transform job finished.")

if __name__ == '__main__':
    main()
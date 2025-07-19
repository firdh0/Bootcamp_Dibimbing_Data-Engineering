import argparse
from spark_session_manager import SparkSessionManager 
from spark_transformer import SparkTransformer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark job for GoFood Bronze to Silver transformation.")
    parser.add_argument("--gcs_input_path", required=True, help="GCS path untuk folder input Bronze.")
    parser.add_argument("--gcs_output_path", required=True, help="GCS path untuk folder output Silver.")
    parser.add_argument("--bigquery_project_id", required=True, help="ID Proyek BigQuery.")
    parser.add_argument("--bigquery_dataset_id", required=True, help="ID Dataset BigQuery.")
    parser.add_argument("--run_date_str", required=True, help="Tanggal eksekusi DAG (format YYYY-MM-DD atau YYYY-MM-DD/HH-mm-ss).")
    args = parser.parse_args()

    spark_manager = SparkSessionManager(app_name="GoFood_Bronze_to_Silver_Transformation")
    spark = spark_manager.get_session()
    
    try:
        transformer = SparkTransformer(
            spark=spark,
            gcs_input_path=args.gcs_input_path,
            gcs_output_path=args.gcs_output_path,
            bigquery_project_id=args.bigquery_project_id,
            bigquery_dataset_id=args.bigquery_dataset_id,
            run_date_str=args.run_date_str
        )
        
        transformer.run()
        
        print("\n🎉 The Bronze to Silver transformation process has been successfully completed.")
        
    except Exception as e:
        print(f"\n❌ An error occurred during the transformation process: {e}")
        raise
        
    finally:
        spark_manager.stop_session()

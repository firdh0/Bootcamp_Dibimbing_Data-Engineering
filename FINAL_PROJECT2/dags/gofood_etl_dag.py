from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from docker.types import Mount
from send_notification_to_email import send_dag_notification

GCS_BUCKET = "gofood-data-lake-bucket"
BRONZE_GCS_PATH = "bronze"
SILVER_GCS_PATH = "silver"
GOLD_GCS_PATH = "gold"

BIGQUERY_PROJECT_ID = "gofood-465817" 
BIGQUERY_DATASET_ID = "gofood_analytics"

GCP_KEYFILE_ON_HOST = "/c/Users/LEGION/Music/GitHub/Bootcamp_Dibimbing_Data-Engineering/FINAL_PROJECT2/secret/gcp_credentials.json" 
GCP_KEYFILE_IN_SCRAPER = "/tmp/gcp_credentials.json"
GCP_KEYFILE_IN_CONTAINER = "/opt/airflow/secret/gcp_credentials.json"

run_datetime_str_full = "{{ data_interval_start.in_timezone('Asia/Jakarta').format('YYYY-MM-DD/HH-mm-ss') }}"
run_date_str_only_date = "{{ data_interval_start.in_timezone('Asia/Jakarta').format('YYYY-MM-DD') }}"
destination_gcs_path = f"{BRONZE_GCS_PATH}/{run_datetime_str_full}"

scraper_command = (
    f"python main.py "
    f"--gcs-bucket {GCS_BUCKET} "
    f"--gcs-path-prefix {destination_gcs_path}"
)

@dag(
    dag_id="gofood_etl_medallion_pipeline",
    schedule="0 8-9 * * *",
    start_date=pendulum.datetime(2025, 7, 19, tz="Asia/Jakarta"), 
    catchup=True, 
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    tags=["gofood", "spark", "gcp", "medallion"],
    doc_md="""
    ### GoFood ETL Pipeline with Medallion Architecture on GCS
    This DAG runs multiple times daily and saves each execution result uniquely in GCS
    using the format `YYYY-MM-DD/HH-mm-ss` in Jakarta timezone (WIB).
    """
)
def gofood_etl_medallion_dag() -> None:
    """
    Defines an end-to-end ETL pipeline for GoFood data using the Medallion Architecture.

    The pipeline consists of the following steps:

    1. **Extraction (Bronze Layer)**:
        - Scrape GoFood data using a Dockerized scraper and store the raw data in GCS under the 'bronze' layer.

    2. **Transformation (Silver Layer)**:
        - Use Spark to clean and transform the bronze data into structured silver data.
        - Write the output back to GCS in the 'silver' layer.

    3. **Loading (Gold Layer / BigQuery)**:
        - Append new fact data and date dimensions directly to BigQuery.
        - For slowly changing dimensions like restaurant, menu, and promotions, perform merge operations using staging tables to ensure data consistency.

    This pipeline ensures proper data lineage, historical tracking, and deduplication
    by leveraging the Medallion data model and BigQuery's MERGE capabilities.

    **Parameters:**
        None (All parameters are defined within the DAG context)

    **Returns:**
        None

    **Raises:**
        AirflowException: If any task fails during execution.
    """

    extract_to_bronze = DockerOperator(
        task_id="extract_and_upload_to_bronze",
        image="gofood-scraper:latest", 
        command=scraper_command,
        docker_url="unix://var/run/docker.sock", 
        network_mode="bridge",
        auto_remove=True, #
        mounts=[ 
            Mount(
                source=GCP_KEYFILE_ON_HOST,
                target=GCP_KEYFILE_IN_SCRAPER,
                type="bind",
                read_only=True
            )
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": GCP_KEYFILE_IN_SCRAPER},
        do_xcom_push=False, 
        mount_tmp_dir=False 
    )

    transform_gcs_input_path = f"gs://{GCS_BUCKET}/{BRONZE_GCS_PATH}/{run_datetime_str_full}"
    transform_gcs_output_path = f"gs://{GCS_BUCKET}/{SILVER_GCS_PATH}/{run_datetime_str_full}"

    transform_bronze_to_silver = SparkSubmitOperator(
        task_id="transform_bronze_to_silver",
        conn_id="spark_main", 
        application="/spark-scripts/main.py", 
        conf={ 
            "spark.driver.host": "dataeng-airflow-scheduler",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile": GCP_KEYFILE_IN_CONTAINER,
            "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0" 
        },
        jars="/opt/airflow/jars/gcs-connector-hadoop3-2.2.9-shaded.jar", 
        application_args=[ 
            "--gcs_input_path", transform_gcs_input_path,
            "--gcs_output_path", transform_gcs_output_path,
            "--bigquery_project_id", BIGQUERY_PROJECT_ID,
            "--bigquery_dataset_id", BIGQUERY_DATASET_ID,
            "--run_date_str", run_date_str_only_date,
        ]
    )

    append_tables = {
        "fact_transaction": {"id_column": "TransactionID"},
        "dim_date": {"id_column": "id_time"} 
    }

    merge_tables = {
        "dim_restaurant": {"business_key": "name", "id_column": "id_restaurant"},
        "dim_menu": {"business_key": "name", "id_column": "id_menu"},
        "dim_promotion": {"business_key": "name", "id_column": "id_promo"} 
    }

    load_append_tables_tasks = []
    for table_name, details in append_tables.items():
        load_task = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[f"{SILVER_GCS_PATH}/{run_datetime_str_full}/{table_name}/*.parquet"], 
            destination_project_dataset_table=f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}",
            source_format="PARQUET",
            create_disposition="CREATE_IF_NEEDED", 
            write_disposition="WRITE_APPEND", 
            autodetect=True, 
            gcp_conn_id="gcp_connection",
        )
        load_append_tables_tasks.append(load_task)

    initialize_merge_tables_tasks = []
    for table_name, details in merge_tables.items():
        init_task = GCSToBigQueryOperator(
            task_id=f"initialize_{table_name}_table",
            bucket=GCS_BUCKET,
            source_objects=[f"{SILVER_GCS_PATH}/{run_datetime_str_full}/{table_name}/*.parquet"],
            destination_project_dataset_table=f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}",
            source_format="PARQUET",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE", 
            autodetect=True,
            gcp_conn_id="gcp_connection",
        )
        initialize_merge_tables_tasks.append(init_task)

    actual_merge_tasks = []
    for table_name, details in merge_tables.items():
        staging_table = f"{table_name}_staging" 
        business_key = details["business_key"]
        id_column = details["id_column"]

        load_to_staging = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_to_staging",
            bucket=GCS_BUCKET,
            source_objects=[f"{SILVER_GCS_PATH}/{run_datetime_str_full}/{table_name}/*.parquet"],
            destination_project_dataset_table=f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{staging_table}",
            source_format="PARQUET",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE", 
            autodetect=True,
            gcp_conn_id="gcp_connection",
        )

        update_cols = ""
        insert_cols_names = ""
        insert_cols_values = ""
        merge_sql = ""

        if table_name == "dim_promotion":
            update_cols = """
                T.status = S.status,
                T.active_until_date = S.active_until_date,
                T.description = S.description,
                T.discount_percentage = S.discount_percentage,
                T.max_discount = S.max_discount,
                T.min_purchase = S.min_purchase,
                T.additional_discount = S.additional_discount,
                T.delivery_discount = S.delivery_discount
            """
            insert_cols_names = "id_promo, name, description, discount_percentage, max_discount, min_purchase, additional_discount, delivery_discount, status, active_until_date"
            insert_cols_values = "S.id_promo, S.name, S.description, S.discount_percentage, S.max_discount, S.min_purchase, S.additional_discount, S.delivery_discount, S.status, S.active_until_date"
            
            merge_sql = f"""
                MERGE INTO `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}` T
                USING `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{staging_table}` S
                ON T.name = S.name
                AND T.description IS NOT DISTINCT FROM S.description -- Tambahkan kolom yang membentuk keunikan, handle NULLs
                AND T.discount_percentage IS NOT DISTINCT FROM S.discount_percentage
                AND T.max_discount IS NOT DISTINCT FROM S.max_discount
                AND T.min_purchase IS NOT DISTINCT FROM S.min_purchase
                AND T.additional_discount IS NOT DISTINCT FROM S.additional_discount
                AND T.delivery_discount IS NOT DISTINCT FROM S.delivery_discount
                WHEN MATCHED THEN
                  UPDATE SET {update_cols}
                WHEN NOT MATCHED BY TARGET THEN
                  INSERT ({insert_cols_names})
                  VALUES ({insert_cols_values})
                WHEN NOT MATCHED BY SOURCE AND T.status = 'aktif' THEN
                  UPDATE SET
                    T.status = 'tidak aktif',
                    T.active_until_date = PARSE_DATE('%Y-%m-%d', '{run_date_str_only_date}')
            """
        elif table_name == "dim_restaurant":
            update_cols = """
                T.avg_rating = S.avg_rating,
                T.total_reviews = S.total_reviews,
                T.distance = S.distance,
                T.address = S.address,
                T.city = S.city,
                T.price_range = S.price_range
            """
            insert_cols_names = "id_restaurant, name, avg_rating, total_reviews, distance, address, city, price_range"
            insert_cols_values = "S.id_restaurant, S.name, S.avg_rating, S.total_reviews, S.distance, S.address, S.city, S.price_range"
            
            merge_sql = f"""
                MERGE INTO `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}` T
                USING `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{staging_table}` S
                ON T.name = S.name
                WHEN MATCHED THEN
                  UPDATE SET {update_cols}
                WHEN NOT MATCHED BY TARGET THEN
                  INSERT ({insert_cols_names})
                  VALUES ({insert_cols_values})
            """
        elif table_name == "dim_menu":
            update_cols = """
                T.category = S.category,
                T.description = S.description,
                T.avg_rating = S.avg_rating
            """
            insert_cols_names = "id_menu, name, category, description, avg_rating"
            insert_cols_values = "S.id_menu, S.name, S.category, S.description, S.avg_rating"

            merge_sql = f"""
                MERGE INTO `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}` T
                USING `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{staging_table}` S
                ON T.name = S.name
                WHEN MATCHED THEN
                  UPDATE SET {update_cols}
                WHEN NOT MATCHED BY TARGET THEN
                  INSERT ({insert_cols_names})
                  VALUES ({insert_cols_values})
            """

        actual_merge_task = BigQueryInsertJobOperator(
            task_id=f"merge_{table_name}_table",
            project_id=BIGQUERY_PROJECT_ID, 
            location="asia-southeast2",
            configuration={ 
                "query": {
                    "query": merge_sql,
                    "useLegacySql": False,
                    "jobReference": { 
                        "projectId": BIGQUERY_PROJECT_ID,
                        "jobId": f"bq_merge_job_{{{{ dag_run.id }}}}_{{{{ task.task_id }}}}_{table_name}",
                        "location": "asia-southeast2",
                    },
                    "timeoutMs": 3600 * 1000, 
                }
            },
            gcp_conn_id="gcp_connection",
        )
        actual_merge_tasks.append((load_to_staging, actual_merge_task))

    extract_to_bronze >> transform_bronze_to_silver

    transform_bronze_to_silver >> load_append_tables_tasks 
    
    transform_bronze_to_silver >> initialize_merge_tables_tasks

    for load_staging_task, merge_task in actual_merge_tasks:
        initialize_merge_tables_tasks >> load_staging_task >> merge_task

gofood_etl_medallion_dag()

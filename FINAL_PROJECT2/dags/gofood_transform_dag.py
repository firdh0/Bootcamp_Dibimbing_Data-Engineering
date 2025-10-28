from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from send_notification_to_email import send_dag_notification

GCS_BUCKET = "gofood-data-lake-bucket"
BRONZE_GCS_PATH = "bronze"
SILVER_GCS_PATH = "silver"

BIGQUERY_PROJECT_ID = "gofood-465817" 
BIGQUERY_DATASET_ID = "gofood_analytics"

GCP_KEYFILE_IN_CONTAINER = "/opt/airflow/secret/gcp_credentials.json"

run_datetime_str_full = "{{ dag_run.conf.get('run_datetime_str_full', '') }}"
run_date_str_only_date = "{{ macros.datetime.strptime(dag_run.conf.get('run_datetime_str_full', '1970-01-01/00-00-00').split('/')[0], '%Y-%m-%d').strftime('%Y-%m-%d') }}"

@dag(
    dag_id="gofood_medallion_transform_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Jakarta"), 
    catchup=True, 
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    tags=["gofood", "transform", "medallion", "silver", "cloud storage", "spark", "bigquery"],
    doc_md="""
    ### GoFood ETL Pipeline with Medallion Architecture on GCS
    This DAG runs multiple times daily and saves each execution result uniquely in GCS
    using the format `YYYY-MM-DD/HH-mm-ss` in Jakarta timezone (WIB).
    """
)
def gofood_transform_dag() -> None:
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

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_to_gold_dag",
        trigger_dag_id="gofood_medallion_load_pipeline",
        wait_for_completion = True,
        poke_interval       = 5,
        conf={
            "run_datetime_str_full": run_datetime_str_full,
            "run_date_str_only_date": run_date_str_only_date
        }
    )

    transform_bronze_to_silver >> trigger_load_dag

gofood_transform_dag()

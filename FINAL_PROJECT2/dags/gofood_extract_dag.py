from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount

GCS_BUCKET = "gofood-data-lake"
BRONZE_GCS_PATH = "bronze"

GCP_KEYFILE_ON_HOST = "C:/Users/LEGION/Music/Bootcamp_Dibimbing_Data-Engineering/FINAL_PROJECT2/secret/gcp_credentials.json" 
GCP_KEYFILE_IN_SCRAPER = "/tmp/gcp_credentials.json"

run_datetime_str_full = "{{ data_interval_start.in_timezone('Asia/Jakarta').format('YYYY-MM-DD/HH-mm-ss') }}"
run_date_str_only_date = "{{ data_interval_start.in_timezone('Asia/Jakarta').format('YYYY-MM-DD') }}"
destination_gcs_path = f"{BRONZE_GCS_PATH}/{run_datetime_str_full}"

scraper_command = (
    f"python main.py "
    f"--gcs-bucket {GCS_BUCKET} "
    f"--gcs-path-prefix {destination_gcs_path}"
)

@dag(
    dag_id="gofood_medallion_extract_pipeline",
    # schedule="0 8-9 * * *",
    schedule=None,
    start_date=None, 
    # start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Jakarta"), 
    catchup=True, 
    tags=["gofood", "transform", "medallion", "bronze", "cloud storage"],
    doc_md="""
    ### GoFood ETL Pipeline with Medallion Architecture on GCS
    This DAG runs multiple times daily and saves each execution result uniquely in GCS
    using the format `YYYY-MM-DD/HH-mm-ss` in Jakarta timezone (WIB).
    """
)
def gofood_extract_dag() -> None:
    extract_to_bronze = DockerOperator(
        task_id="extract_and_upload_to_bronze",
        image="gofood-scraper:latest", 
        command=scraper_command,
        docker_url="unix://var/run/docker.sock", 
        network_mode="bridge",
        auto_remove=True, 
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
        mount_tmp_dir=False,
        shm_size=f"{5}g"
    )

    # trigger_transform_dag = TriggerDagRunOperator(
    #     task_id="trigger_transform_to_silver_dag",
    #     trigger_dag_id="gofood_medallion_transform_pipeline",  
    #     wait_for_completion = True,
    #     poke_interval       = 5,
    #     conf={"run_datetime_str_full": run_datetime_str_full} 
    # )

    # extract_to_bronze >> trigger_transform_dag

gofood_extract_dag()
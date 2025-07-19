from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from docker.types import Mount
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

@dag(
    dag_id="gofood_scraping_via_docker",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 7, 10, tz="Asia/Jakarta"),
    catchup=False,
    tags=["gofood", "scraping", "docker"],
    doc_md="""
    ### DAG Scraping GoFood via DockerOperator
    DAG ini menjalankan skrip scraping di dalam kontainer Docker yang terisolasi
    dan menyimpan hasilnya ke folder /data di host.
    """
)
def gofood_scraping_docker_dag():
    """
    Mengorkestrasi tugas scraping menggunakan DockerOperator.
    """
    
    data_mount = Mount(
        source="/c/Users/LEGION/Music/GitHub/Bootcamp_Dibimbing_Data-Engineering/FINAL_PROJECT/data",
        target="/app/data",
        type="bind"
    )

    extract_task = DockerOperator(
        task_id="run_gofood_scraper_in_container",
        image="dataeng-dibimbing/scraper", 
        command="python /app/scripts/extract/scrape_gofood_data.py", 
        docker_url="unix://var/run/docker.sock", 
        network_mode="bridge",
        auto_remove=True, 
        mounts=[data_mount] #
    )

    transform_task = SparkSubmitOperator(
        task_id="transform_gofood_data_with_spark",
        conn_id="spark_main", 
        application="/spark-scripts/transform_gofood_data.py", 
        
    )

    extract_task >> transform_task

gofood_scraping_docker_dag()
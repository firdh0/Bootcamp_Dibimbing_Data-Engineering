from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.baseoperator import BaseOperator

# Konfigurasi terpusat untuk setiap tahap ETL.
ETL_STAGES_CONFIG = [
    {
        "stage": "extract",
        "script": "extract.py",
    },
    {
        "stage": "transform",
        "script": "transform.py",
    },
    {
        "stage": "load",
        "script": "load.py",
    },
    {
        "stage": "verify",
        "script": "read_verify.py",
    },
]

with DAG(
    dag_id="customer_analytics_etl_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    doc_md="""
    ### Customer Analytics Modular ETL DAG (Dynamic)
    
    DAG ini menjalankan proses ETL dalam empat tahap terpisah yang dibuat secara dinamis dari sebuah konfigurasi.
    Setiap tahap menggunakan direktori /data/processed sebagai penyimpanan sementara antar-tugas.
    """,
    tags=["assignment", "pyspark", "etl", "modular", "dynamic"],
) as dag:
    
    previous_task: BaseOperator | None = None
    
    # Loop melalui konfigurasi untuk membuat setiap tugas secara dinamis
    for config in ETL_STAGES_CONFIG:
        stage_name = config["stage"]
        
        task = SparkSubmitOperator(
            task_id=f"run_{stage_name}_job",
            application=f"/spark-scripts/{config['script']}", # Path disesuaikan
            conn_id="spark_main",
            packages="org.postgresql:postgresql:42.3.8",
        )
        
        if previous_task:
            previous_task >> task
            
        previous_task = task

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Mendefinisikan DAG untuk proses ETL Customer Analytics
with DAG(
    dag_id="customer_analytics_etl_dag_2",
    schedule="@daily",  # Menjadwalkan DAG untuk berjalan setiap hari
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    doc_md="""
    ### Customer Analytics ETL DAG
    
    DAG ini menjalankan proses ETL pada data pelanggan yang telah dibersihkan:
    1.  **Extract**: Membaca data Parquet (aktivitas, loyalitas, kalender).
    2.  **Transform**: Menggabungkan dataset dan menghitung ringkasan per pelanggan (total penerbangan, jarak, poin).
    3.  **Load**: Menyimpan hasil agregasi ke dalam tabel di Data Warehouse PostgreSQL.
    """,
    tags=["assignment", "pyspark", "etl"],
) as dag:
    # Operator untuk menjalankan skrip PySpark
    run_customer_analytics_job = SparkSubmitOperator(
        task_id="run_customer_analytics_job_2",
        # Path ini menunjuk ke skrip PySpark di dalam kontainer
        application="/spark-scripts/assignment_etl.py",
        # Menggunakan koneksi 'spark_main' yang dibuat oleh entrypoint.sh
        conn_id="spark_main",
        packages="org.postgresql:postgresql:42.3.8",
    )

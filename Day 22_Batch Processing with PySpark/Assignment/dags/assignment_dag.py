from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

try:
    from send_notification_to_email import send_dag_notification
except ImportError:
    logging.warning("File send_notification_to_email.py tidak ditemukan. Notifikasi email dinonaktifkan.")


# SPARK_APP_PATH = "/opt/airflow/spark-scripts/versi_1"
REQUIRED_PY_FILES = [
    "config.py",
    "spark_utils.py",
    "extract.py",
    "transform.py",
    "load.py",
    "verify.py",
]

PY_FILES_STRING = ",".join([f"/spark-scripts/versi_1/{file}" for file in REQUIRED_PY_FILES])

@dag(
    dag_id="customer_analytics_etl_dag_1",
    schedule=None,
    start_date=pendulum.datetime(2025, 6, 21, tz="UTC"),
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    catchup=False,
    doc_md="""
    ### Customer Analytics Dynamic ETL DAG (TaskFlow API Version)

    This DAG fully utilizes the TaskFlow API and has a Start -> Job -> End structure.
    
    You can choose the 'Load' method when triggering the DAG:
    - **spark_jdbc**: Transformations are done in Spark.
    - **postgres_pushdown**: Transformations are done with a query in PostgreSQL.

    Use the 'Trigger DAG w/ config' menu to choose the method.
    """,
    params={
        "load_method": Param(
            "spark_jdbc", 
            type="string",
            title="Load Method",
            description="Select a method to perform the transformation and load the data.",
            enum=["spark_jdbc", "postgres_pushdown"],
        )
    },
    tags=["assignment", "pyspark", "postgresql", "etl", "notification_to_email", "modular"],
)
def customer_analytics_etl_pipeline() -> None:
    """
    Defines the Customer Analytics ETL DAG using Airflow's TaskFlow API.

    The pipeline includes:
        - A start marker task.
        - A Spark job submission task to run a PySpark ETL script with configurable parameters.
        - An end marker task.

    The DAG supports two transformation modes:
        - 'spark_jdbc': Transformation is done in Spark.
        - 'postgres_pushdown': Transformation is delegated to PostgreSQL using SQL.

    Returns:
        None
    """

    start = EmptyOperator(task_id="start_pipeline_1")

    spark_etl = SparkSubmitOperator(
        task_id="etl_pipeline_1",
        application=f"/spark-scripts/versi_1/main.py",
        conn_id="spark_main", 
        py_files=PY_FILES_STRING,
        packages="org.postgresql:postgresql:42.7.3",
        application_args=["{{ params.load_method }}"],
    )

    end = EmptyOperator(task_id="end_pipeline_1")

    start >> spark_etl >> end

customer_analytics_etl_pipeline()
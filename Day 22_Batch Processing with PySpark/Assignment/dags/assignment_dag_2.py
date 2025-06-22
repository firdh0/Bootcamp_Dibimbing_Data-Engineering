from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

try:
    from send_notification_to_email import send_dag_notification
except ImportError:
    logging.warning("File send_notification_to_email.py tidak ditemukan. Notifikasi email dinonaktifkan.")

@dag(
    dag_id="customer_analytics_etl_dag_2",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
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
            "spark_jdbc", # Default value
            type="string",
            title="Load Method",
            description="Select a method to perform the transformation and load the data.",
            enum=["spark_jdbc", "postgres_pushdown"],
        )
    },
    tags=["assignment", "pyspark", "postgresql", "etl", "notification_to_email"],
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

    start = EmptyOperator(task_id="start_pipeline_2")

    spark_etl = SparkSubmitOperator(
        task_id="etl_pipeline_2",
        application="/spark-scripts/versi_2/assignment_etl.py",
        conn_id="spark_main",
        packages="org.postgresql:postgresql:42.3.8",
        application_args=["{{ params.load_method }}"],
    )

    end = EmptyOperator(task_id="end_pipeline_2")

    start >> spark_etl >> end


customer_analytics_etl_pipeline()

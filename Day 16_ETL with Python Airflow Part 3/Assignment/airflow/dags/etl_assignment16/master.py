from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# Import notifikasi jika ada (opsional untuk DAG master ini)
try:
    from send_notification_to_email import send_dag_notification
    NOTIFICATION_ENABLED = True
except ImportError:
    NOTIFICATION_ENABLED = False
    def send_dag_notification(context):
        logging.warning("send_notification_to_email not found, notifications disabled for master DAG.")

log = logging.getLogger(__name__)

# DAG IDs of the DAGs to be orchestrated
EXTRACT_MYSQL_DAG_ID = "dag_extract_assignment16"  # Dari artifact dag_ekstrak_dari_mysql
TRANSFORM_DAG_ID = "dag_transform_assignment16" # Dari artifact transform_dag_py_user_script_final (berdasarkan transform.py Anda)
LOAD_POSTGRES_DAG_ID = "dag_load_assignment16" # Dari artifact load_transformed_to_postgres_dag

@dag(
    dag_id="master_etl_assignment16",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Bisa diatur jika ingin berjalan secara periodik
    on_success_callback=send_dag_notification if NOTIFICATION_ENABLED else None,
    on_failure_callback=send_dag_notification if NOTIFICATION_ENABLED else None,
    tags=["master_pipeline", "etl", "orchestration"],
    doc_md="""\
    ### Master ETL Pipeline for News Data

    This DAG orchestrates the execution of three main ETL DAGs:
    1. **Extract from MySQL**: Extracts data from the MySQL `news_articles` table and stages it.
    2. **Transform Staged Data**: Processes the staged Parquet files (cleaning, text preprocessing).
    3. **Load to PostgreSQL DW**: Loads the transformed data into the PostgreSQL data warehouse.

    Each step is triggered sequentially, waiting for the completion of the previous DAG.
    """,
)
def master_etl():
    """
    Orchestrates the news ETL pipeline by triggering Extract, Transform, and Load DAGs.
    """

    trigger_extract_mysql_dag = TriggerDagRunOperator(
        task_id="trigger_dag_extract_assignment16",
        trigger_dag_id=EXTRACT_MYSQL_DAG_ID,
        wait_for_completion=True,  
        poke_interval=60,         
        failed_states=["failed"],  
    )

    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_dag_transform_assignment16",
        trigger_dag_id=TRANSFORM_DAG_ID,
        wait_for_completion=True,
        poke_interval=60,
        failed_states=["failed"],
    )

    trigger_load_to_postgres_dag = TriggerDagRunOperator(
        task_id="trigger_dag_load_assignment16",
        trigger_dag_id=LOAD_POSTGRES_DAG_ID,
        wait_for_completion=True,
        poke_interval=60,
        failed_states=["failed"],
    )

    trigger_extract_mysql_dag >> trigger_transform_dag >> trigger_load_to_postgres_dag

master_news_etl_orchestrator_dag = master_etl()

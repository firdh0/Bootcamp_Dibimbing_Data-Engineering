from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook # To fetch the connection
from airflow.exceptions import AirflowNotFoundException

# Setup logging
log = logging.getLogger(__name__)

# PostgreSQL Connection ID to test
# This should match what you defined in AIRFLOW_CONN_POSTGRES_EXTERNAL_DB (e.g.)
# or what you set in the Airflow UI for your general PostgreSQL service.
POSTGRES_CONN_ID_TO_TEST = "postgres_default" 

@dag(
    dag_id="dag_test_postgre_connection", # DAG ID updated for PostgreSQL
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["testing", "connection", "postgresql"],
    doc_md="""\
    ### DAG to Test PostgreSQL Connection

    This DAG aims to verify if the Airflow connection for PostgreSQL
    (with the specified Conn ID, default: `postgres_external_db`) has been successfully
    created (e.g., via an `AIRFLOW_CONN_...` environment variable) and is accessible.
    This test is for the general purpose PostgreSQL service, not Airflow's metadata DB.
    """,
)
def test_postgres_connection_dag(): # Function name updated
    """
    This DAG tests the existence and accessibility of a PostgreSQL connection.
    """

    @task(task_id="check_postgres_connection_task") # Task ID updated
    def check_postgres_connection_task(): # Callable name updated
        """
        Tries to fetch and print details of the PostgreSQL connection.
        """
        log.info(f"Attempting to retrieve connection with Conn ID: '{POSTGRES_CONN_ID_TO_TEST}'")
        try:
            # Fetch the connection object
            connection = BaseHook.get_connection(POSTGRES_CONN_ID_TO_TEST)
            
            log.info(f"Connection '{POSTGRES_CONN_ID_TO_TEST}' SUCCEEDED to be found!")
            log.info(f"  Conn ID: {connection.conn_id}")
            log.info(f"  Conn Type: {connection.conn_type}") # Should be 'postgres'
            log.info(f"  Host: {connection.host}")
            log.info(f"  Port: {connection.port}")
            log.info(f"  Schema (Database): {connection.schema}")
            log.info(f"  Login (User): {connection.login}")
            # NEVER print connection.password to logs!
            log.info(f"  Extra: {connection.extra}")
            log.info("PostgreSQL connection test successful (based on definition found).")

        except AirflowNotFoundException:
            log.error(f"Connection '{POSTGRES_CONN_ID_TO_TEST}' NOT FOUND in Airflow Connections.")
            log.error("Ensure the connection has been defined via the Airflow UI or an AIRFLOW_CONN_... environment variable (e.g., AIRFLOW_CONN_POSTGRES_EXTERNAL_DB).")
            raise # Make the task fail to make it obvious
        except Exception as e:
            log.error(f"Another error occurred while trying to retrieve connection '{POSTGRES_CONN_ID_TO_TEST}': {str(e)}")
            raise

    # Call the task function to define it within the DAG
    check_postgres_connection_task()

# Instantiate the DAG
test_postgres_connection_dag()

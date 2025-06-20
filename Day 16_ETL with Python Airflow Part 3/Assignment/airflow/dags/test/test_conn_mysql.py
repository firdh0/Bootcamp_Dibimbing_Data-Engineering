from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook # To fetch the connection
from airflow.exceptions import AirflowNotFoundException

log = logging.getLogger(__name__)

MYSQL_CONN_ID_TO_TEST = "mysql_default" 

@dag(
    dag_id="dag_test_mysql_connection", 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["testing", "connection", "mysql"],
    doc_md="""\
    ### DAG to Test MySQL Connection

    This DAG aims to verify if the Airflow connection for MySQL
    (with the specified Conn ID, default: `mysql_default`) has been successfully
    created (e.g., via an `AIRFLOW_CONN_...` environment variable) and is accessible.
    """,
)
def test_mysql_connection_dag(): # Function name updated
    """
    This DAG tests the existence and accessibility of a MySQL connection.
    """

    @task
    def check_mysql_connection_task():
        """
        Tries to fetch and print details of the MySQL connection.
        """
        log.info(f"Attempting to retrieve connection with Conn ID: '{MYSQL_CONN_ID_TO_TEST}'")
        try:
            # Fetch the connection object
            connection = BaseHook.get_connection(MYSQL_CONN_ID_TO_TEST)
            
            log.info(f"Connection '{MYSQL_CONN_ID_TO_TEST}' SUCCEEDED to be found!")
            log.info(f"  Conn ID: {connection.conn_id}")
            log.info(f"  Conn Type: {connection.conn_type}")
            log.info(f"  Host: {connection.host}")
            log.info(f"  Port: {connection.port}")
            log.info(f"  Schema (Database): {connection.schema}")
            log.info(f"  Login (User): {connection.login}")
            # NEVER print connection.password to logs!
            log.info(f"  Extra: {connection.extra}")
            log.info("Connection test successful (based on definition found).")

        except AirflowNotFoundException:
            log.error(f"Connection '{MYSQL_CONN_ID_TO_TEST}' NOT FOUND in Airflow Connections.")
            log.error("Ensure the connection has been defined via the Airflow UI or an AIRFLOW_CONN_... environment variable.")
            raise # Make the task fail to make it obvious
        except Exception as e:
            log.error(f"Another error occurred while trying to retrieve connection '{MYSQL_CONN_ID_TO_TEST}': {str(e)}")
            raise

    # Call the task function to define it within the DAG
    check_mysql_connection_task()

# Instantiate the DAG
test_mysql_connection_dag()

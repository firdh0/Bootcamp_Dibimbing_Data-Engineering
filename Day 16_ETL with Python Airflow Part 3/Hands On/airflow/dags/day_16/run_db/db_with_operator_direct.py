from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag()
def db_with_operator_direct():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    query_mysql = SQLExecuteQueryOperator(
        task_id = "query_mysql",
        conn_id = "mysql_dibimbing", # penting
        sql     = "SELECT * FROM dibimbing.users" # penting
    )

    query_postgres = SQLExecuteQueryOperator(
        task_id = "query_postgres",
        conn_id = "postgres_dibimbing",
        sql     = "SELECT * FROM dibimbing.users"
    )

    start_task >> [query_mysql, query_postgres] >> end_task

db_with_operator_direct()



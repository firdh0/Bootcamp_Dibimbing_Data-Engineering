from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def db_with_hook():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def query_mysql():
        import pandas as pd
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        mysql_hook = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()

        with mysql_hook.connect() as conn:
            df = pd.read_sql(
                sql = "SELECT * FROM dibimbing.users",
                con = conn,
            )

        print(df)





    @task
    def query_postgres():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

        with postgres_hook.connect() as conn:
            df = pd.read_sql(
                sql = "SELECT * FROM dibimbing.users",
                con = conn,
            )

        print(df)

    start_task >> [query_mysql(), query_postgres()] >> end_task

db_with_hook()







import yaml
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with open("dags/resources/dynamic-dag/list_tables.yaml") as f:
    list_tables = yaml.safe_load(f)

@dag()
def dynamic_task_file():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    for table in list_tables:
        extract = BashOperator(
            task_id      = f"extract.{table}",
            bash_command = f"echo config: extract table {table}",
        )

        load = BashOperator(
            task_id      = f"load.{table}",
            bash_command = f"echo config: load table {table}",
        )

        start_task >> extract >> load >> end_task

dynamic_task_file()



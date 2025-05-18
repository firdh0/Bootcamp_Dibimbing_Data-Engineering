import yaml
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with open("dags/resources/dynamic-dag/list_tables.yaml") as f:
    list_tables = yaml.safe_load(f)

for table in list_tables:

    @dag(dag_id=f"dynamic_dag_file_el_table_{table}")
    def dynamic_dag_file():
        start_task = EmptyOperator(task_id="start_task")
        end_task   = EmptyOperator(task_id="end_task")

        extract = BashOperator(
            task_id      = f"extract",
            bash_command = f"echo config: extract table {table}",
        )

        load = BashOperator(
            task_id      = f"load",
            bash_command = f"echo config: load table {table}",
        )

        start_task >> extract >> load >> end_task

    dynamic_dag_file()



from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

list_tables = Variable.get("dynamic_dag_list_tables", deserialize_json=True)

@dag()
def dynamic_task_variable():
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

dynamic_task_variable()



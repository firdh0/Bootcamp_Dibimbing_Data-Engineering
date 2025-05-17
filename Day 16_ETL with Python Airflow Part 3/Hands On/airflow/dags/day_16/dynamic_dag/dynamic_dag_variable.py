from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

list_tables = Variable.get("dynamic_dag_list_tables", deserialize_json=True)

for table in list_tables:

    @dag(dag_id=f"dynamic_dag_variable_el_table_{table}")
    def dynamic_dag_variable():
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

    dynamic_dag_variable()



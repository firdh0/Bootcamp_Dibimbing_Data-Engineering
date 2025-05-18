from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import datetime

@dag()
def variable_with_reference_template():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    prev_variable = BashOperator(
        task_id      = "prev_variable",
        bash_command = "echo DATA dari variable sebelumnya adalah: {{ var.json.data_variable }}",
    )

    @task
    def set_variable():
        Variable.set(
            key   = "data_variable",
            value = {
                "nama"    : "dibimbing",
                "divisi"  : "DE",
                "datetime": str(datetime.now()),
            },
            serialize_json = True
        )


    get_variable = BashOperator(
        task_id      = "get_variable",
        bash_command = "echo DATA dari variable yang baru adalah: {{ var.json.data_variable }}",
    )

    start_task >> prev_variable >> set_variable() >> get_variable >> end_task

variable_with_reference_template()


















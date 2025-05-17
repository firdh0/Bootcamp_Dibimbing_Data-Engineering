from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

@dag()
def xcom_with_return_and_template_reference():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def sender():
        return {
            "nama"  : "dibimbing",
            "divisi": "DE",
        }

    receiver = BashOperator(
        task_id      = "receiver",
        bash_command = "echo DATA DARI SENDER: {{ ti.xcom_pull('sender') }}"
    )

    start_task >> sender() >> receiver >> end_task

xcom_with_return_and_template_reference()



from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def xcom_with_return_temp_var():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def sender():
        return {
            "nama"  : "dibimbing",
            "divisi": "DE",
        }

    @task
    def receiver(data):
        print("DATA DARI SENDER:", data)

    sender = sender()
    start_task >> sender >> receiver(sender) >> end_task

xcom_with_return_temp_var()


from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def xcom_with_return_direct_param():
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

    start_task >> receiver(sender()) >> end_task

xcom_with_return_direct_param()


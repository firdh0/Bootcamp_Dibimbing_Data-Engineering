from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def xcom_with_return_and_current_context():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def sender():
        return {
            "nama"  : "dibimbing",
            "divisi": "DE",
        }




    @task
    def receiver(**kwargs):
        ti   = kwargs["ti"]
        data = ti.xcom_pull(
            task_ids = "sender"
        )

        print("DATA DARI SENDER:", data)

    start_task >> sender() >> receiver() >> end_task

xcom_with_return_and_current_context()






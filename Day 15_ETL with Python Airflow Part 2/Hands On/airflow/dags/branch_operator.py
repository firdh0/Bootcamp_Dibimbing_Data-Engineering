from airflow.decorators import dag, branch_task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    schedule_interval = "* * * * *",
    start_date        = datetime(2024, 7, 1),
    catchup           = False,
)
def branch_operator():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")
    odd_task   = EmptyOperator(task_id="odd_task")
    even_task  = EmptyOperator(task_id="even_task")

    @branch_task
    def branch_odd_or_even(**kwargs):
        data_interval_start = kwargs['data_interval_start']
        print("data_interval_start:", data_interval_start)

        dag_run_minute = data_interval_start.minute
        print("dag_run_minute:", dag_run_minute)

        if dag_run_minute % 2 == 0:
            return "even_task"
        else:
            return "odd_task"

    start_task >> branch_odd_or_even() >> [odd_task, even_task] >> end_task

branch_operator()



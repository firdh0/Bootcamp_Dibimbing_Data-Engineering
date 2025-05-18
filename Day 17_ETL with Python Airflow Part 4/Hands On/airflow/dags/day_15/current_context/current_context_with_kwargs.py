import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

pd.options.display.max_columns       = None
pd.options.display.expand_frame_repr = None

@dag()
def current_context_with_kwargs():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def print_all_context(param1, **kwargs):
        df = pd.DataFrame([
            {
                "key"  : key,
                "type" : str(type(value)),
                "value": str(value)
            }
            for key, value in kwargs.items()]
        )

        print(f"\n{df}")




    @task
    def print_sample_context(**kwargs):
        print("=== logical date ==>")
        print("data interval start:", kwargs['data_interval_start'])
        print("data interval end:", kwargs['data_interval_end'])

        print("=== get date and timestamp ==>")
        print("ds:", kwargs['ds'])
        print("ts:", kwargs['ts'])

        print("=== task instance ==>")
        print("task instance execution_date:", kwargs['ti'].start_date)
        print("task instance current_state:", kwargs['ti'].current_state())

        print("=== realtime ==>")
        print("dag_run start_date:", kwargs['dag_run'].start_date)
        print("dag_run start_task start_date:", kwargs['dag_run'].get_task_instance('start_task').start_date)

        print("=== transform ==>")
        print("data interval end - wib:", kwargs['data_interval_end'].in_timezone('Asia/Jakarta').add(days=-1).to_datetime_string())
        print("datetime - string:", datetime.strptime('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') - timedelta(days=1))

    start_task >> print_all_context(
        param1 = "ini adalah param1",
        param2 = "ini adalah param2",
    ) >> print_sample_context() >> end_task

current_context_with_kwargs()




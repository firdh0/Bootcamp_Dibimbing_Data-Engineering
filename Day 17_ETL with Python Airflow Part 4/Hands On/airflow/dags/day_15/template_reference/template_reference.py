from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

@dag()
def template_reference():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    print_sample_context = BashOperator(
        task_id      = "print_sample_context",
        bash_command = (
            "echo '=== logical date ==>'\n" \
            "echo data interval start: {{ data_interval_start }}\n" \
            "echo data interval end: {{ data_interval_end }}\n" \

            "echo '=== get date and timestamp ==>'\n" \
            "echo ds: {{ ds }}\n" \
            "echo ts: {{ ts }}\n" \

            "echo '=== task instance ==>'\n" \
            "echo task instance execution_date: {{ ti.start_date }}\n" \
            "echo task instance current_state: {{ ti.current_state() }}\n" \






            "echo '=== realtime ==>'\n" \
            "echo dag_run start_date: {{ dag_run.start_date }}\n" \
            "echo dag_run start_task start_date: {{ dag_run.get_task_instance('start_task').start_date }}\n" \

            "echo '=== transform ==>'\n" \
            "echo data interval end - wib: {{ data_interval_end.in_timezone('Asia/Jakarta').add(days=-1).to_datetime_string() }}\n" \
            "echo datetime - string: {{ macros.datetime.strptime('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') - macros.timedelta(days=1) }}\n" \
        )
    )

    start_task >> print_sample_context >> end_task

template_reference()












from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id            = "dag_without_default_args",
    schedule_interval = None,
    tags              = ["exercise"],
)
def main():
    task_1 = EmptyOperator(
        task_id = "task_ke_1",
        retries = 3,
    )

    task_2 = EmptyOperator(
        task_id = "task_ke_2",
        retries = 3,
    )

    task_3 = EmptyOperator(
        task_id = "task_ke_3",
        retries = 4,
    )

    task_1
    task_2
    task_3

main()


import pytz
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag(
    dag_id            = "schedule",
    schedule_interval = "30 9 * * *",
    start_date        = datetime(2024, 7, 1, tzinfo=pytz.timezone("Asia/Jakarta")),
    catchup           = True,
    tags              = ["exercise"],
)
def main():
    task_1 = EmptyOperator(
        task_id = "task_ke_1"
    )

    task_1

main()


from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id            = "dag_full_config",
    description       = "ini adalah deskripsi dag",
    schedule_interval = "* * * * *",
    start_date        = datetime(2024, 7, 1),
    catchup           = False,
    tags              = ["exercise"],
    default_args      = {
        "owner": "dibimbing, galuh",
    },
    owner_links = {
        "dibimbing": "https://dibimbing.id/",
        "galuh"    : "mailto:galuh.ramaditya@gmail.com",
    }
)
def main():
    task_1 = EmptyOperator(
        task_id = "task_ke_1"
    )

    task_1

main()


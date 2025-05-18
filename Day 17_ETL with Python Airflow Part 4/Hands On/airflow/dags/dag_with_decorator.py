from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag()
def dag_with_decorator():
    task_1 = EmptyOperator(
        task_id = "task_ke_1"
    )

    task_1

dag_with_decorator()


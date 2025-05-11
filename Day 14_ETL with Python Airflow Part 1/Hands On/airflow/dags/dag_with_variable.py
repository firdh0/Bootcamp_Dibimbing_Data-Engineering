from airflow import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(dag_id="dag_with_variable")

task_1 = EmptyOperator(
    task_id = "task_ke_1",
    dag     = dag,
)

task_1


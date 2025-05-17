import pandas as pd
from airflow.decorators import dag, task
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

pd.options.display.max_columns       = None
pd.options.display.expand_frame_repr = None

@dag()
def sensor_dag_last_run():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def get_dag_run():
        dag_runs = DagRun.find(dag_id="sensor_sleep")
        print(dag_runs)



        df = pd.DataFrame([
            {
                "key"  : key,
                "type" : str(type(value)),
                "value": str(value)
            }
            for key, value in dag_runs[-1].__dict__.items()]
        )
        print(f"\n{df}")

    def get_last_dag(dt):
        dag_runs = DagRun.find(dag_id="sensor_sleep")
        return dag_runs[-1].data_interval_start

    wait_last_dag = ExternalTaskSensor(
        task_id           = "wait_last_dag",
        external_dag_id   = "sensor_sleep",
        poke_interval     = 5,
        execution_date_fn = get_last_dag,
        timeout           = 60,
    )

    start_task >> get_dag_run() >> wait_last_dag >> end_task

sensor_dag_last_run()


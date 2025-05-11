from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

@dag(schedule_interval="* * * * *", start_date=datetime(2024, 8, 1), catchup=False)
def sensor_sleep():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    wait_sleep = TimeDeltaSensor(
        task_id       = "wait_sleep",
        delta         = timedelta(seconds=30),
        poke_interval = 5,
    )

    start_task >> wait_sleep >> end_task

sensor_sleep()


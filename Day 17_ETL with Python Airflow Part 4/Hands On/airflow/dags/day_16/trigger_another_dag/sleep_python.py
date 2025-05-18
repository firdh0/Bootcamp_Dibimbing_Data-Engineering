from airflow.decorators import dag, task

@dag()
def sleep_python():
    @task
    def sleep():
        import time
        time.sleep(30)

    sleep()

sleep_python()



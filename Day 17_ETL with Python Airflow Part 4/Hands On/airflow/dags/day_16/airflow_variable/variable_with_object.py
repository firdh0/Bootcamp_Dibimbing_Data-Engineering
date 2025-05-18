from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from datetime import datetime

@dag()
def variable_with_object():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def prev_variable():
        var = Variable.get("data_variable", deserialize_json=True)
        print("DATA dari variable sebelumnya adalah:", var)

    @task
    def set_variable():
        Variable.set(
            key   = "data_variable",
            value = {
                "nama"    : "dibimbing",
                "divisi"  : "DE",
                "datetime": str(datetime.now()),
            },
            serialize_json = True # pengganti dari json.dumps
        )



    @task
    def get_variable():
        var = Variable.get("data_variable", deserialize_json=True)
        print("DATA dari variable yang baru adalah:", var)

    start_task >> prev_variable() >> set_variable() >> get_variable() >> end_task

variable_with_object()
















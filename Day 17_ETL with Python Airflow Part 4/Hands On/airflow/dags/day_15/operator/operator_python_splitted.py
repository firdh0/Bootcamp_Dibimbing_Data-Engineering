from airflow.decorators import dag, task
from resources.scripts.splitted_python import splitted_python

@dag()
def operator_python_splitted():
   python_1 = task(splitted_python)
   python_2 = task(splitted_python, task_id="declarative_task_id")

   python_1(
       param1 = "ini adalah param1",
       param2 = "ini adalah param2",
   )

   python_2(
       param1 = "ini adalah param1",
       param2 = "ini adalah param2",
   )

operator_python_splitted()




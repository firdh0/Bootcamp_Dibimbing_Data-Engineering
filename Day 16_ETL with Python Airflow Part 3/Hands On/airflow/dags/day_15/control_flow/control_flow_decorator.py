from airflow.decorators import dag, task

@dag()
def control_flow_decorator():
   @task
   def python_1():
       print("task 1")

   @task
   def python_2():
       print("task 2")

   @task
   def python_3():
       print("task 3")

   @task
   def python_4():
       print("task 4")

   python_1() >> [python_2(), python_3()] >> python_4()

control_flow_decorator()



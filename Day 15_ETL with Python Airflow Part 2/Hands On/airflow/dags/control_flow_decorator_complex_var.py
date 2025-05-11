from airflow.decorators import dag, task

@dag()
def control_flow_decorator_complex_var():
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

   @task
   def python_5():
       print("task 5")

   task_python_1 = python_1()
   task_python_2 = python_2()
   task_python_3 = python_3()
   task_python_4 = python_4()
   task_python_5 = python_5()

   task_python_1 >> [task_python_2, task_python_3]
   [task_python_4, task_python_2] >> task_python_5

control_flow_decorator_complex_var()



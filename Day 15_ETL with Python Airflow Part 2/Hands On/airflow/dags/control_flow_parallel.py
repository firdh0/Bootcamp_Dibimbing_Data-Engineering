from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag()
def control_flow_parallel():
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")
    task_4 = EmptyOperator(task_id="task_4")
   
    # cara 1
    task_1 >> [task_2, task_3] >> task_4

    # cara 2
    # task_1 >> [task_2, task_3]
    # [task_2, task_3] >> task_4
    
    # cara 3
    # task_1 >> task_2
    # task_1 >> task_3
    # task_2 >> task_4
    # task_3 >> task_4
control_flow_parallel()


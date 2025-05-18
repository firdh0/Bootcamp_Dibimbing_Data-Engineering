from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException, AirflowFailException

@dag()
def trigger_rule():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")
   
    all_success_task = EmptyOperator(
        task_id      = "all_success_task",
        trigger_rule = TriggerRule.ALL_SUCCESS,
    )
   
    all_done_task = EmptyOperator(
        task_id      = "all_done_task",
        trigger_rule = TriggerRule.ALL_DONE,
    )

    one_success_task = EmptyOperator(
        task_id      = "one_success_task",
        trigger_rule = TriggerRule.ONE_SUCCESS,
    )



    @task
    def success_task():
        print("ini task yang sukses")
   
    @task
    def failed_task():
        raise AirflowFailException("ini task yang gagal")
   
    @task
    def skipped_task():
        raise AirflowSkipException("ini task yang diskip")

    prev_tasks = [success_task(), failed_task(), skipped_task()]
    next_tasks = [all_success_task, all_done_task]
   
    start_task >> prev_tasks
    prev_tasks >> all_success_task
    prev_tasks >> all_done_task
    next_tasks >> one_success_task >> end_task

trigger_rule()




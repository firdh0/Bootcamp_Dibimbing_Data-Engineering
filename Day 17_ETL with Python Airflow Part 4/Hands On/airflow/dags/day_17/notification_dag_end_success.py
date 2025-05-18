from airflow.decorators import dag, task
from airflow.utils.context import Context
from airflow.utils.email import send_email
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException

def send_email_on_failure(context: Context):
    send_email(
        to           = ["muhammadfirdho26@gmail.com"],
        subject      = "Airflow Failed!",
        html_content = f"""
            <center><h1>!!! DAG RUN FAILED !!!</h1></center>
            <b>Dag</b>    : <i>{ context['ti'].dag_id }</i><br>
            <b>Task</b>   : <i>{ context['ti'].task_id }</i><br>
            <b>Log URL</b>: <i>{ context['ti'].log_url }</i><br>
        """
    )





@dag(on_failure_callback=send_email_on_failure)
def notification_dag_end_success():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task",
                               trigger_rule=TriggerRule.ALL_DONE)

    @task
    def failed_task():
        raise AirflowFailException("ini task yang gagal")

    start_task >> failed_task() >> end_task

notification_dag_end_success()








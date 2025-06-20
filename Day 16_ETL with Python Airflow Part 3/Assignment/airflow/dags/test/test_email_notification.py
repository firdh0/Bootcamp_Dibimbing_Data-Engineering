from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException # Saved for potential manual failure tests
# Param is no longer needed
# from airflow.models.param import Param

# Importing the notification function from plugins
# Make sure the notification.py file is in airflow/plugins/notification.py
try:
    from send_notification_to_email import send_dag_notification
    NOTIFICATION_CALLBACK_AVAILABLE = True
    log_message_callback_status = "The send_dag_notification function was found and will be used for callbacks."
except ImportError as e:
    logging.error(f"Failed to import send_dag_notification from plugins.notification: {e}")
    logging.warning("Email notifications will not work. Make sure notification.py is in the plugins folder.")
    # Provide a dummy function if it's missing, so the DAG can still be parsed and tested without notifications
    def send_dag_notification(context):
        logging.error("Notification callback was called, but the send_dag_notification function could not be imported.")
    NOTIFICATION_CALLBACK_AVAILABLE = False
    log_message_callback_status = "The send_dag_notification function was NOT found. Callbacks will not send emails."

# Setup logging
log = logging.getLogger(__name__)
log.info(log_message_callback_status)


@dag(
    dag_id="dag_test_email_notification", # DAG ID changed to avoid conflicts
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    tags=["testing", "notification"],
    # Params removed as there is no longer branching based on user input
    doc_md="""\
    ### DAG for Direct Email Notification Test (Success)

    This DAG is designed to directly run a successful workflow
    to test the `on_success_callback` which sends an email notification
    using the `send_dag_notification` function.

    **How to Use:**
    1. Ensure `notification.py` is in `airflow/plugins/`.
    2. Ensure the Airflow Variable `email_recipients` is set in the Airflow UI.
    3. Trigger this DAG manually.
    4. This DAG will run through a success path. You should receive a success notification email.

    Check the Airflow Scheduler logs for the output from the `send_dag_notification` function.

    **To Test Failure Notification:**
    You will need to create another DAG specifically designed to fail, or modify this DAG
    (e.g., make `example_success_task` throw an `AirflowException` by uncommenting the relevant line)
    and run it again.
    """,
)
def direct_email_notification_test_dag():
    """
    This DAG directly tests the on_success callback
    by running a successful workflow.
    """

    @task
    def start_direct_test():
        log.info("Starting DAG for direct email notification test (success path).")
        return "Data from start task"

    @task(task_id="example_success_task")
    def example_success_task(data_from_upstream: str):
        log.info(f"Received: {data_from_upstream}")
        log.info("This task is running successfully.")
        # To test the on_failure_callback, you can uncomment the line below:
        # raise AirflowException("Intentional failure for failure notification test.")
        return "Example success task finished"

    @task(task_id="finish_direct_test")
    def finish_direct_test(previous_task_result: str):
        log.info(f"The direct test DAG has completed successfully. Result: {previous_task_result}")
        log.info("The DAG should send a SUCCESS notification after this task.")

    # Defining the linear DAG flow
    initial_data = start_direct_test()
    intermediate_result = example_success_task(initial_data)
    finish_direct_test(intermediate_result)

# Instantiate the DAG
direct_email_notification_test_dag()
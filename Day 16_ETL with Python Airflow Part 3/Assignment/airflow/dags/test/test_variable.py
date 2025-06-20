from __future__ import annotations

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models import Variable

# Setup logging
log = logging.getLogger(__name__)

@dag(
    dag_id="dag_test_email_recipients_variable", # Slightly changed to avoid conflicts if the old DAG still exists
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["testing", "variable"],
    doc_md="""\
    ### DAG to Test the Email Recipients Variable (Decorator)
    This DAG aims to verify if the Airflow Variable 'email_recipients'
    (which is expected to be defined via the environment variable AIRFLOW_VAR_EMAIL_RECIPIENTS)
    has been successfully imported and is accessible. This version uses the @dag decorator.
    """,
)
def test_email_recipients_variable_dag():
    """
    ### Test Email Recipients Variable DAG (Decorator Version)
    This DAG tests if the Airflow Variable 'email_recipients' is accessible.
    It's expected to be set via the environment variable AIRFLOW_VAR_EMAIL_RECIPIENTS.
    """

    @task
    def check_email_recipients_variable_task():
        """
        A function to retrieve and print the value of the Airflow Variable 'email_recipients'.
        """
        variable_name = "email_recipients"
        try:
            recipients_value = Variable.get(variable_name, default_var=None)

            if recipients_value is not None:
                log.info(f"Variable '{variable_name}' found with value: '{recipients_value}'")
                if isinstance(recipients_value, str):
                    log.info(f"The data type of variable '{variable_name}' is a string, as expected.")
                    if "," in recipients_value:
                        log.info(f"Variable '{variable_name}' appears to contain multiple comma-separated emails.")
                    else:
                        log.info(f"Variable '{variable_name}' appears to contain a single email or is not comma-separated.")
                else:
                    log.warning(f"The data type of variable '{variable_name}' is not a string, but rather: {type(recipients_value)}")
            else:
                log.error(f"Variable '{variable_name}' NOT FOUND in Airflow Variables.")
                # You can raise an error here if you want the task to fail when the variable is missing
                # raise ValueError(f"Airflow Variable '{variable_name}' not found.")

        except Exception as e:
            log.error(f"An error occurred while trying to retrieve the variable '{variable_name}': {str(e)}")
            raise # Reraise the error to mark the task as failed

    # Call the task function to define it within the DAG
    check_email_recipients_variable_task()

# Instantiate the DAG
test_email_recipients_variable_dag()
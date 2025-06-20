from __future__ import annotations

import pendulum
import logging
import os
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable 
from airflow.exceptions import AirflowException


try:
    from utils.staging_utils import save_df_to_staging_parquet
    from send_notification_to_email import send_dag_notification
    HELPERS_IMPORTED_SUCCESSFULLY = True
    logging.info("Helper staging_utils and send_notification imported successfully.")
except ImportError:
    logging.error(f"Failed to import helpers from plugins/utils: {e}")
    HELPERS_IMPORTED_SUCCESSFULLY  = False
    def save_df_to_staging_parquet(df, news_source, base_staging_path=None): 
        raise NotImplementedError("save_df_to_staging_parquet is not available.")
    def send_dag_notification(context):
        raise ImportError("send_dag_notification function is not available, email notifications cannot be sent.")

log = logging.getLogger(__name__)

MYSQL_CONN_ID = "mysql_default"
MYSQL_TABLE_TO_EXTRACT = "news_articles"
STAGING_NEWS_SOURCE_NAME = "mysql_extracted_news"

@dag(
    dag_id="dag_extract_assignment16",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    tags=["extract", "mysql", "staging"],
    doc_md="""\
    ### DAG to Extract Data from MySQL and Stage as Parquet

    This DAG extracts all data from the `news_articles` table (or as configured by Airflow Variables)
    in MySQL, which was previously loaded, and then saves it back to the staging area
    as a Parquet file.

    **Goals:**
    - Ensure data can be retrieved from MySQL without errors.
    - Verify data integrity and structure post-extraction.
    - Stage the data in Parquet format for potential further processing or archiving.
    """,
)
def extract_mysql_to_staging_dag():

    if not HELPERS_IMPORTED_SUCCESSFULLY:
        raise ImportError("Staging helper function could not be loaded, DAG cannot run.")

    @task()
    def start_mysql_extraction_task():
        log.info(f"Starting extraction from MySQL table: {MYSQL_TABLE_TO_EXTRACT}")
        return f"Extraction initiated for table {MYSQL_TABLE_TO_EXTRACT}"

    @task()
    def extract_data_from_mysql_task(trigger_message: str, dag_run=None):
        log.info(trigger_message)
        log.info(f"Attempting to connect to MySQL using Conn ID: {MYSQL_CONN_ID}")
        
        query = f"SELECT id, title, news_url, publication_at, content, scraped_at, source FROM {MYSQL_TABLE_TO_EXTRACT};"
        
        log.info(f"Executing query: {query}")

        try:
            mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            df = mysql_hook.get_pandas_df(sql=query)

            if df.empty:
                log.warning(f"No data extracted from MySQL table '{MYSQL_TABLE_TO_EXTRACT}'. The table might be empty.")
                return {"status": "no_data_extracted", "rows_extracted": 0, "table_name": MYSQL_TABLE_TO_EXTRACT}
            
            log.info(f"Successfully extracted {len(df)} rows from '{MYSQL_TABLE_TO_EXTRACT}'.")
            log.info("Sample of extracted data (first 3 rows):")
            log.info("\n" + df.head(3).to_string())

            news_source_for_staging = STAGING_NEWS_SOURCE_NAME
            
            run_id = "manual__" + pendulum.now().to_iso8601_string() # Fallback
            if dag_run and hasattr(dag_run, 'run_id'):
                run_id = dag_run.run_id

            staged_file_path = save_df_to_staging_parquet(
                df=df,
                news_source=news_source_for_staging 
                # base_staging_path bisa di-override jika perlu
            )

            if staged_file_path:
                log.info(f"Extracted data successfully staged to: {staged_file_path}")
                return {"status": "success", "rows_extracted": len(df), "staged_file": staged_file_path, "news_source_staged_as": news_source_for_staging}
            else:
                log.error("Staging extracted data failed, save_df_to_staging_parquet did not return a path.")
                raise AirflowException("Failed to stage the extracted data.")

        except Exception as e:
            log.error(f"Error during MySQL data extraction or staging: {e}")
            log.exception("Detailed error during MySQL extraction/staging:")
            raise AirflowException(f"MySQL extraction/staging failed: {e}")

    @task()
    def finish_mysql_extraction_task(extraction_result: dict):
        log.info("MySQL data extraction and staging process finished.")
        if extraction_result:
            log.info(f"  Status: {extraction_result.get('status')}")
            if extraction_result.get('status') == 'success':
                log.info(f"  Rows extracted: {extraction_result.get('rows_extracted')}")
                log.info(f"  Data staged as source: {extraction_result.get('news_source_staged_as')}")
                log.info(f"  Staged file path: {extraction_result.get('staged_file')}")
            elif extraction_result.get('status') == 'no_data_extracted':
                log.info(f"  No data was found in table: {extraction_result.get('table_name')}")
        else:
            log.warning("No result received from extraction task.")


    start_op_result = start_mysql_extraction_task()
    extracted_data_result = extract_data_from_mysql_task(start_op_result)
    final_summary_result = finish_mysql_extraction_task(extracted_data_result)

    start_op_result >> extracted_data_result >> final_summary_result

extract_mysql_to_staging_dag()

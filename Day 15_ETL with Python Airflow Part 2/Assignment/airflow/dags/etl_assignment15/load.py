from __future__ import annotations

import pendulum
import logging
import os
import glob
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook 
from airflow.exceptions import AirflowException
from airflow.models import Variable 

# Import helper functions from plugins
try:
    from utils.dataframe_utils import convert_objects_to_json, prep_df_for_mysql
    from send_notification_to_email import send_dag_notification
    HELPERS_IMPORTED_SUCCESSFULLY = True
    logging.info("Helper dataframe_utils and send_notification imported successfully.")
except ImportError as e:
    logging.error(f"Failed to import helpers from plugins/utils/dataframe_utils.py: {e}")
    HELPERS_IMPORTED_SUCCESSFULLY = False
    def convert_objects_to_json(df): raise NotImplementedError("convert_objects_to_json not available.")
    def prep_df_for_mysql(df, news_source, target_columns): raise NotImplementedError("prep_df_for_mysql not available.")
    def send_dag_notification(context):
        raise ImportError("send_dag_notification function is not available, email notifications cannot be sent.")

log = logging.getLogger(__name__)

# --- Configuration from Airflow Variables or Defaults ---
STAGING_BASE_PATH = "/opt/airflow/data/staging"
MYSQL_CONN_ID = "mysql_default"
MYSQL_TABLE_NAME = "news_articles"
IF_EXISTS_POLICY = "append"

MYSQL_TARGET_COLUMNS = ['title', 'news_url', 'publication_at', 'content', 'scraped_at', 'source']


@dag(
    dag_id="dag_load_assignment15", # DAG ID updated
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, 
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    tags=["load", "staging", "mysql"],
    doc_md="""\
    ### Load Staged Parquet Data to MySQL (Simplified Staging Path)

    This DAG scans a predefined staging area (e.g., `/opt/airflow/data/staging`) 
    for Parquet files, processes them, and loads the data into a MySQL table.
    The staging area is expected to have a structure like:
    `<staging_base_path>/<news_source>/<news_source>_data_<timestamp>.parquet`
    MySQL connection details and table name are configured via Airflow Variables or use defaults.
    """,
)
def load():

    if not HELPERS_IMPORTED_SUCCESSFULLY:
        raise ImportError("Helper functions could not be loaded, DAG cannot run.")

    @task()
    def discover_staged_files_task():
        log.info(f"Scanning for Parquet files in staging area with simplified path: {STAGING_BASE_PATH}")
        
        # Updated search pattern for the new structure: STAGING_BASE_PATH / news_source / *.parquet
        search_pattern = os.path.join(STAGING_BASE_PATH, "*", "*.parquet") 
        
        discovered_files = []
        for file_path in glob.glob(search_pattern, recursive=False): # recursive=False as it's a shallower structure
            try:
                # Derive news_source from the path: base_path / news_source / filename.parquet
                # parts will be [<news_source>, <filename.parquet>] relative to STAGING_BASE_PATH
                relative_path = file_path.replace(STAGING_BASE_PATH, "").strip(os.sep)
                parts = relative_path.split(os.sep)
                
                if len(parts) == 2: # Expecting news_source and filename
                    news_source = parts[0]
                    # original_source_type is no longer part of this simplified staging path structure
                    discovered_files.append({
                        "file_path": file_path,
                        "news_source": news_source 
                        # "original_source_type": original_source_type # Removed
                    })
                else:
                    log.warning(f"Could not parse news_source from path: {file_path} (expected structure: <staging_path>/<news_source>/<file.parquet>)")
            except Exception as e:
                log.error(f"Error parsing path {file_path}: {e}")
        
        if not discovered_files:
            log.info("No Parquet files found in the staging area matching the simplified criteria.")
        else:
            log.info(f"Found {len(discovered_files)} Parquet file(s) to process.")
        return discovered_files

    @task()
    def process_and_load_parquet_file_task(file_info: dict):
        file_path = file_info["file_path"]
        news_source = file_info["news_source"] # news_source is now directly from discover_staged_files_task

        log.info(f"Processing Parquet file: {file_path} for news source: {news_source}")

        try:
            df = pd.read_parquet(file_path)
            if df.empty:
                log.info(f"Parquet file {file_path} is empty. Skipping.")
                return {"file_path": file_path, "status": "skipped_empty_file", "rows_loaded": 0}

            log.info(f"Read {len(df)} rows from {file_path}.")

            df_processed = convert_objects_to_json(df) 
            df_ready_for_mysql = prep_df_for_mysql(df_processed, news_source, MYSQL_TARGET_COLUMNS) 

            if df_ready_for_mysql.empty:
                 log.warning(f"DataFrame became empty after column processing for {file_path}. Original rows: {len(df) if df is not None else 0}")
                 return {"file_path": file_path, "status": "skipped_empty_after_processing", "rows_loaded": 0}
            
            log.info(f"Loading {len(df_ready_for_mysql)} rows into MySQL table '{MYSQL_TABLE_NAME}' from {file_path} using Conn ID '{MYSQL_CONN_ID}'...")
            mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            
            engine = mysql_hook.get_sqlalchemy_engine()
            df_ready_for_mysql.to_sql(
                name=MYSQL_TABLE_NAME,
                con=engine,
                if_exists=IF_EXISTS_POLICY,
                index=False,
                chunksize=1000 
            )
            log.info(f"Successfully loaded data from {file_path} to MySQL table '{MYSQL_TABLE_NAME}'.")
            return {"file_path": file_path, "status": "loaded_successfully", "rows_loaded": len(df_ready_for_mysql)}

        except Exception as e:
            log.error(f"Error processing or loading file {file_path}: {e}")
            log.exception(f"Detailed error for file {file_path}:")
            return {"file_path": file_path, "status": "failed_processing", "error": str(e), "rows_loaded": 0}


    @task(trigger_rule="all_done") 
    def finish_load_process_task(load_results: list | None): 
        log.info("Load process from staging to MySQL finished.")
        successful_loads = 0
        failed_processing_count = 0
        skipped_files = 0
        total_rows_loaded = 0

        if load_results: 
            for result in load_results:
                if result and result.get("status") == "loaded_successfully":
                    successful_loads += 1
                    rows = result.get('rows_loaded', 0)
                    total_rows_loaded += rows
                    log.info(f"  Successfully loaded: {result.get('file_path')}, Rows: {rows}")
                elif result and "skipped" in result.get("status", ""):
                    skipped_files +=1
                    log.info(f"  Skipped file: {result.get('file_path')}, Reason: {result.get('status')}")
                elif result and result.get("status") == "failed_processing": 
                    failed_processing_count +=1
                    log.error(f"  Failed to process/load: {result.get('file_path')}. Error: {result.get('error')}")
                elif result: 
                    log.warning(f"  Received unexpected result from upstream: {result}")
        else:
            log.info("No files were processed (discover_staged_files_task might have returned an empty list).")


        log.info(f"Summary: {successful_loads} file(s) loaded successfully. Total rows loaded: {total_rows_loaded}. {skipped_files} file(s) skipped. {failed_processing_count} file(s) failed processing.")
        if failed_processing_count > 0:
            log.error(f"{failed_processing_count} file(s) encountered errors during processing or loading.")


    list_of_staged_files = discover_staged_files_task()
    mapped_load_results = process_and_load_parquet_file_task.expand(file_info=list_of_staged_files)
    final_summary = finish_load_process_task(mapped_load_results)

    list_of_staged_files >> mapped_load_results >> final_summary

load()


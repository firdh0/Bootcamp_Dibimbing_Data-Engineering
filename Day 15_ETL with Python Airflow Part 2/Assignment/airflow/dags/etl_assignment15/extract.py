from __future__ import annotations

import pendulum
import logging
import os
import glob 

from airflow.decorators import dag, task, branch_task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from utils.staging_utils import save_df_to_staging_parquet
    from utils.data_readers import read_csv_file, read_json_file
    from send_notification_to_email import send_dag_notification
    HELPERS_IMPORTED_SUCCESSFULLY = True
    logging.info("Helper staging_utils, data_readers, send_notification imported successfully.")
except ImportError as e:
    logging.error(f"Failed to import helpers from plugins/utils: {e}")
    HELPERS_IMPORTED_SUCCESSFULLY = False
    # Provide dummy fallbacks if import fails so DAG can parse
    def save_df_to_staging_parquet(df, source_type, dag_run_id, news_source, base_staging_path=None):
        raise NotImplementedError("save_df_to_staging_parquet is not available.")
    def read_csv_file(file_path): raise NotImplementedError("read_csv_file is not available.")
    def read_json_file(file_path): raise NotImplementedError("read_json_file is not available.")
    def send_dag_notification(context):
        raise ImportError("send_dag_notification function is not available, email notifications cannot be sent.")

log = logging.getLogger(__name__)

DEFAULT_SOURCE_DATA_PATH_PREFIX = "/opt/airflow/data/scraped_news"
CSV_SOURCE_DIR = os.path.join(DEFAULT_SOURCE_DATA_PATH_PREFIX, "csv")
JSON_SOURCE_DIR = os.path.join(DEFAULT_SOURCE_DATA_PATH_PREFIX, "json")

@dag(
    dag_id="dag_extract_assignment15", 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    on_success_callback=send_dag_notification,
    on_failure_callback=send_dag_notification,
    tags=["extract", "staging", "parquet"], 
    params={
        "source_type": Param("csv", type="string", enum=["csv", "json"], title="Data Source Type", description="Select the type of data source to extract."),
    },
    doc_md="""\
    ### Data Extraction to Staging Area (Parquet) - Dynamic File Processing

    Extracts data from the selected source and saves it to the staging area
    in Parquet format. 
    - For CSV and JSON, this DAG discovers files and dynamically maps tasks for each file.
    - `news_source` is parsed from the filename for CSV/JSON.
    """,
)
def extract(): 

    if not HELPERS_IMPORTED_SUCCESSFULLY:
        raise ImportError("Helper functions could not be loaded, DAG cannot run.")

    # --- Utility Functions ---
    def get_filename_base(filepath: str) -> str:
        """Gets the filename without its extension."""
        return os.path.splitext(os.path.basename(filepath))[0]

    def get_news_source_from_filename_base(filename_base: str) -> str:
        """Parses news_source from a filename base (part before the first underscore)."""
        return filename_base.split('_')[0].lower() if '_' in filename_base else "unknown"

    # --- Task Definitions ---
    @task
    def start_extraction_task(dag_run=None):
        log.info("Starting data extraction process (refactored with dynamic paths)...")
        run_id = "manual__" + pendulum.now().to_iso8601_string()
        if dag_run and hasattr(dag_run, 'run_id'):
            run_id = dag_run.run_id 
        return {"dag_run_id_info": run_id, "message": "Extraction process started"}

    @task.branch() # This is the branch operator instance
    def select_extraction_path_callable(params=None): # Renamed callable for clarity
        source_type = params.get("source_type", "csv") if params else "csv"
        log.info(f"Selected data source type: {source_type}")
        if source_type == "csv":
            return "discover_csv_files_task" 
        elif source_type == "json":
            return "discover_json_files_task" 
        else:
            log.error(f"Unknown data source type: {source_type}")
            raise ValueError(f"Unknown data source type: {source_type}")

    @task()
    def discover_files_task(directory: str, file_pattern: str):
        """Generic task to discover files in a directory based on a pattern."""
        log.info(f"Scanning for files matching '{file_pattern}' in directory: {directory}")
        files = glob.glob(os.path.join(directory, file_pattern))
        if not files:
            log.info(f"No files found matching '{file_pattern}' in {directory}.")
        else:
            log.info(f"Found {len(files)} file(s) to process: {files}")
        return files 

    @task()
    def process_single_file_task(file_path: str, read_function: callable, source_type: str, start_info_data: dict):
        """Generic task to process a single file (CSV or JSON)."""
        output_filename_base = get_filename_base(file_path) 
        news_source_for_staging_path = get_news_source_from_filename_base(output_filename_base)
        
        log.info(f"Processing file: {file_path} for news_source_dir: {news_source_for_staging_path}, output_base: {output_filename_base} (type: {source_type})")
        try:
            df = read_function(file_path) 
            if df is not None and not df.empty:
                staged_file_path = save_df_to_staging_parquet(df=df, news_source=news_source_for_staging_path)
                                
                if staged_file_path:
                    return {"original_file": file_path, "staged_file": staged_file_path, "news_source_parsed": news_source_for_staging_path, "output_filename_base": output_filename_base, "status": "processed"}
                else:
                    return {"original_file": file_path, "status": "staging_failed", "news_source_parsed": news_source_for_staging_path}
            else:
                log.warning(f"No data read from {file_path} or DataFrame is empty.")
                return {"original_file": file_path, "status": "empty_or_read_error", "news_source_parsed": news_source_for_staging_path}
        except Exception as e:
            log.error(f"Failed to process file {file_path}: {e}")
            return {"original_file": file_path, "status": "error", "error_message": str(e), "news_source_parsed": news_source_for_staging_path}


    @task(task_id="finish_staging_task", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def finish_staging_task_func( # Renamed callable
        csv_processing_results: list | None = None, 
        json_processing_results: list | None = None, 
    ):
        log.info("Checking results from upstream extraction tasks...")
        all_results_to_log = []
        if csv_processing_results: all_results_to_log.extend(csv_processing_results)
        if json_processing_results: all_results_to_log.extend(json_processing_results)

        any_data_staged = False
        if not all_results_to_log:
            log.info("No results received from any extraction path.")
        
        for result in all_results_to_log:
            if result: 
                if result.get("staged_file_path"): 
                    any_data_staged = True
                    log.info(f"  Successfully Staged: News Source='{result.get('news_source_parsed', result.get('news_source'))}', File='{result.get('staged_file_path')}', Original='{result.get('original_file', 'N/A')}'")
                elif result.get("status") == "skipped_no_files":
                    log.info(f"  Skipped (no files): Source Type='{result.get('source_type')}'")
                elif result.get("status") == "empty_or_read_error":
                    log.info(f"  Skipped (empty/read error): Original File='{result.get('original_file')}', News Source='{result.get('news_source_parsed', result.get('news_source'))}'")
                elif result.get("status") == "empty_data":
                    log.info(f"  Skipped (empty data): News Source='{result.get('news_source')}'")
                elif result.get("status") == "error":
                    log.error(f"  Error processing: Original File='{result.get('original_file', 'N/A')}', News Source='{result.get('news_source_parsed', result.get('news_source'))}', Message='{result.get('error_message')}'")

        if not any_data_staged and any(r is not None for r in all_results_to_log) : 
             log.warning("Data processing finished, but no new data was successfully staged in this DAG run.")
        elif any_data_staged:
            log.info("Extraction and staging to Parquet completed for at least one source file.")
        else: 
            log.info("No data processed in this DAG run (no files found or all paths skipped).")
            
        log.info("Staging area path structure: /opt/airflow/data/staging/<source_type>/<output_filename_base>_<timestamp>.parquet")


    # --- Define DAG flow ---
    start_info = start_extraction_task()
    branch_op = select_extraction_path_callable() # Call the renamed callable

    discovered_csv_files = discover_files_task.override(task_id="discover_csv_files_task")(
        directory=CSV_SOURCE_DIR, file_pattern="*.csv"
    )
    discovered_json_files = discover_files_task.override(task_id="discover_json_files_task")(
        directory=JSON_SOURCE_DIR, file_pattern="*.json"
    )

    start_info >> branch_op
    branch_op >> [discovered_csv_files, discovered_json_files]

    processed_csv_results = process_single_file_task.override(task_id="process_csv_files_task").partial(
        start_info_data=start_info, read_function=read_csv_file, source_type="csv"
    ).expand(file_path=discovered_csv_files)

    processed_json_results = process_single_file_task.override(task_id="process_json_files_task").partial(
        start_info_data=start_info, read_function=read_json_file, source_type="json"
    ).expand(file_path=discovered_json_files)
    
    final_summary_task = finish_staging_task_func( 
        csv_processing_results=processed_csv_results, 
        json_processing_results=processed_json_results,
    )
    
    processed_csv_results >> final_summary_task
    processed_json_results >> final_summary_task

extract()

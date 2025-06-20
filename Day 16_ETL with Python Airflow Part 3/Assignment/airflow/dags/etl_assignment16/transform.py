from __future__ import annotations

import pendulum
import logging
import os
import glob
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable 
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

# Import helper functions from plugins based on user's provided files
try:
    from utils.staging_utils import save_df_to_staging_parquet
    from utils.assessing_data_utils import (
        check_and_return_combined_duplicates,
        clean_date_string,                 
        convert_to_datetime as convert_to_datetime_assess, 
        check_and_return_null_values
    )
    from utils.cleaning_data_utils import (
        handling_duplicates,
        handling_null_values
    )
    from utils.text_preprocessing_utils import (
        case_folding_text,
        cleaning_text
    )
    from send_notification_to_email import send_dag_notification 
    HELPERS_IMPORTED_SUCCESSFULLY = True
    logging.info("All user-provided helper utils imported successfully for Transform DAG.")
except ImportError as e:
    logging.error(f"Failed to import helpers from plugins/utils or send_notification_to_email.py for Transform DAG: {e}")
    HELPERS_IMPORTED_SUCCESSFULLY = False
    def save_df_to_staging_parquet(*args, **kwargs): raise NotImplementedError("save_df_to_staging_parquet not available.")
    def check_and_return_combined_duplicates(*args, **kwargs): return pd.DataFrame()
    def clean_date_string(*args, **kwargs): return pd.NaT # Changed return to pd.NaT for date cleaning context
    def convert_to_datetime_assess(*args, **kwargs): return args[0] # Return df as is if not available
    def check_and_return_null_values(*args, **kwargs): return pd.DataFrame()
    def handling_duplicates(*args, **kwargs): pass
    def handling_null_values(*args, **kwargs): pass
    def case_folding_text(*args, **kwargs): return "" # Return empty string for text processing context
    def cleaning_text(*args, **kwargs): return "" # Return empty string for text processing context
    def send_dag_notification(context):
        logging.warning("send_dag_notification function is not available, email notifications cannot be sent.")


log = logging.getLogger(__name__)

# Configuration
INPUT_STAGING_PATH = Variable.get("transform_input_staging_path_user", default_var="/opt/airflow/data/staging")
MYSQL_EXTRACTED_SUBDIR = Variable.get("transform_mysql_extracted_subdir", default_var="mysql_extracted_news")
OUTPUT_TRANSFORMED_PATH = Variable.get("transform_output_path_user", default_var="/opt/airflow/data/transformed")
TEXT_COLUMN_TO_PROCESS = Variable.get("transform_content_column_user", default_var="content")
TITLE_COLUMN_TO_PROCESS = Variable.get("transform_title_column_user", default_var="title") 
DROP_NULL_THRESHOLD_PERCENT = float(Variable.get("transform_drop_null_threshold_user", default_var="14.0")) 

@dag(
    dag_id="dag_transform_assignment16", 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, 
    on_success_callback=send_dag_notification if HELPERS_IMPORTED_SUCCESSFULLY else None,
    on_failure_callback=send_dag_notification if HELPERS_IMPORTED_SUCCESSFULLY else None,
    tags=["transform", "staging"], 
    doc_md="""\
    ### Transform Staged News Data DAG (Targeting MySQL Extracted Files - Comprehensive Date Handling)

    This DAG processes Parquet files specifically from the `<INPUT_STAGING_PATH>/mysql_extracted_news/` directory.
    It applies data assessment (duplicates, nulls), data cleaning (remove duplicates, handle nulls), 
    attempts date conversion on all object columns, and basic text preprocessing 
    using user-provided utility functions.
    The transformed data is saved to a new output directory as Parquet files.
    
    Input path: `<INPUT_STAGING_PATH>/<MYSQL_EXTRACTED_SUBDIR>/<filename>.parquet`
    Output path: `<OUTPUT_TRANSFORMED_PATH>/<MYSQL_EXTRACTED_SUBDIR>/<filename_prefix_from_subdir>_data_<timestamp>.parquet`
    """,
)
def transform(): 

    if not HELPERS_IMPORTED_SUCCESSFULLY:
        raise ImportError("Critical helper functions could not be loaded. DAG cannot be constructed.")

    @task() 
    def initialize_transform_task_placeholder(): 
        log.info("Initializing transformation process (placeholder).")
        return "Initialization complete for transform."

    @task() 
    def discover_staged_parquet_files_task(): 
        target_discovery_path = os.path.join(INPUT_STAGING_PATH, MYSQL_EXTRACTED_SUBDIR)
        log.info(f"Scanning for Parquet files in specific input staging subdirectory: {target_discovery_path}")
        search_pattern = os.path.join(target_discovery_path, "*.parquet")
        
        discovered_files_info = []
        for file_path in glob.glob(search_pattern, recursive=False):
            try:
                news_source_for_path = MYSQL_EXTRACTED_SUBDIR 
                original_filename_base = os.path.splitext(os.path.basename(file_path))[0] 
                discovered_files_info.append({
                    "file_path": file_path,
                    "news_source_for_path": news_source_for_path, 
                    "original_filename_base_for_log": original_filename_base 
                })
            except Exception as e:
                log.error(f"Error parsing path {file_path}: {e}")
        
        if not discovered_files_info:
            log.info(f"No Parquet files found in {target_discovery_path} for transformation.")
        else:
            log.info(f"Found {len(discovered_files_info)} Parquet file(s) in {target_discovery_path} to transform.")
        return discovered_files_info

    @task() 
    def transform_single_file_task(file_processing_info: dict, init_status: str): 
        file_path = file_processing_info["file_path"]
        news_source_for_path = file_processing_info["news_source_for_path"] 
        original_filename_base = file_processing_info["original_filename_base_for_log"] 
        
        df_name_for_logs = f"DataFrame from '{original_filename_base}.parquet' (Source: {news_source_for_path})"
        log.info(f"Transforming file: {file_path}. Init status: {init_status}")

        try:
            df = pd.read_parquet(file_path)
            if df.empty:
                log.warning(f"Parquet file {file_path} is empty. Skipping.")
                return {"original_file": file_path, "status": "skipped_empty_source_file", "news_source_used_for_path": news_source_for_path}

            log.info(f"Read {len(df)} rows from {file_path}. Starting preprocessing for {df_name_for_logs}...")
            
            # Buat salinan DataFrame untuk operasi in-place, agar tidak mengubah DataFrame asli di luar fungsi jika tidak diinginkan
            # Namun, karena fungsi handling_duplicates dan handling_null_values memodifikasi dict,
            # kita perlu memastikan df_cleaned adalah referensi ke DataFrame yang dimodifikasi.
            df_cleaned = df.copy() # Mulai dengan semua kolom asli
            datasets_dict = {"current_df_data": df_cleaned} # Masukkan ke dict untuk fungsi utilitas

            log.info(f"Assessing duplicates for {df_name_for_logs} using user's function...")
            duplicates_summary_df = check_and_return_combined_duplicates(datasets_dict)
            if not duplicates_summary_df.empty and 'Dataset' in duplicates_summary_df.columns:
                log.info(f"Duplicate summary for {df_name_for_logs} (first 5 rows of identified duplicates):\n{duplicates_summary_df.head().to_string()}")
            
            if not duplicates_summary_df.empty and 'Dataset' in duplicates_summary_df.columns:
                log.info(f"Handling (removing) duplicates for {df_name_for_logs} using user's function...")
                handling_duplicates(datasets_dict, duplicates_summary_df) 
                df_cleaned = datasets_dict["current_df_data"] # Pastikan df_cleaned mereferensikan DataFrame yang dimodifikasi
                log.info(f"Shape after duplicate removal for {df_name_for_logs}: {df_cleaned.shape}")
            else:
                log.info(f"No duplicates to remove or summary was malformed for {df_name_for_logs}.")

            # --- Date Cleaning and Conversion for ALL object columns ---
            log.info(f"Attempting to clean all object columns as potential date strings for {df_name_for_logs}...")
            # Iterasi pada kolom df_cleaned.columns untuk memastikan kita memproses kolom yang sudah ada setelah duplikat.
            # Lakukan operasi pada df_cleaned secara in-place.
            for col_name in df_cleaned.columns:
                if df_cleaned[col_name].dtype == 'object':
                    log.debug(f"Applying user's clean_date_string to object column '{col_name}'")
                    df_cleaned[col_name] = df_cleaned[col_name].apply(clean_date_string) 
                    df_cleaned[col_name] = pd.to_datetime(df_cleaned[col_name], errors='coerce')
            
            log.info(f"Applying convert_to_datetime_assess for final date type check for {df_name_for_logs}...")
            df_cleaned = convert_to_datetime_assess(df_cleaned) 

            log.info(f"Assessing and handling null values for {df_name_for_logs}...")
            # Gunakan df_cleaned yang sudah dimodifikasi untuk pengecekan null
            datasets_for_null_ops = {"current_df_data": df_cleaned} # Pastikan ini referensi yang sama/salinan terbaru
            null_summary_df = check_and_return_null_values(datasets_for_null_ops) 
            if not null_summary_df.empty:
                 log.info(f"Null value summary (before handling) for {df_name_for_logs}:\n{null_summary_df.to_string()}")
            
            if not null_summary_df.empty:
                handling_null_values(
                    datasets=datasets_dict, # Gunakan datasets_dict yang berisi df_cleaned yang ingin dimodifikasi
                    null_values_df=null_summary_df, 
                    drop_null_threshold=DROP_NULL_THRESHOLD_PERCENT 
                )
                df_cleaned = datasets_dict["current_df_data"] # Ambil lagi referensi ke DataFrame yang sudah dimodifikasi
                log.info(f"Shape after null handling for {df_name_for_logs}: {df_cleaned.shape}")
            else:
                log.info(f"No nulls to handle based on summary for {df_name_for_logs}.")

            # --- Text Preprocessing (akan menambahkan kolom baru) ---
            if TEXT_COLUMN_TO_PROCESS in df_cleaned.columns:
                log.info(f"Applying text preprocessing to content column: '{TEXT_COLUMN_TO_PROCESS}' for {df_name_for_logs}")
                df_cleaned['processed_content'] = df_cleaned[TEXT_COLUMN_TO_PROCESS].astype(str).apply(case_folding_text)
                df_cleaned['processed_content'] = df_cleaned['processed_content'].apply(cleaning_text)
                log.info(f"Text preprocessing for content column complete for {df_name_for_logs}.")
            else:
                log.warning(f"Content column '{TEXT_COLUMN_TO_PROCESS}' not found in {df_name_for_logs}. Skipping content preprocessing and setting 'processed_content' to None.")
                df_cleaned['processed_content'] = None # Tambahkan kolom ini agar konsisten

            if TITLE_COLUMN_TO_PROCESS in df_cleaned.columns:
                log.info(f"Applying text preprocessing to title column: '{TITLE_COLUMN_TO_PROCESS}' for {df_name_for_logs}")
                df_cleaned['processed_title'] = df_cleaned[TITLE_COLUMN_TO_PROCESS].astype(str).apply(case_folding_text)
                df_cleaned['processed_title'] = df_cleaned['processed_title'].apply(cleaning_text)
                log.info(f"Text preprocessing for title column complete for {df_name_for_logs}.")
            else:
                log.warning(f"Title column '{TITLE_COLUMN_TO_PROCESS}' not found in {df_name_for_logs}. Skipping title preprocessing and setting 'processed_title' to None.")
                df_cleaned['processed_title'] = None # Tambahkan kolom ini agar konsisten

            # --- Mengganti nama kolom 'source' menjadi 'news_source' ---
            if 'source' in df_cleaned.columns:
                df_cleaned.rename(columns={'source': 'news_source'}, inplace=True)
                log.info(f"Renamed column 'source' to 'news_source' for {df_name_for_logs}.")
            else:
                log.warning(f"Column 'source' not found in {df_name_for_logs}. 'news_source' column will not be present from original data. Please check extraction process if 'source' is expected.")
            
            # --- Perbaikan untuk kolom yang muncul di gambar Anda ---
            # Kolom 'id' dan 'publication_at', 'scraped_at' seharusnya sudah ada di df_cleaned
            # karena mereka berasal dari input awal.
            # Tidak perlu penanganan khusus di sini selain dari operasi pembersihan umum.
            # Pastikan kolom-kolom ini ada sebelum disimpan.
            expected_essential_columns = [
                'id', 'title', 'news_url', 'publication_at', 'content', 'scraped_at', 
                'news_source', 'processed_content', 'processed_title'
            ]
            
            # Hanya untuk memastikan kolom-kolom esensial yang diharapkan ada di output
            # dan untuk debugging jika ada kolom yang hilang secara tidak terduga.
            for col in expected_essential_columns:
                if col not in df_cleaned.columns:
                    log.warning(f"Expected column '{col}' is missing from the transformed DataFrame for {df_name_for_logs}.")

            # --- Simpan DataFrame yang sudah dibersihkan dan ditransformasi ---
            transformed_file_path = save_df_to_staging_parquet(
                df=df_cleaned, # Simpan DataFrame dengan semua kolom yang sudah ada dan yang baru
                news_source=news_source_for_path, # Gunakan ini untuk path output
                base_staging_path=OUTPUT_TRANSFORMED_PATH
            )

            if transformed_file_path:
                return {"original_file": file_path, "transformed_file": transformed_file_path, "news_source_used_for_path": news_source_for_path, "status": "transformed_and_staged"}
            else: 
                log.error(f"Staging transformed data failed for {file_path}, save_df_to_staging_parquet returned None.")
                return {"original_file": file_path, "status": "transform_ok_staging_failed", "news_source_used_for_path": news_source_for_path}

        except Exception as e:
            log.error(f"Error transforming file {file_path}: {e}")
            log.exception(f"Detailed error for file {file_path}:")
            return {"original_file": file_path, "status": "error_transforming", "error_message": str(e), "news_source_used_for_path": news_source_for_path}

    @task(task_id="finish_transformation_task", trigger_rule=TriggerRule.ALL_DONE)
    def finish_transformation_task(transform_results: list | None): 
        log.info("Data transformation process finished.")
        successful_transforms = 0
        if transform_results:
            for result in transform_results:
                if result and result.get("status") == "transformed_and_staged":
                    successful_transforms += 1
                    log.info(f"  Successfully transformed: {result.get('original_file')} -> {result.get('transformed_file')}")
        log.info(f"Total files successfully transformed and staged: {successful_transforms}")


    # --- Define DAG flow ---
    init_op = initialize_transform_task_placeholder() 
    discovered_files = discover_staged_parquet_files_task()
    
    init_op >> discovered_files
    
    transformed_results_list = transform_single_file_task.partial(
        init_status=init_op 
    ).expand(file_processing_info=discovered_files)
    
    summary_op = finish_transformation_task(transformed_results_list)

    discovered_files >> transformed_results_list
    transformed_results_list >> summary_op

# Instantiate the DAG
transform()
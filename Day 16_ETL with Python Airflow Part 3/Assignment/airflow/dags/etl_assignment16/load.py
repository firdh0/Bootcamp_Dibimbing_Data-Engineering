from __future__ import annotations

import pendulum
import logging
import os
import glob
import pandas as pd
from datetime import datetime
import locale # For Indonesian day/month names

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

# Import helper functions from plugins
try:
    from send_notification_to_email import send_dag_notification 
    from utils.readibility_utils import flesch_reading_ease
    from utils.db_utils import get_or_create_dimension_id
    HELPERS_IMPORTED_SUCCESSFULLY = True
    logging.info("All helper functions imported successfully.")
except ImportError as e:
    logging.warning(f"Could not import all helper functions: {e}")
    HELPERS_IMPORTED_SUCCESSFULLY = False
    def send_dag_notification(context):
        logging.warning("send_notification_to_email not found, notifications disabled.")
    def flesch_reading_ease(text_content: str) -> float: 
        logging.warning("flesch_reading_ease function not found, returning default score 0.0.")
        return 0.0
    def get_or_create_dimension_id(*args, **kwargs) -> int | None:
        logging.error("get_or_create_dimension_id function not found from db_utils.py.")
        raise NotImplementedError("get_or_create_dimension_id is not available.")


log = logging.getLogger(__name__)

# --- DAG Configuration ---
POSTGRES_CONN_ID = "postgres_default"  
INPUT_TRANSFORMED_PATH = "/opt/airflow/data/transformed"
SOURCE_SUBDIR_PATTERN = "*"

DEFAULT_CATEGORY_NAME = "Health" 
DEFAULT_MEDIA_URL_PREFIX = "https://example.com/media/" 

PROCESSED_CONTENT_COLUMN = "processed_content"
PROCESSED_TITLE_COLUMN = "processed_title"
ORIGINAL_CONTENT_COLUMN_FALLBACK = "content"
ORIGINAL_TITLE_COLUMN_FALLBACK = "title"


# Mappings for Indonesian Day and Month names to their prescribed IDs
# Monday=1, ..., Sunday=7
INDONESIAN_DAY_MAP = {
    "Senin": 1, "Selasa": 2, "Rabu": 3, "Kamis": 4, "Jumat": 5, "Sabtu": 6, "Minggu": 7,
    # English names as fallback if strftime produces English day names
    "Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7
}
INDONESIAN_MONTH_MAP = {
    "Januari": 1, "Februari": 2, "Maret": 3, "April": 4, "Mei": 5, "Juni": 6,
    "Juli": 7, "Agustus": 8, "September": 9, "Oktober": 10, "November": 11, "Desember": 12,
    # English names as fallback
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12
}

# --- DAG Definition ---
@dag(
    dag_id="dag_load_assignment16", 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    on_success_callback=send_dag_notification if HELPERS_IMPORTED_SUCCESSFULLY else None,
    on_failure_callback=send_dag_notification if HELPERS_IMPORTED_SUCCESSFULLY else None,
    tags=["load", "postgres"], 
    doc_md="""\
    ### Load Transformed News Data to PostgreSQL DW (Corrected & Modular)

    This DAG reads transformed Parquet files, maps data to a dimensional model (including pre-populating ordered dimensions),
    calculates Flesch Reading Ease score (capped 0-100) and other metrics using processed content/title,
    and loads data into PostgreSQL.
    - Dim_Article uses processed_title and processed_content.
    - Dim_Category defaults to 'Health'.
    - Dim_Day and Dim_Month use Indonesian names with fixed IDs.
    - Fact_Analytics metrics (word counts, reading time) are recalculated.
    """,
)
def load(): 

    if not HELPERS_IMPORTED_SUCCESSFULLY:
        raise ImportError("Critical helper functions (notification, readability, db_utils) could not be loaded.")

    @task
    def prepopulate_ordered_dimensions_task():
        """Pre-populates Dim_Day and Dim_Month with Indonesian names and fixed IDs if they don't exist."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        log.info("Pre-populating Dim_Day and Dim_Month with Indonesian names and ordered IDs.")

        # Pre-populate Dim_Day
        for name, day_id_val in INDONESIAN_DAY_MAP.items():
            # Only insert if the ID doesn't exist to preserve the intended order
            # And also check if the name exists with a different ID (less likely if IDs are PK)
            id_exists = pg_hook.get_first(f"SELECT day_id FROM Dim_Day WHERE day_id = {day_id_val}")
            if not id_exists:
                name_exists_with_id = pg_hook.get_first(f"SELECT day_id FROM Dim_Day WHERE name = '{name}' AND day_id = {day_id_val}")
                if not name_exists_with_id: # If name exists with different ID, this logic might need adjustment or rely on PK constraint
                    log.info(f"Inserting into Dim_Day: ID={day_id_val}, Name='{name}'")
                    # ON CONFLICT on day_id (PK) will prevent duplicates.
                    # If name should also be unique, add a UNIQUE constraint on 'name' in DB schema.
                    pg_hook.run(f"INSERT INTO Dim_Day (day_id, name) VALUES ({day_id_val}, '{name}') ON CONFLICT (day_id) DO NOTHING;", autocommit=True)
            else:
                 log.debug(f"Day ID {day_id_val} ('{name}') already exists in Dim_Day.")
        
        # Pre-populate Dim_Month
        for name, month_id_val in INDONESIAN_MONTH_MAP.items():
            id_exists = pg_hook.get_first(f"SELECT month_id FROM Dim_Month WHERE month_id = {month_id_val}")
            if not id_exists:
                name_exists_with_id = pg_hook.get_first(f"SELECT month_id FROM Dim_Month WHERE name = '{name}' AND month_id = {month_id_val}")
                if not name_exists_with_id:
                    log.info(f"Inserting into Dim_Month: ID={month_id_val}, Name='{name}'")
                    pg_hook.run(f"INSERT INTO Dim_Month (month_id, name) VALUES ({month_id_val}, '{name}') ON CONFLICT (month_id) DO NOTHING;", autocommit=True)
            else:
                log.debug(f"Month ID {month_id_val} ('{name}') already exists in Dim_Month.")
        
        log.info("Finished pre-populating ordered dimensions.")
        return "Ordered dimensions pre-populated/verified."


    @task
    def discover_transformed_parquet_files_task():
        target_path = os.path.join(INPUT_TRANSFORMED_PATH, SOURCE_SUBDIR_PATTERN)
        log.info(f"Scanning for Parquet files in transformed data path: {target_path}")
        search_pattern = os.path.join(target_path, "*.parquet") 
        
        discovered_files = []
        for file_path in glob.glob(search_pattern, recursive=True): 
            try:
                relative_to_base = os.path.relpath(file_path, INPUT_TRANSFORMED_PATH)
                news_source = os.path.dirname(relative_to_base) 
                if not news_source or news_source == '.': 
                    news_source = "unknown_source" 
                discovered_files.append({
                    "file_path": file_path,
                    "news_source": news_source
                })
            except Exception as e:
                log.error(f"Error parsing path for file {file_path}: {e}")
        if not discovered_files:
            log.info(f"No Parquet files found in {target_path} for loading.")
        else:
            log.info(f"Found {len(discovered_files)} transformed Parquet file(s) to load.")
        return discovered_files

    @task
    def load_single_file_to_postgres_task(file_info: dict, init_status: str):
        file_path = file_info["file_path"]
        news_source = file_info["news_source"] 
        
        log.info(f"Processing file: {file_path} from source: {news_source}. Init status: {init_status}")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        try:
            df = pd.read_parquet(file_path)
            if df.empty:
                log.warning(f"Parquet file {file_path} is empty. Skipping load.")
                return {"file_path": file_path, "status": "skipped_empty_file", "rows_processed": 0, "facts_loaded": 0}

            log.info(f"Read {len(df)} rows from {file_path}. Preparing to load into PostgreSQL DW.")
            fact_table_data = []
            
            for index, row in df.iterrows():
                # --- Populate Time Dimensions ---
                pub_dt_obj = pd.to_datetime(row.get('publication_at'), errors='coerce')
                pub_time_id, pub_day_id, pub_month_id, pub_quartal_id, pub_date_id, pub_datetime_id = None, None, None, None, None, None
                if pd.notna(pub_dt_obj):
                    pub_time_id = get_or_create_dimension_id(pg_hook, "Dim_Time", {"hour": pub_dt_obj.hour, "minute": pub_dt_obj.minute}, "time_id")
                    
                    day_name_english = pub_dt_obj.strftime('%A') # Get English day name
                    pub_day_id = INDONESIAN_DAY_MAP.get(day_name_english) # Get ID from map
                    if pub_day_id is None: # Fallback if English name not in map (should not happen with full map)
                        log.warning(f"Day name '{day_name_english}' not found in INDONESIAN_DAY_MAP. Cannot determine day_id.")

                    month_name_english = pub_dt_obj.strftime('%B') # Get English month name
                    pub_month_id = INDONESIAN_MONTH_MAP.get(month_name_english) # Get ID from map
                    if pub_month_id is None:
                        log.warning(f"Month name '{month_name_english}' not found in INDONESIAN_MONTH_MAP. Cannot determine month_id.")

                    pub_quartal_id = get_or_create_dimension_id(pg_hook, "Dim_Quartal", {"name": f"Q{(pub_dt_obj.month - 1) // 3 + 1}"}, "quartal_id")
                    
                    if pub_day_id and pub_month_id and pub_quartal_id :
                        pub_date_id = get_or_create_dimension_id(pg_hook, "Dim_Date", 
                                                                {"day_id": pub_day_id, "month_id": pub_month_id, "quartal_id": pub_quartal_id, 
                                                                 "date": pub_dt_obj.date(), "year": pub_dt_obj.year}, "date_id")
                    if pub_date_id and pub_time_id:
                        pub_datetime_id = get_or_create_dimension_id(pg_hook, "Dim_Datetime", {"date_id": pub_date_id, "time_id": pub_time_id}, "datetime_id")

                scrape_dt_obj = pd.to_datetime(row.get('scraped_at'), errors='coerce')
                scrape_time_id, scrape_day_id, scrape_month_id, scrape_quartal_id, scrape_date_id, scrape_datetime_id = None, None, None, None, None, None
                if pd.notna(scrape_dt_obj):
                    scrape_time_id = get_or_create_dimension_id(pg_hook, "Dim_Time", {"hour": scrape_dt_obj.hour, "minute": scrape_dt_obj.minute}, "time_id")

                    s_day_name_english = scrape_dt_obj.strftime('%A')
                    scrape_day_id = INDONESIAN_DAY_MAP.get(s_day_name_english)
                    if scrape_day_id is None: log.warning(f"Day name '{s_day_name_english}' not found in INDONESIAN_DAY_MAP for scrape_date.")

                    s_month_name_english = scrape_dt_obj.strftime('%B')
                    scrape_month_id = INDONESIAN_MONTH_MAP.get(s_month_name_english)
                    if scrape_month_id is None: log.warning(f"Month name '{s_month_name_english}' not found in INDONESIAN_MONTH_MAP for scrape_date.")
                    
                    scrape_quartal_id = get_or_create_dimension_id(pg_hook, "Dim_Quartal", {"name": f"Q{(scrape_dt_obj.month - 1) // 3 + 1}"}, "quartal_id")
                    if scrape_day_id and scrape_month_id and scrape_quartal_id:
                        scrape_date_id = get_or_create_dimension_id(pg_hook, "Dim_Date",
                                                                    {"day_id": scrape_day_id, "month_id": scrape_month_id, "quartal_id": scrape_quartal_id,
                                                                     "date": scrape_dt_obj.date(), "year": scrape_dt_obj.year}, "date_id")
                    if scrape_date_id and scrape_time_id:
                        scrape_datetime_id = get_or_create_dimension_id(pg_hook, "Dim_Datetime", {"date_id": scrape_date_id, "time_id": scrape_time_id}, "datetime_id")
                
                article_title_to_load = str(row.get(PROCESSED_TITLE_COLUMN, row.get(ORIGINAL_TITLE_COLUMN_FALLBACK, 'Unknown Title')))[:255]
                article_url_to_load = str(row.get('news_url', ''))[:255] 
                article_content_to_load = str(row.get(PROCESSED_CONTENT_COLUMN, row.get(ORIGINAL_CONTENT_COLUMN_FALLBACK, '')))
                
                article_id = get_or_create_dimension_id(pg_hook, "Dim_Article", 
                                                        {"title": article_title_to_load, "url": article_url_to_load, "content": article_content_to_load}, 
                                                        "article_id")
                if article_id is None: 
                    log.error(f"Could not get or create article_id for URL {article_url_to_load}. Skipping fact record.")
                    continue 

                media_name_from_df = str(row.get('news_source', news_source))[:50] 
                media_url_placeholder = f"{DEFAULT_MEDIA_URL_PREFIX}{media_name_from_df.lower().replace(' ', '_')}"[:255]
                media_id = get_or_create_dimension_id(pg_hook, "Dim_Media", {"name": media_name_from_df, "url": media_url_placeholder}, "media_id")
                
                category_name = DEFAULT_CATEGORY_NAME 
                category_url_placeholder = f"{DEFAULT_MEDIA_URL_PREFIX}category/{category_name.lower()}"[:255]
                category_id = get_or_create_dimension_id(pg_hook, "Dim_Category", {"name": category_name, "url": category_url_placeholder}, "category_id")

                readability_score_val = 0.0 
                if HELPERS_IMPORTED_SUCCESSFULLY and callable(flesch_reading_ease) and pd.notna(article_content_to_load) and article_content_to_load.strip():
                    readability_score_val = flesch_reading_ease(article_content_to_load)
                
                content_words_list = article_content_to_load.split()
                title_words_list = article_title_to_load.split()
                word_content_count_val = len(content_words_list) if article_content_to_load else 0
                word_title_count_val = len(title_words_list) if article_title_to_load else 0
                
                estimated_reading_time_val = (word_content_count_val / 250.0) if word_content_count_val > 0 else 0.0 # Use float division
                estimated_reading_time_minutes_val = int(round(estimated_reading_time_val))

                if category_id and media_id and pub_datetime_id and scrape_datetime_id: # article_id already checked
                    fact_row = {
                        "article_id": article_id, "category_id": category_id, "media_id": media_id,
                        "publish_datetime_id": pub_datetime_id, "scrape_datetime_id": scrape_datetime_id,
                        "word_content_count": word_content_count_val,
                        "word_title_count": word_title_count_val,
                        "estimated_reading_time_minutes": estimated_reading_time_minutes_val,
                        "readibility_score": readability_score_val
                    }
                    fact_table_data.append(fact_row)
                else:
                    log.warning(f"Skipping fact row for article URL {article_url_to_load} due to missing dimension IDs (pub_dt: {pub_datetime_id}, scrape_dt: {scrape_datetime_id}, article: {article_id}, media: {media_id}, cat: {category_id}).")
            
            if fact_table_data:
                fact_df = pd.DataFrame(fact_table_data)
                log.info(f"Loading {len(fact_df)} rows into Fact_Analytics table.")
                fact_df.to_sql("Fact_Analytics", con=pg_hook.get_sqlalchemy_engine(), if_exists="append", index=False, chunksize=500)
                log.info("Successfully loaded data into Fact_Analytics.")
            else:
                log.info("No data to load into Fact_Analytics for this file.")

            return {"file_path": file_path, "status": "loaded_to_postgres", "rows_processed": len(df), "facts_loaded": len(fact_table_data)}

        except Exception as e:
            log.error(f"Error loading file {file_path} to PostgreSQL: {e}")
            log.exception(f"Detailed error for file {file_path}:")
            return {"file_path": file_path, "status": "error_loading_to_postgres", "error_message": str(e)}

    @task(task_id="finish_postgres_load_task", trigger_rule=TriggerRule.ALL_DONE)
    def finish_postgres_load_task(load_results: list | None):
        log.info("Load to PostgreSQL DW process finished.")
        successful_loads = 0
        if load_results:
            for result in load_results:
                if result and result.get("status") == "loaded_to_postgres":
                    successful_loads += 1
                    log.info(f"  Successfully processed and loaded: {result.get('file_path')}, Rows: {result.get('rows_processed')}, Facts: {result.get('facts_loaded')}")
                elif result:
                    log.error(f"  Failed or skipped file: {result.get('file_path')}, Status: {result.get('status')}, Error: {result.get('error_message', 'N/A')}")
        log.info(f"Total files successfully processed and loaded to PostgreSQL: {successful_loads}")
        if successful_loads == 0 and load_results : 
            log.warning("No files were successfully loaded into PostgreSQL DW in this run.")


    # --- Define DAG flow ---
    prepopulate_dims_op = prepopulate_ordered_dimensions_task()
    discovered_files_op = discover_transformed_parquet_files_task()
    
    prepopulate_dims_op >> discovered_files_op 
    
    load_operation_results = load_single_file_to_postgres_task.partial(
        init_status=prepopulate_dims_op 
    ).expand(file_info=discovered_files_op)
    
    summary_op = finish_postgres_load_task(load_operation_results)

    # Explicit dependencies
    discovered_files_op >> load_operation_results
    load_operation_results >> summary_op

# Instantiate the DAG
load()

from __future__ import annotations

import logging
import os
import pandas as pd
from datetime import datetime 

# Setup logging
log = logging.getLogger(__name__)

# Base path for the staging area within the Airflow container
DEFAULT_STAGING_AREA_PATH = "/opt/airflow/data/staging"

def save_df_to_staging_parquet(
    df: pd.DataFrame,
    news_source: str, # Hanya news_source yang dibutuhkan untuk path dan nama file
    base_staging_path: str = DEFAULT_STAGING_AREA_PATH
) -> str | None:
    """
    Saves a Pandas DataFrame to a Parquet file in the staging area.
    Path structure: <base_staging_path>/<news_source>/<news_source>_data_<timestamp>.parquet

    :param df: Pandas DataFrame to be saved.
    :param news_source: News source (e.g., 'cnn', 'tempo'), used for subdirectory and filename.
    :param base_staging_path: Base path for the staging area.
    :return: Path to the saved Parquet file, or None if the DataFrame is empty.
    """
    if df.empty:
        log.info(f"DataFrame is empty, no data to save to staging area for news source '{news_source}'.")
        return None

    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M") 

    # Clean news_source for path and filename
    safe_news_source_for_path = news_source.lower().replace(" ", "_").replace("-", "_")
    safe_news_source_for_filename = news_source.lower().replace(" ", "_").replace("-", "_")
    
    # Staging directory structure: <base_staging_path>/<news_source>/
    staging_file_dir = os.path.join(base_staging_path, safe_news_source_for_path)
    os.makedirs(staging_file_dir, exist_ok=True)
    
    # Filename: <news_source>_data_<timestamp>.parquet
    file_name = f"{safe_news_source_for_filename}_data_{current_timestamp}.parquet"
    staging_file_path = os.path.join(staging_file_dir, file_name)

    log.info(f"Saving data for news source '{news_source}' to Parquet file in staging: '{staging_file_path}'...")
    
    try:
        df.to_parquet(staging_file_path, index=False, engine='pyarrow')
        log.info(f"Data successfully saved to Parquet: '{staging_file_path}'. Row count: {len(df)}")
        return staging_file_path
    except Exception as e:
        log.error(f"Error saving data to Parquet at '{staging_file_path}': {e}")
        log.exception("Detailed error while saving Parquet:")
        raise

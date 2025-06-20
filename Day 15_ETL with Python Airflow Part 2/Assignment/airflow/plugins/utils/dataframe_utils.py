from __future__ import annotations

import logging
import pandas as pd
import json # For converting dict/list to JSON string

log = logging.getLogger(__name__)

def convert_objects_to_json(df: pd.DataFrame) -> pd.DataFrame: # NAMA FUNGSI DIPERBARUI
    """
    Converts columns of dtype 'object' that contain dicts or lists
    into JSON strings. Handles NaNs appropriately.

    :param df: Pandas DataFrame to process.
    :return: Pandas DataFrame with object columns converted.
    """
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == 'object':
            # Check if the column actually contains dicts or lists by inspecting the first non-null value
            first_non_null = None
            if not df_copy[col].dropna().empty:
                first_non_null = df_copy[col].dropna().iloc[0]
            
            if isinstance(first_non_null, (dict, list)):
                log.info(f"Column '{col}' appears to be object/JSON-like, converting to JSON string.")
                
                # Apply conversion, ensuring NaNs are not converted to "null" string but remain NaN
                # and then are handled by to_sql (usually as NULL in DB)
                df_copy[col] = df_copy[col].apply(
                    lambda x: json.dumps(x) if pd.notnull(x) and isinstance(x, (dict, list)) else x
                )
            elif first_non_null is not None:
                log.debug(f"Column '{col}' is object type but first non-null value is not dict/list (type: {type(first_non_null)}). Skipping conversion for this column.")
            else:
                log.debug(f"Column '{col}' is object type but all values are null. Skipping conversion.")
    return df_copy

def prep_df_for_mysql(df: pd.DataFrame, news_source: str, target_columns: list) -> pd.DataFrame: 
    """
    Ensures the DataFrame has the required columns for the news_articles MySQL table
    and adds the 'source' column. Prepares DataFrame for MySQL insertion.

    :param df: Input Pandas DataFrame.
    :param news_source: The source of the news (e.g., 'cnn', 'tempo').
    :param target_columns: A list of expected column names in the MySQL table (excluding auto-increment id).
    :return: Processed Pandas DataFrame.
    """
    df_copy = df.copy()
    
    # Add the news_source to a column named 'source'
    df_copy['source'] = news_source
    
    # Create a new DataFrame with only the columns that are in target_columns or 'source'
    # This also helps in ordering the columns if needed, though pandas.to_sql is generally robust.
    final_df_columns = []
    for col in target_columns: # Iterate through the desired target columns
        if col in df_copy.columns:
            final_df_columns.append(col)
        elif col == 'source': # 'source' might not be in target_columns list but is essential
            if 'source' not in final_df_columns: # Add if not already selected
                 final_df_columns.append('source')
        else:
            log.warning(f"Target column '{col}' not found in DataFrame. It will be added with None/NaN values.")
            df_copy[col] = None # Add missing target columns with None
            final_df_columns.append(col)
    
    # Ensure 'source' column is definitely included if it was in target_columns or added
    if 'source' not in final_df_columns and 'source' in df_copy.columns:
        final_df_columns.append('source')

    # Filter out any extra columns from df_copy that are not in final_df_columns
    # and ensure the order matches final_df_columns
    # Also ensure all columns in final_df_columns are present in df_copy (they should be after the loop above)
    df_processed = df_copy[[col for col in final_df_columns if col in df_copy.columns]]
    
    return df_processed


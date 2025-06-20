from __future__ import annotations

import logging
import pandas as pd
from dateutil import parser # Diperlukan oleh salah satu fungsi di bawah
import re # Diperlukan oleh clean_date_string_for_assessment

log = logging.getLogger(__name__)

def check_and_return_combined_duplicates(datasets: dict) -> pd.DataFrame:
    """
    Checks for and combines duplicate rows from multiple datasets.

    Parameters:
        datasets (dict): A dictionary where the keys are dataset names (strings) and the values are DataFrames.

    Returns:
        pd.DataFrame: A DataFrame combining information about duplicate rows from all datasets.
    """

    combined_duplicates = pd.DataFrame()

    for name, df in datasets.items():
        duplicate_count = df.duplicated(keep='first').sum()

        if duplicate_count > 0:
            print(f"Dataset '{name}' memiliki {duplicate_count} baris duplikat.\n")

            duplicates_df = df[df.duplicated(keep='first')].copy()
            duplicates_df['Dataset'] = name
            duplicates_df['Jumlah Duplikasi'] = df.groupby(list(df.columns)).transform('size')

            # Pindahkan kolom 'Dataset' ke paling kiri
            cols = ['Dataset'] + [col for col in duplicates_df.columns if col != 'Dataset']
            duplicates_df = duplicates_df[cols]

            combined_duplicates = pd.concat([combined_duplicates, duplicates_df], ignore_index=True)

    return combined_duplicates

INDO_MONTHS = {
    'januari': 'January',
    'februari': 'February',
    'maret': 'March',
    'april': 'April',
    'mei': 'May',
    'juni': 'June',
    'juli': 'July',
    'agustus': 'August',
    'september': 'September',
    'oktober': 'October',
    'november': 'November',
    'desember': 'December',
}

def clean_date_string(date_str):
    if pd.isna(date_str):
        return None
    try:
        date_str = str(date_str).lower()

        # Ganti bulan Indonesia ke Inggris
        for indo, eng in INDO_MONTHS.items():
            date_str = re.sub(rf'\b{indo}\b', eng, date_str, flags=re.IGNORECASE)

        # Hapus teks tidak penting
        date_str = re.sub(r'kompas\.com-\s*', '', date_str)
        date_str = re.sub(r'wib', '', date_str)
        date_str = re.sub(r'\|', '', date_str)
        date_str = re.sub(r'^[a-zA-Z]+,\s*', '', date_str)  # Hapus nama hari jika ada

        # Ganti waktu dari format 09.15 ke 09:15
        date_str = re.sub(r'(\d{1,2})\.(\d{2})', r'\1:\2', date_str)

        # Trim spasi
        date_str = date_str.strip()

        # Parse
        return parser.parse(date_str, dayfirst=True)
    except Exception:
        return pd.NaT

def convert_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns containing 'date' or 'timestamp' in their name to datetime type.

    Parameters:
        df (pd.DataFrame): The DataFrame containing data to be checked and converted.

    Returns:
        pd.DataFrame: The DataFrame with relevant columns converted to datetime type.
    """

    date_columns = [col for col in df.columns if 'date' in col.lower() or 'timestamp' in col.lower()]

    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

def check_and_return_null_values(datasets: dict) -> pd.DataFrame:
    """
    Checks and returns information about null values across various datasets.

    Parameters:
        datasets (dict): A dictionary where keys are dataset names and values are DataFrames.

    Returns:
        pd.DataFrame: A DataFrame combining information about null values from all datasets.
    """

    combined_nulls = pd.DataFrame()

    for name, df in datasets.items():
        df = convert_to_datetime(df)

        null_values = df.isnull().sum()
        null_columns = null_values[null_values > 0]

        if not null_columns.empty:
            print(f"Dataset '{name}' memiliki null values.\n")

            column_types = df.dtypes[null_columns.index]

            is_date_type = column_types.apply(pd.api.types.is_datetime64_any_dtype)

            null_value_summary = pd.DataFrame({
                'Dataset': name,
                'Kolom': null_columns.index,
                'Tipe Data': column_types.values,
                'Jumlah Null Values': null_columns.values,
                'Persentase Null Values (%)': (null_columns.values / len(df)) * 100,
                'Apakah Tipe Data Date?': is_date_type.values
            })

            combined_nulls = pd.concat([combined_nulls, null_value_summary], ignore_index=True)

    return combined_nulls


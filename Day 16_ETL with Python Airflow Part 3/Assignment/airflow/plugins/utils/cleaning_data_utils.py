from __future__ import annotations

import logging
import pandas as pd
import re
from dateutil import parser

log = logging.getLogger(__name__)

def handling_duplicates(datasets: dict, duplicates_df: pd.DataFrame) -> None:
    """
    Menghapus baris duplikat dari dataset yang telah diidentifikasi memiliki duplikat.
    Perubahan diterapkan langsung pada DataFrame asli di dalam dictionary `datasets`.

    Parameters:
        datasets (dict): Dictionary dengan key sebagai nama dataset dan value sebagai DataFrame.
        duplicates_df (pd.DataFrame): DataFrame yang berisi informasi tentang dataset yang memiliki duplikat.

    Returns:
        None: Perubahan diterapkan langsung pada DataFrame yang ada di dalam dictionary `datasets`.
    """

    datasets_with_duplicates = duplicates_df['Dataset'].unique()

    for dataset_name in datasets_with_duplicates:
        if dataset_name in datasets:
            datasets[dataset_name] = datasets[dataset_name].drop_duplicates(keep='first')

def impute_value(df: pd.DataFrame, column_name: str, method: str, dataset_name: str) -> None:
    """
    Menerapkan nilai imputasi langsung pada DataFrame berdasarkan metode yang diberikan.

    Parameters:
        df (pd.DataFrame): DataFrame tempat nilai akan diimputasi.
        column_name (str): Nama kolom yang akan diimputasi.
        method (str): Metode imputasi yang digunakan ('mean', 'median', 'mode', atau 'interpolate').
        dataset_name (str): Nama dataset yang sedang diproses.

    Returns:
        None: Perubahan diterapkan langsung pada DataFrame.
    """
    if method == 'mean':
        df[column_name] = df[column_name].fillna(df[column_name].mean())
    elif method == 'median':
        df[column_name] = df[column_name].fillna(df[column_name].median())
    elif method == 'mode':
        df[column_name] = df[column_name].fillna(df[column_name].mode()[0])
    elif method == 'interpolate':
        # Menginterpolasi dan mengonversi ke resolusi detik
        df[column_name] = df[column_name].interpolate()
        df[column_name] = df[column_name].dt.floor('s')
    else:
        raise ValueError(f"Metode imputasi '{method}' tidak dikenal.")

    # Mencetak informasi imputasi yang dilakukan
    print(f"Imputasi dilakukan pada kolom '{column_name}' dari dataset '{dataset_name}' menggunakan metode '{method}'.")

def handling_null_values(
    datasets: dict,
    null_values_df: pd.DataFrame,
    skewness_df: pd.DataFrame = None,  # Default value set to None
    drop_null_threshold: float = 50.0,
    exclude_columns: dict = None
) -> None:
    """
    Menangani nilai null di beberapa kolom pada suatu dataset berdasarkan informasi dari null_values_df dan skewness_df.
    Perubahan diterapkan langsung pada DataFrame asli.

    Parameters:
        datasets (dict): Dictionary dengan key sebagai nama dataset dan value sebagai DataFrame.
        null_values_df (pd.DataFrame): DataFrame yang berisi informasi tentang kolom-kolom yang memiliki nilai null.
        skewness_df (pd.DataFrame, optional): DataFrame yang berisi informasi tentang nilai skewness dari kolom-kolom numerik. Default adalah None.
        drop_null_threshold (float): Persentase nilai null di kolom yang jika melebihi threshold, kolom tersebut akan di-drop. Default adalah 50%.
        exclude_columns (dict, optional): Dictionary dengan key sebagai nama dataset dan value sebagai list kolom yang dikecualikan dari penanganan null. Default adalah None.

    Returns:
        None: Perubahan diterapkan langsung pada DataFrame yang ada di dalam dictionary datasets.
    """

    if exclude_columns is None:
        exclude_columns = {}

    for _, row in null_values_df.iterrows():
        dataset_name = row['Dataset']
        column_name = row['Kolom']
        null_percentage = row['Persentase Null Values (%)']

        # Cek apakah kolom dikecualikan untuk dataset ini
        if dataset_name in exclude_columns and column_name in exclude_columns[dataset_name]:
            print(f"Kolom '{column_name}' pada dataset '{dataset_name}' dikecualikan dari proses penanganan null.")
            continue

        # Ambil dataset dari dictionary
        df = datasets.get(dataset_name)

        if df is None:
            print(f"Dataset '{dataset_name}' tidak ditemukan.")
            continue

        # Drop kolom jika persentase null melebihi threshold
        if null_percentage > drop_null_threshold: 
            if column_name in df.columns:
                df.drop(column_name, axis=1, inplace=True)
                print(f"Kolom '{column_name}' pada dataset '{dataset_name}' telah di-drop karena persentase null ({null_percentage}%) melebihi threshold.")
            else:
                print(f"Kolom '{column_name}' tidak ditemukan dalam dataset '{dataset_name}' dan tidak dapat di-drop.")
                continue

        # Menangani nilai null jika kolom tidak di-drop
        elif column_name in df.columns:
            if pd.api.types.is_numeric_dtype(df[column_name]):
                if skewness_df is not None:
                    mask = (skewness_df['Dataset'] == dataset_name) & (skewness_df['Kolom'] == column_name)
                    if mask.any():
                        skewness_value = skewness_df.loc[mask, 'Nilai Skewness'].values[0]
                        method = 'median' if skewness_value != 0 else 'mean'
                    else:
                        method = 'median'
                        print(f"Skewness information for column '{column_name}' in dataset '{dataset_name}' not found. Using median.")
                else:
                    method = 'median'
                impute_value(df, column_name, method, dataset_name)

            elif pd.api.types.is_datetime64_any_dtype(df[column_name]):
                impute_value(df, column_name, 'interpolate', dataset_name)

            else:
                impute_value(df, column_name, 'mode', dataset_name)
        else:
            print(f"Kolom '{column_name}' tidak ada dalam dataset '{dataset_name}', lanjutkan ke kolom berikutnya.")
            continue


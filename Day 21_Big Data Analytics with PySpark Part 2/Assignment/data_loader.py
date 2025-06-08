import os
import sys
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession

class DataLoader:
    """
    Manages loading data from various sources into Spark DataFrames.

    This class provides standardized methods for loading datasets from files,
    handling different formats like CSV and Parquet, and organizing them
    into a dictionary of DataFrames.

    Attributes:
        spark (SparkSession): An active SparkSession instance used for all read operations.

    Methods:
        load_all_csv_from_directory(directory):
            Loads all CSV files from a specified directory into a dictionary of DataFrames.
        load_all_parquet_from_directory(directory):
            Loads all Parquet files from a specified directory into a dictionary of DataFrames.
    """


    def __init__(self, spark: SparkSession):
        """
        Initializes the DataLoader with an active SparkSession.

        Parameters:
            spark (SparkSession): The SparkSession to be used for loading data.
        """
        self.spark = spark


    def _load_single_csv(self, file_path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """
        Loads data from a single CSV file and performs initial cleaning.

        This is a private helper method that automatically drops the common '_c0' index column
        if it exists.

        Parameters:
            file_path (str): The path to the CSV file.
            header (bool): Specifies if the CSV file has a header row. Defaults to True.
            infer_schema (bool): Specifies if Spark should automatically infer the schema.
                                 Defaults to True.

        Returns:
            DataFrame: A Spark DataFrame loaded from the CSV, or an empty DataFrame on error.
        """
        print(f" -> Loading data from CSV: {os.path.basename(file_path)}")
        try:
            df = self.spark.read.csv(file_path, header=header, inferSchema=infer_schema)
            
            if '_c0' in df.columns:
                df = df.drop('_c0')
                print(f"    -> Column '_c0' found and removed from {os.path.basename(file_path)}.")
            
            if not df.head(1):
                print(f"    -> Warning: No data loaded from {file_path} or the file is empty.")
                return self.spark.createDataFrame([], StructType([]))
            
            return df
            
        except Exception as e:
            print(f"    -> ERROR loading CSV {file_path}: {e}")
            return self.spark.createDataFrame([], StructType([]))


    def load_all_csv_from_directory(self, directory: str) -> dict[str, DataFrame]:
        """
        Dynamically loads all CSV files from a specified directory into a dictionary.

        It uses the filename (without extension) as the key for each DataFrame.

        Parameters:
            directory (str): The path to the directory containing CSV files.

        Returns:
            dict[str, DataFrame]: A dictionary where keys are filenames and values are the
                                  corresponding Spark DataFrames.
        """
        print(f"\n--- Loading all CSV files from directory: '{directory}' ---")
        if not os.path.isdir(directory):
            print(f" -> ERROR: Directory '{directory}' not found.")
            return {}
        
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        print(f" -> Found {len(csv_files)} CSV file(s).")

        if not csv_files:
            return {}
        
        dataframes = {}
        for filename in csv_files:
            file_path = os.path.join(directory, filename)
            key_name = os.path.splitext(filename)[0]
            
            df = self._load_single_csv(file_path)
            
            if df and df.head(1): 
                dataframes[key_name] = df
        
        print(f"--- Successfully loaded {len(dataframes)} DataFrame(s). ---")
        return dataframes


    def load_all_parquet_from_directory(self, directory: str) -> dict[str, DataFrame]:
        """
        Loads all Parquet datasets from a specified directory.

        This method assumes that each subdirectory within the given directory is a
        Parquet dataset.

        Parameters:
            directory (str): The path to the directory containing Parquet subdirectories.

        Returns:
            dict[str, DataFrame]: A dictionary where keys are the directory names and values
                                  are the corresponding Spark DataFrames.
        """
        print(f"\n--- Loading all Parquet datasets from directory: '{directory}' ---")
        if not os.path.isdir(directory):
            print(f" -> ERROR: Directory '{directory}' not found.")
            return {}
        
        parquet_dirs = [d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]
        print(f" -> Found {len(parquet_dirs)} potential Parquet directorie(s).")
        
        dataframes = {}
        for dir_name in parquet_dirs:
            full_path = os.path.join(directory, dir_name)
            try:
                df = self.spark.read.parquet(full_path)
                dataframes[dir_name] = df
                print(f" -> Successfully loaded: {dir_name}")
            except Exception as e:
                print(f" -> FAILED to load {full_path}: {e}")
                
        print(f"--- Successfully loaded {len(dataframes)} Parquet dataset(s). ---")
        return dataframes


# if __name__ == '__main__':
#     """
#     Main execution block to test the DataLoader class.
#     """
#     try:
#         from spark_utils import SparkManager
#     except ImportError:
#         print("Could not import SparkManager. Defining a fallback for testing.")
#         class SparkManager:
#             def create_session(self, app_name="FallbackSession"):
#                 return SparkSession.builder.appName(app_name).getOrCreate()

#     print("--- Starting DataLoader Class Test ---")

#     # 1. Initialize Spark Session using SparkManager
#     spark_manager = SparkManager()
#     spark_session = spark_manager.create_session("DataLoaderTest")

#     # 2. Create an instance of the DataLoader
#     data_loader = DataLoader(spark_session)

#     # 3. Define the directory for raw data
#     raw_data_directory = "data/raw"
    
#     # 4. Load all CSV files from the directory
#     loaded_dataframes = data_loader.load_all_csv_from_directory(raw_data_directory)

#     # 5. Display a summary of the loaded DataFrames
#     if loaded_dataframes:
#         print("\n--- Summary of Loaded DataFrames ---")
#         for name, df in loaded_dataframes.items():
#             print(f"\nDataFrame: '{name}' (First 5 rows)")
#             df.show(5)
#             print("Schema:")
#             df.printSchema()
#     else:
#         print("\nNo DataFrames were loaded.")

#     # 6. Stop the SparkSession
#     spark_session.stop()
#     print("\n -> SparkSession stopped.")
#     print("\n--- DataLoader Class Test Completed ---")

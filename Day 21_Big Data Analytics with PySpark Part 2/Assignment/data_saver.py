import os
from pyspark.sql import DataFrame, SparkSession
import sys

class DataSaver:
    """
    Manages the process of saving Spark DataFrames to persistent storage.

    This class provides a standardized method for writing DataFrames to disk
    in an efficient, columnar format like Parquet. It handles the creation of
    output directories and iterates through a collection of DataFrames to save each one.

    Methods:
        save_as_parquet(dataframes_dict, output_directory):
            Saves a dictionary of DataFrames to the specified directory in Parquet format.
    """


    def __init__(self):
        """
        Initializes the DataSaver.
        """
        pass


    def save_as_parquet(self, dataframes_dict: dict[str, DataFrame], output_directory: str):
        """
        Saves each DataFrame in a dictionary to a specified directory in Parquet format.

        This method will create the output directory if it does not exist. It writes
        each DataFrame into a subdirectory named after its key in the input dictionary.

        Parameters:
            dataframes_dict (dict[str, DataFrame]): A dictionary where keys are the base names
                                                    for the output subdirectories and values
                                                    are the Spark DataFrames to be saved.
            output_directory (str): The path to the parent directory where the Parquet
                                    datasets will be stored.
        """
        print(f"\n--- Starting Save Process to Directory: '{output_directory}' ---")
        
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            print(f" -> Output directory '{output_directory}' has been created.")

        if not dataframes_dict:
            print(" -> Warning: The DataFrames dictionary is empty. Nothing will be saved.")
            return

        for name, df in dataframes_dict.items():
            if not isinstance(df, DataFrame):
                print(f" -> Warning: The item with key '{name}' is not a Spark DataFrame and will be skipped.")
                continue

            output_path = os.path.join(output_directory, name)
            print(f" -> Saving '{name}' as Parquet to: {output_path}")
            
            try:
                df.repartition(1).write.mode("overwrite").parquet(output_path)
                print(f"    -> Successfully saved.")
            except Exception as e:
                print(f"    -> FAILED to save '{name}' as Parquet: {e}")
                
        print("--- Save Process Completed ---")


    def save_as_csv(self, dataframes_dict: dict[str, DataFrame], output_directory: str):
        """
        Saves each DataFrame in a dictionary to a specified directory in CSV format.

        This method writes each DataFrame into its own subdirectory. Using coalesce(1)
        ensures that each subdirectory contains a single CSV part file with a header.

        Parameters:
            dataframes_dict (dict[str, DataFrame]): A dictionary where keys are the base names
                                                    for the output subdirectories and values
                                                    are the Spark DataFrames to be saved.
            output_directory (str): The path to the parent directory where the CSV
                                    datasets will be stored.
        """
        print(f"\n--- Starting Save Process to Directory (CSV Format): '{output_directory}' ---")
        
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            print(f" -> Output directory '{output_directory}' has been created.")

        if not dataframes_dict:
            print(" -> Warning: The DataFrames dictionary is empty. Nothing will be saved.")
            return

        for name, df in dataframes_dict.items():
            if not isinstance(df, DataFrame):
                print(f" -> Warning: The item with key '{name}' is not a Spark DataFrame and will be skipped.")
                continue

            output_path = os.path.join(output_directory, name)
            print(f" -> Saving '{name}' as CSV to: {output_path}")
            
            try:
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
                print(f"    -> Successfully saved.")
            except Exception as e:
                print(f"    -> FAILED to save '{name}' as CSV: {e}")
                
        print("--- CSV Save Process Completed ---")


# if __name__ == '__main__':
#     """
#     Main execution block to test the DataSaver class.
#     """
#     try:
#         from spark_utils import SparkManager
#     except ImportError:
#         print("Could not import SparkManager. Defining a fallback for testing.")
#         class SparkManager:
#             def create_session(self, app_name="FallbackSession"):
#                 return SparkSession.builder.appName(app_name).getOrCreate()

#     print("--- Starting DataSaver Class Test ---")
    
#     # 1. Create a Spark Session
#     spark_manager = SparkManager()
#     spark = spark_manager.create_session("DataSaverTest")

#     # 2. Create dummy data for testing
#     print(" -> Creating dummy data for testing...")
#     test_data_1 = [(1, 'apple'), (2, 'banana')]
#     test_df_1 = spark.createDataFrame(test_data_1, ['id', 'fruit'])
    
#     test_data_2 = [(10.5, 'red'), (12.3, 'yellow')]
#     test_df_2 = spark.createDataFrame(test_data_2, ['price', 'color'])

#     dummy_dataframes = {
#         "fruits_data": test_df_1,
#         "details_data": test_df_2,
#         "not_a_dataframe": "this is just a string"
#     }
    
#     # 3. Define the output directory for the test
#     test_output_dir = "data/test_cleaned_output"

#     # 4. Instantiate the saver and run the save process
#     data_saver = DataSaver()
#     data_saver.save_as_parquet(dummy_dataframes, test_output_dir)

#     # 5. Optional verification step
#     print("\n -> Verifying the saved data:")
#     try:
#         loaded_df = spark.read.parquet(os.path.join(test_output_dir, "fruits_data"))
#         print("    -> Successfully reloaded 'fruits_data.parquet':")
#         loaded_df.show()
#     except Exception as e:
#         print(f"    -> Failed to reload test data: {e}")

#     # 6. Stop the Spark Session
#     spark.stop()
#     print("\n--- DataSaver Class Test Completed ---")

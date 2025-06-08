from pyspark.sql import DataFrame, SparkSession
import sys
import os

class DataTransformer:
    """
    Manages data transformation processes, specifically joining related datasets.

    This class provides a standardized method for merging different but related
    Spark DataFrames into a single, cohesive DataFrame suitable for analysis.

    Methods:
        join_customer_data(flight_activity_df, loyalty_history_df):
            Merges flight activity data with customer loyalty history data.
    """


    def __init__(self):
        """
        Initializes the DataTransformer.
        """
        pass


    def join_customer_data(self, flight_activity_df: DataFrame, loyalty_history_df: DataFrame) -> DataFrame:
        """
        Joins the flight activity DataFrame with the customer loyalty history DataFrame.

        This method performs a left join on the 'Loyalty_Number' column to ensure all
        flight records are preserved, even if a corresponding customer demographic
        record is not found.

        Parameters:
            flight_activity_df (DataFrame): A DataFrame containing flight activity data.
            loyalty_history_df (DataFrame): A DataFrame containing customer demographic and
                                            loyalty history data.

        Returns:
            DataFrame: A new, merged DataFrame, or None if one of the input DataFrames is invalid.
        """
        print("\n--- Starting Customer Data Join Process ---")
        
        if flight_activity_df is None or loyalty_history_df is None:
            print(" -> ERROR: One or both of the input DataFrames is invalid (None).")
            return None
            
        join_column = "Loyalty_Number"
        
        print(f" -> Performing a 'left join' on column: '{join_column}'")
        
        merged_df = flight_activity_df.join(
            loyalty_history_df,
            on=join_column,
            how="left"
        )
        
        print(f" -> Join successful. Initial rows (flight_activity): {flight_activity_df.count()}")
        print(f" -> Rows after join (merged_df): {merged_df.count()}")
        
        return merged_df


# if __name__ == '__main__':
#     """
#     Main execution block to test the DataTransformer class.
#     """
#     try:
#         from spark_utils import SparkManager
#         from data_loader import DataLoader
#     except ImportError as e:
#         print(f"Please ensure spark_utils.py and data_loader.py are available: {e}")
#         sys.exit(1)

#     print("--- Starting DataTransformer Class Test ---")
    
#     # 1. Initialize Spark Session and DataLoader
#     spark_manager = SparkManager()
#     spark = spark_manager.create_session("TransformationTest")
#     data_loader = DataLoader(spark)
    
#     # 2. Define the directory for cleaned data
#     cleaned_data_dir = "data/cleaned/"
    
#     # 3. Load the cleaned datasets
#     cleaned_dataframes = data_loader.load_all_parquet_from_directory(cleaned_data_dir)
    
#     if not cleaned_dataframes:
#         print(f"ERROR: No cleaned data found in '{cleaned_data_dir}'. Please run data_cleaning.py first.")
#         spark.stop()
#         sys.exit(1)
        
#     # 4. Get the required DataFrames for the join process
#     flight_df = cleaned_dataframes.get("customer_flight_activity")
#     loyalty_df = cleaned_dataframes.get("customer_loyalty_history")
    
#     # 5. Check for DataFrame availability
#     if flight_df is None or loyalty_df is None:
#         print("ERROR: 'customer_flight_activity' or 'customer_loyalty_history' DataFrame not found.")
#         print("Please ensure both datasets exist in the cleaned data directory.")
#         spark.stop()
#         sys.exit(1)
    
#     # 6. Instantiate the transformer and run the join function
#     data_transformer = DataTransformer()
#     merged_customer_data = data_transformer.join_customer_data(flight_df, loyalty_df)
    
#     # 7. Display results for verification
#     if merged_customer_data:
#         print("\n--- Sample of Merged Customer Data ---")
#         merged_customer_data.show(10, truncate=False)
        
#         print("\n--- Schema of Merged Data ---")
#         merged_customer_data.printSchema()

#     # 8. Stop the Spark Session
#     spark.stop()
#     print("\n--- DataTransformer Class Test Completed ---")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, skewness, mean as spark_mean, desc, row_number, lit, monotonically_increasing_id, last
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType
from pyspark.sql.window import Window
import sys
import os

class DataCleaner:
    """
    Encapsulates a series of data cleaning and preprocessing steps for Spark DataFrames.

    This class provides a structured workflow for handling common data quality issues,
    including adjusting column names, handling duplicates, checking for null values,
    and performing data imputation based on statistical properties.

    Methods:
        run_full_cleaning_process(df, df_name, drop_null_threshold):
            Executes the complete cleaning pipeline on a given DataFrame.
    """

    def __init__(self):
        """
        Initializes the DataCleaner.
        """
        pass

    def _adjust_column_names(self, df: DataFrame, df_name: str) -> DataFrame:
        """
        Adjusts column names to be SQL-friendly by removing spaces and special characters.

        This is a private helper method.

        Parameters:
            df (DataFrame): The Spark DataFrame to process.
            df_name (str): The name of the DataFrame for logging purposes.

        Returns:
            DataFrame: A new DataFrame with standardized column names.
        """
        print(f"\n--- Checking and Adjusting Data Types for: {df_name} ---")
        
        for column_name in df.columns:
            new_column_name = column_name.replace(' ', '_').replace('.', '')
            if new_column_name != column_name:
                df = df.withColumnRenamed(column_name, new_column_name)
                print(f" -> Column '{column_name}' renamed to '{new_column_name}'")
        
        print(f"\nFinal Schema for {df_name}:")
        df.printSchema()
        return df

    def _check_duplicates(self, df: DataFrame, df_name: str) -> int:
        """
        Checks for and reports the number of duplicate rows in a DataFrame.

        This private helper method also displays the details of any duplicate rows found.

        Parameters:
            df (DataFrame): The Spark DataFrame to check.
            df_name (str): The name of the DataFrame for logging purposes.

        Returns:
            int: The total count of duplicate rows.
        """
        print(f"\n--- Checking for Duplicates in: {df_name} ---")
        initial_count = df.count()
        distinct_count = df.distinct().count()
        duplicate_rows_count = initial_count - distinct_count

        if duplicate_rows_count > 0:
            print(f" -> Found a total of {duplicate_rows_count} duplicate row(s).")
            print(" -> Details of duplicate rows and their total occurrence count:")
            duplicate_details = df.groupBy(df.columns).count().where(col("count") > 1)
            duplicate_details.show(truncate=False)
        else:
            print(" -> No duplicate rows found.")
            
        return duplicate_rows_count

    def _handle_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Removes duplicate rows from a DataFrame, keeping the first occurrence.

        This is a private helper method.

        Parameters:
            df (DataFrame): The Spark DataFrame with duplicates.

        Returns:
            DataFrame: A new DataFrame with duplicates removed.
        """
        print(" -> Removing duplicates using a Window Function (keeping one row per set)...")
        window_spec = Window.partitionBy(df.columns).orderBy(lit(1))
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        deduplicated_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
        return deduplicated_df

    def _check_skewness(self, df: DataFrame) -> dict:
        """
        Calculates the skewness for each numeric column in the DataFrame.

        This is a private helper method.

        Parameters:
            df (DataFrame): The Spark DataFrame to analyze.

        Returns:
            dict: A dictionary where keys are column names and values are their skewness.
        """
        print(f"\n--- Calculating Skewness ---")
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
        if not numeric_cols:
            print(" -> No numeric columns found to calculate skewness.")
            return {}
            
        skewness_exprs = [skewness(col(c)).alias(c) for c in numeric_cols]
        skewness_results = df.agg(*skewness_exprs).first().asDict()
        print(f" -> Skewness successfully calculated for {len(skewness_results)} column(s).")
        return skewness_results

    def _check_nulls(self, df: DataFrame, df_name: str) -> dict:
        """
        Checks for null values in each column and reports the count and percentage.

        This is a private helper method.

        Parameters:
            df (DataFrame): The Spark DataFrame to check.
            df_name (str): The name of the DataFrame for logging purposes.

        Returns:
            dict: A dictionary where keys are column names and values are details
                  about the null counts and percentages.
        """
        print(f"\n--- Checking for Null Values in: {df_name} ---")
        total_rows = df.count()
        if total_rows == 0:
            print(" -> DataFrame is empty, no nulls to check.")
            return {}

        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).first().asDict()
        
        null_info = {}
        for column, num_nulls in null_counts.items():
            if num_nulls > 0:
                percentage = (num_nulls / total_rows) * 100
                null_info[column] = {'count': num_nulls, 'percentage': percentage}

        if null_info:
            print(" -> Found null values in the following columns:")
            for col_name, info in null_info.items():
                print(f"    - {col_name}: {info['count']} nulls ({info['percentage']:.2f}%)")
        else:
            print(" -> No null values found in any column.")
            
        return null_info

    def _handle_nulls(self, df: DataFrame, null_info: dict, skewness_map: dict, drop_threshold: float) -> DataFrame:
        """
        Handles null values by dropping columns or imputing data.

        This private helper method drops columns with nulls exceeding a threshold, otherwise
        imputes values using mean/median for numeric data (based on skewness) or mode
        for categorical data.

        Parameters:
            df (DataFrame): The DataFrame with nulls.
            null_info (dict): Dictionary from the _check_nulls method.
            skewness_map (dict): Dictionary from the _check_skewness method.
            drop_threshold (float): The percentage threshold above which columns are dropped.

        Returns:
            DataFrame: A new DataFrame after handling null values.
        """
        print(f"\n--- Starting Null Value Handling Process (Drop Threshold = {drop_threshold}%) ---")

        columns_to_drop = [col_name for col_name, info in null_info.items() if info['percentage'] > drop_threshold]
        
        if columns_to_drop:
            df = df.drop(*columns_to_drop)
            print(f" -> The following columns have been dropped: {columns_to_drop}")

        for column, info in null_info.items():
            if column in columns_to_drop or column not in df.columns:
                continue
                
            print(f" -> Processing imputation for column '{column}'...")
            
            column_type = df.schema[column].dataType
            
            if isinstance(column_type, NumericType):
                skew_val = skewness_map.get(column)
                method = 'mean' if skew_val is not None and -0.5 <= skew_val <= 0.5 else 'median'
                
                if method == 'mean':
                    impute_value = df.select(spark_mean(col(column))).first()[0]
                else:
                    impute_value = df.approxQuantile(column, [0.5], 0.01)[0]

                skew_str = f"{skew_val:.4f}" if skew_val is not None else "N/A"
                print(f"    -> Numeric column. Skewness = {skew_str}. Using '{method}' imputation.")
                df = df.fillna({column: impute_value})

            elif isinstance(column_type, StringType):
                impute_value = df.groupBy(column).count().orderBy(desc("count")).first()[0]
                print(f"    -> Categorical column. Using 'mode' imputation.")
                df = df.fillna({column: impute_value})
            
            elif isinstance(column_type, (DateType, TimestampType)):
                print(f"    -> Date column. Imputing using Last Observation Carried Forward (LOCF)...")
                if "temp_order_id" not in df.columns:
                     df = df.withColumn("temp_order_id", monotonically_increasing_id())

                window_spec = Window.orderBy("temp_order_id").rowsBetween(Window.unboundedPreceding, 0)
                filled_col = last(col(column), ignorenulls=True).over(window_spec)
                df = df.withColumn(column, when(col(column).isNull(), filled_col).otherwise(col(column)))
                df = df.drop("temp_order_id")

            else:
                print(f"    -> Column type '{column}' ({str(column_type)}) is not handled for automatic imputation.")
                
        return df

    def run_full_cleaning_process(self, df: DataFrame, df_name: str, drop_null_threshold: float = 50.0) -> DataFrame:
        """
        Executes the full cleaning workflow for a single DataFrame.

        This method orchestrates the sequence of cleaning steps:
        1. Adjusts column names.
        2. Checks for and handles duplicates.
        3. Calculates skewness for imputation decisions.
        4. Checks for and handles null values.

        Parameters:
            df (DataFrame): The raw Spark DataFrame to be cleaned.
            df_name (str): The name of the dataset for logging.
            drop_null_threshold (float): The percentage of nulls in a column to trigger dropping it.
                                         Defaults to 50.0.

        Returns:
            DataFrame: A cleaned and processed Spark DataFrame.
        """
        print(f"\n==================== Starting Full Cleaning Process for: {df_name} ====================")
        df_adjusted = self._adjust_column_names(df, df_name)
        
        if self._check_duplicates(df_adjusted, df_name) > 0:
            df_deduplicated = self._handle_duplicates(df_adjusted)
        else:
            df_deduplicated = df_adjusted
        
        skewness_info = self._check_skewness(df_deduplicated)
        null_info = self._check_nulls(df_deduplicated, df_name)

        if null_info:
            df_cleaned = self._handle_nulls(df_deduplicated, null_info, skewness_info, drop_null_threshold)
        else:
            df_cleaned = df_deduplicated

        print(f"==================== Full Cleaning for {df_name} Completed ====================")
        
        return df_cleaned


# if __name__ == '__main__':
#     """
#     Main execution block to test the DataCleaner class.
#     """
#     try:
#         from spark_utils import SparkManager
#         from data_loader import DataLoader
#         from data_saver import save_dataframes_as_parquet
#     except ImportError:
#         print("Please ensure spark_utils.py, data_loader.py, and data_saver.py are available.")
#         sys.exit(1)

#     print("--- Starting Data Cleaning Module Test ---")
    
#     # 1. Initialize managers and session
#     spark_manager = SparkManager()
#     spark = spark_manager.create_session("DataCleaningTest")
#     data_loader = DataLoader(spark)
    
#     # 2. Load raw data
#     raw_dataframes = data_loader.load_all_csv_from_directory("data/raw")
    
#     if not raw_dataframes:
#         print("No data available to clean. Halting process.")
#         spark.stop()
#         sys.exit(1)
        
#     # 3. Instantiate the cleaner
#     data_cleaner = DataCleaner()
    
#     # 4. Run the cleaning process
#     cleaned_dataframes = {}
#     for name, df_raw in raw_dataframes.items():
#         cleaned_df = data_cleaner.run_full_cleaning_process(df_raw, name, drop_null_threshold=50.0)
#         cleaned_dataframes[name] = cleaned_df
        
#         print(f"\n--- Sample of Cleaned Data for: {name} ---")
#         cleaned_df.show(5)

#     # 5. Save the cleaned data
#     output_directory = "data/cleaned"
#     save_dataframes_as_parquet(cleaned_dataframes, output_directory)

#     print("\n--- Data Cleaning Module Test Completed ---")
#     spark.stop()

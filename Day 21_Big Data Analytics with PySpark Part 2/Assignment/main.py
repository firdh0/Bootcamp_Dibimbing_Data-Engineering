# main.py

import os
import sys

# Add the project's root directory to the system path to ensure all modules can be imported
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

try:
    # Import all necessary classes from their respective modules
    from spark_utils import SparkManager
    from data_loader import DataLoader
    from data_cleaner import DataCleaner
    from data_saver import DataSaver
    from data_transformer import DataTransformer
    from data_analyzer import CustomerDataAnalyzer
    from data_visualizer import DataVisualizer
except ImportError as e:
    print(f"Error: Failed to import a required module. Please ensure all .py files are in the same directory.")
    print(f"Error detail: {e}")
    sys.exit(1)

class DataPipeline:
    """
    Orchestrates the entire end-to-end data processing workflow.

    This class encapsulates the full ETL (Extract, Transform, Load) and analysis
    pipeline, from loading raw data to generating final visualizations. It utilizes
    specialized classes for each major step of the process.

    Methods:
        run():
            Executes the complete data pipeline in a sequential manner.
    """

    def __init__(self):
        """
        Initializes the DataPipeline and its component handlers.
        """
        print("\n==================== DATA PROCESSING PIPELINE INITIALIZED ====================")
        self.spark_manager = SparkManager()
        self.data_saver = DataSaver()
        self.data_transformer = DataTransformer()
        self.data_cleaner = DataCleaner()
        # Other classes will be initialized after the SparkSession is active

    def run(self):
        """
        Executes the full data processing pipeline.

        The sequence of operations is as follows:
        1.  **Initialization**: A SparkSession is created.
        2.  **Extraction & Cleaning**: Raw data is loaded, cleaned, and saved as Parquet.
        3.  **Transformation**: Cleaned datasets are joined to create a comprehensive view.
        4.  **Analysis**: SQL-based analyses are run on the transformed data.
        5.  **Visualization**: Results from the analysis are plotted and saved as images.
        """
        spark = None
        try:
            # STAGE 0: INITIALIZATION
            spark = self.spark_manager.create_session(app_name="FullETLPipeline")
            data_loader = DataLoader(spark)
            customer_analyzer = CustomerDataAnalyzer(spark)
            data_visualizer = DataVisualizer()
            
            raw_data_dir = "data/raw"
            cleaned_data_dir = "data/cleaned"
            visualization_output_dir = "final_visualizations"

            # STAGE 1: DATA EXTRACTION & CLEANING
            print("\n--- STAGE 1: Loading and Cleaning Raw Data ---")
            raw_dataframes = data_loader.load_all_csv_from_directory(raw_data_dir)
            
            if not raw_dataframes:
                print(f"CRITICAL: No raw data found in '{raw_data_dir}'. Halting pipeline.")
                return

            cleaned_dataframes = {}
            for name, df_raw in raw_dataframes.items():
                cleaned_df = self.data_cleaner.run_full_cleaning_process(df_raw, name)
                cleaned_dataframes[name] = cleaned_df
            
            # Save cleaned data to disk (checkpointing step)
            self.data_saver.save_as_parquet(cleaned_dataframes, cleaned_data_dir)
            print("--- STAGE 1 Complete: Data has been cleaned and saved. ---\n")

            # STAGE 2: DATA TRANSFORMATION
            print("\n--- STAGE 2: Joining Customer Data ---")
            flight_df = cleaned_dataframes.get("customer_flight_activity")
            loyalty_df = cleaned_dataframes.get("customer_loyalty_history")

            if flight_df is None or loyalty_df is None:
                print("CRITICAL: 'customer_flight_activity' or 'customer_loyalty_history' not found after cleaning. Halting pipeline.")
                return
                
            merged_df = self.data_transformer.join_customer_data(flight_df, loyalty_df)
            print("--- STAGE 2 Complete: Data successfully joined. ---\n")
            
            # STAGE 3: DATA ANALYSIS
            print("\n--- STAGE 3: Running SQL Analyses ---")
            analysis_results = customer_analyzer.run_all_analyses(merged_df, flight_df)
            print("--- STAGE 3 Complete: All analyses have been executed. ---\n")
            
            # STAGE 4: DATA VISUALIZATION
            print("\n--- STAGE 4: Creating Visualizations from Analysis Results ---")
            if analysis_results:
                data_visualizer.create_all_visualizations(analysis_results, output_folder=visualization_output_dir)
            else:
                print("Warning: No analysis results were generated to visualize.")
            print("--- STAGE 4 Complete: Visualizations have been created. ---\n")

        except Exception as error:
            print(f"\nFATAL ERROR: An exception occurred during the pipeline execution: {error}")
            import traceback
            traceback.print_exc()
        finally:
            if spark:
                spark.stop()
                print("\nSpark Session has been stopped.")
            
            print("\n==================== DATA PROCESSING PIPELINE FINISHED ====================")

if __name__ == "__main__":
    # Instantiate the pipeline and run it
    pipeline = DataPipeline()
    pipeline.run()

import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import sys
from pyspark.sql import DataFrame

class DataVisualizer:
    """
    Manages the creation and saving of various plots from Spark DataFrames.

    This class provides a suite of methods to generate different types of
    visualizations (e.g., bar charts, line charts, scatter plots) from analysis
    results and save them as image files.

    Methods:
        create_all_visualizations(analysis_results_dict, output_folder):
            Orchestrates the creation of all predefined plots from a dictionary
            of analysis DataFrames.
    """


    def __init__(self):
        """
        Initializes the DataVisualizer.
        """
        pass


    def _ensure_output_folder_exists(self, output_folder: str):
        """
        Ensures the output folder exists; if not, it creates the folder.

        This is a private helper method.

        Parameters:
            output_folder (str): The path to the directory for saving visualizations.
        """
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            print(f" -> Output folder '{output_folder}' created.")


    def _plot_bar_chart(self, df_spark: DataFrame, x_col: str, y_col: str, title: str, xlabel: str, ylabel: str, output_filename: str, output_folder: str, palette: str, sort_by_y: bool):
        """
        Creates and saves a bar chart from a Spark DataFrame.

        This is a private helper method.

        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data untuk diplot.
            x_col (str): Nama kolom yang akan digunakan sebagai sumbu X (kategori).
            y_col (str): Nama kolom yang akan digunakan sebagai sumbu Y (nilai).
            title (str): Judul utama dari grafik.
            xlabel (str): Label untuk sumbu X.
            ylabel (str): Label untuk sumbu Y.
            output_filename (str): Nama file untuk menyimpan gambar (misal: 'grafik.png').
            output_folder (str): Folder tempat gambar akan disimpan.
            palette (str): Palet warna Seaborn yang akan digunakan.
            sort_by_y (bool): Jika True, grafik batang akan diurutkan berdasarkan nilai sumbu Y.
        """
        df_pd = df_spark.toPandas()
        plt.figure(figsize=(12, 7))
        order = df_pd.sort_values(y_col, ascending=False)[x_col] if sort_by_y else None
        sns.barplot(x=x_col, y=y_col, data=df_pd, palette=palette, order=order)
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def _plot_line_chart(self, df_spark: DataFrame, x_col: str, y_col: str, title: str, xlabel: str, ylabel: str, output_filename: str, output_folder: str):
        """
        Creates and saves a line chart from a Spark DataFrame.
        
        This private helper method automatically creates a 'Date' column from 'Year'
        and 'Month' if they exist, for better time-series plotting.
        
        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data time-series.
            x_col (str): Nama kolom untuk sumbu X (biasanya waktu).
            y_col (str): Nama kolom untuk sumbu Y (nilai).
            title (str): Judul utama dari grafik.
            xlabel (str): Label untuk sumbu X.
            ylabel (str): Label untuk sumbu Y.
            output_filename (str): Nama file untuk menyimpan gambar.
            output_folder (str): Folder tempat gambar akan disimpan.
        """
        df_pd = df_spark.toPandas()
        if 'Date' not in df_pd.columns and 'Year' in df_pd.columns and 'Month' in df_pd.columns:
            df_pd['Date'] = pd.to_datetime(df_pd['Year'].astype(str) + '-' + df_pd['Month'].astype(str))
            x_col = 'Date'
        df_pd = df_pd.sort_values(by=x_col)
        plt.figure(figsize=(14, 7))
        sns.lineplot(x=x_col, y=y_col, data=df_pd, marker='o')
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.grid(True)
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def _plot_scatter_plot(self, df_spark: DataFrame, x_col: str, y_col: str, title: str, xlabel: str, ylabel: str, output_filename: str, output_folder: str):
        """
        Creates and saves a scatter plot.
        
        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data untuk diplot.
            x_col (str): Nama kolom untuk sumbu X (variabel pertama).
            y_col (str): Nama kolom untuk sumbu Y (variabel kedua).
            title (str): Judul utama dari grafik.
            xlabel (str): Label untuk sumbu X.
            ylabel (str): Label untuk sumbu Y.
            output_filename (str): Nama file untuk menyimpan gambar.
            output_folder (str): Folder tempat gambar akan disimpan.
        """
        df_pd = df_spark.toPandas()
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=x_col, y=y_col, data=df_pd, alpha=0.6)
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.grid(True)
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def _plot_box_plot(self, df_spark: DataFrame, x_col: str, y_col: str, title: str, xlabel: str, ylabel: str, output_filename: str, output_folder: str):
        """
        Creates and saves a box plot.
        
        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data untuk diplot.
            x_col (str): Nama kolom untuk sumbu X (kategori yang akan dibandingkan).
            y_col (str): Nama kolom untuk sumbu Y (variabel numerik yang distribusinya akan ditampilkan).
            title (str): Judul utama dari grafik.
            xlabel (str): Label untuk sumbu X.
            ylabel (str): Label untuk sumbu Y.
            output_filename (str): Nama file untuk menyimpan gambar.
            output_folder (str): Folder tempat gambar akan disimpan.
        """
        df_pd = df_spark.toPandas()
        plt.figure(figsize=(12, 7))
        sns.boxplot(x=x_col, y=y_col, data=df_pd, palette="pastel")
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def _plot_histogram(self, df_spark: DataFrame, col_name: str, title: str, xlabel: str, output_filename: str, output_folder: str, bins: int):
        """
        Creates and saves a histogram.
        
        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data.
            col_name (str): Nama kolom numerik yang distribusinya akan ditampilkan.
            title (str): Judul utama dari grafik.
            xlabel (str): Label untuk sumbu X.
            output_filename (str): Nama file untuk menyimpan gambar.
            output_folder (str): Folder tempat gambar akan disimpan.
            bins (int): Jumlah 'bin' atau 'batang' yang akan digunakan dalam histogram.
        """
        df_pd = df_spark.toPandas()
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df_pd, x=col_name, bins=bins, kde=True)
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel("Frequency", fontsize=12)
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def _plot_pie_chart(self, df_spark: DataFrame, labels_col: str, values_col: str, title: str, output_filename: str, output_folder: str):
        """
        Creates and saves a pie chart.
        
        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data.
            labels_col (str): Nama kolom yang berisi label untuk setiap 'potongan' pie.
            values_col (str): Nama kolom numerik yang menentukan ukuran setiap 'potongan' pie.
            title (str): Judul utama dari grafik.
            output_filename (str): Nama file untuk menyimpan gambar.
            output_folder (str): Folder tempat gambar akan disimpan.
        """
        df_pd = df_spark.toPandas()
        plt.figure(figsize=(8, 8))
        plt.pie(df_pd[values_col], labels=df_pd[labels_col], autopct='%1.1f%%', startangle=140, colors=sns.color_palette("Set2"))
        plt.title(title, fontsize=16)
        plt.axis('equal')
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def _plot_grouped_bar_chart(self, df_spark: DataFrame, x_col: str, y_col: str, hue_col: str, title: str, xlabel: str, ylabel: str, output_filename: str, output_folder: str):
        """
        Creates and saves a grouped bar chart.
        
        Parameters:
            df_spark (DataFrame): DataFrame Spark yang berisi data.
            x_col (str): Nama kolom untuk sumbu X (kategori utama).
            y_col (str): Nama kolom untuk sumbu Y (nilai).
            hue_col (str): Nama kolom yang digunakan untuk membuat grup (sub-kategori) di dalam setiap kategori X.
            title (str): Judul utama dari grafik.
            xlabel (str): Label untuk sumbu X.
            ylabel (str): Label untuk sumbu Y.
            output_filename (str): Nama file untuk menyimpan gambar.
            output_folder (str): Folder tempat gambar akan disimpan.
        """
        df_pd = df_spark.toPandas()
        plt.figure(figsize=(14, 8))
        sns.barplot(data=df_pd, x=x_col, y=y_col, hue=hue_col, palette="muted")
        plt.title(title, fontsize=16)
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.legend(title=hue_col)
        plt.tight_layout()
        file_path = os.path.join(output_folder, output_filename)
        plt.savefig(file_path)
        plt.close()
        print(f" -> Visualization '{output_filename}' saved to: {file_path}")


    def create_all_visualizations(self, analysis_results_dict: dict, output_folder: str = "visualizations"):
        """
        Creates all visualizations based on the analysis results.

        This orchestrator method iterates through a dictionary of analysis results
        and calls the appropriate private plotting method for each entry.

        Parameters:
            analysis_results_dict (dict): Sebuah dictionary di mana setiap key adalah nama analisis 
                                          dan value adalah DataFrame hasil analisis tersebut.
            output_folder (str): Folder untuk menyimpan semua gambar visualisasi.
        """
        print("\n==================== Creating All Visualizations ====================")
        self._ensure_output_folder_exists(output_folder)

        plot_map = {
            "points_distribution_by_card": lambda df: self._plot_bar_chart(df, "Loyalty_Card", "Total_Points_Accumulated", "Total Points by Loyalty Card", "Loyalty Card", "Total Points", "points_by_card.png", output_folder, "viridis", True),
            "education_vs_flights": lambda df: self._plot_bar_chart(df, "Education", "Avg_Total_Flights_Per_Record", "Average Flights by Education", "Education Level", "Average Flights", "flights_by_education.png", output_folder, "magma", True),
            "flight_trends": lambda df: self._plot_line_chart(df, "Date", "Total_Flights_Per_Month", "Monthly Flight Trends", "Time (Year-Month)", "Total Flights", "flight_trends.png", output_folder),
            "salary_vs_distance": lambda df: self._plot_scatter_plot(df, "Salary", "Distance", "Salary vs. Flight Distance", "Salary", "Distance (miles)", "salary_vs_distance.png", output_folder),
            "points_exchange_rate": lambda df: self._plot_scatter_plot(df, "Points_Redeemed", "Dollar_Cost_Points_Redeemed", "Points Redeemed vs. Dollar Value", "Points Redeemed", "Cost in Dollars", "points_exchange.png", output_folder),
            "salary_distribution_by_education": lambda df: self._plot_box_plot(df, "Education", "Salary", "Salary Distribution by Education", "Education Level", "Salary", "salary_dist_by_education.png", output_folder),
            "overall_salary_distribution": lambda df: self._plot_histogram(df, "Salary", "Overall Customer Salary Distribution", "Salary", "salary_distribution.png", output_folder, 30),
            "demographic_composition": lambda df: self._plot_grouped_bar_chart(df, "Education", "Customer_Count_Per_Segment", "Marital_Status", "Customer Demographics by Education", "Education Level", "Number of Customers", "demographic_composition.png", output_folder),
            "regional_activity": lambda df: self._plot_bar_chart(df, "Province", "Total_Flights_Per_Province", "Total Flights by Province", "Province", "Total Flights", "regional_activity.png", output_folder, "crest", True),
            "regional_redemption_value": lambda df: self._plot_bar_chart(df, "Province", "Total_Redemption_Value", "Total Redemption Value (USD) by Province", "Province", "Total Redemption Value (USD)", "regional_redemption.png", output_folder, "rocket", True),
            "financial_demographics": lambda df: self._plot_bar_chart(df, "Marital_Status", "Average_Salary", "Average Salary by Marital Status", "Marital Status", "Average Salary", "avg_salary_by_marital_status.png", output_folder, "cubehelix", True),
            "gender_composition": lambda df: self._plot_pie_chart(df, "Gender", "Number_of_Customers", "Customer Composition by Gender", "gender_composition.png", output_folder),
            "loyalty_tier_engagement": lambda df: self._plot_bar_chart(df, "Loyalty_Card", "Total_Distance_Flown", "Total Distance Flown by Loyalty Tier", "Loyalty Card", "Total Distance (miles)", "tier_engagement_by_distance.png", output_folder, "plasma", True)
        }

        for key, df in analysis_results_dict.items():
            if key in plot_map:
                print(f" -> Creating visualization for '{key}'...")
                if df and not df.rdd.isEmpty():
                    plot_map[key](df)
                else:
                    print(f" -> Skipping visualization for '{key}' because the DataFrame is empty.")
            else:
                print(f" -> Skipping visualization for '{key}': No plot defined.")

        print("==================== All Visualizations Have Been Created ====================")


if __name__ == '__main__':
    """
    Main execution block to test the DataVisualizer class.
    """
    try:
        from spark_utils import SparkManager
        from data_loader import DataLoader
        from data_transformer import DataTransformer
        from data_analyzer import CustomerDataAnalyzer
    except ImportError as e:
        print(f"Please ensure all required modules are available: {e}")
        sys.exit(1)

    print("--- Starting Visualizer Module Test ---")
    
    spark_manager = SparkManager()
    spark = spark_manager.create_session("VisualizerTest")
    
    data_loader = DataLoader(spark)
    data_transformer = DataTransformer()
    customer_analyzer = CustomerDataAnalyzer(spark)
    
    cleaned_data_dir = "data/cleaned/"
    cleaned_dataframes = data_loader.load_all_parquet_from_directory(cleaned_data_dir)
    
    if not cleaned_dataframes:
        print("ERROR: No cleaned data found. Please run the cleaning process first.")
        spark.stop()
        sys.exit(1)
        
    flight_df = cleaned_dataframes.get("customer_flight_activity")
    loyalty_df = cleaned_dataframes.get("customer_loyalty_history")
    
    if flight_df is None or loyalty_df is None:
        print("ERROR: Required 'customer_flight_activity' or 'customer_loyalty_history' DataFrame not found.")
        spark.stop()
        sys.exit(1)
    
    merged_df = data_transformer.join_customer_data(flight_df, loyalty_df)
    
    if merged_df:
        analysis_results = customer_analyzer.run_all_analyses(merged_df, flight_df)
        if analysis_results:
            visualizer = DataVisualizer()
            visualizer.create_all_visualizations(analysis_results, output_folder="test_visualizations")
        else:
            print(" -> No analysis results to visualize.")
    else:
        print(" -> Merging failed, cannot proceed to analysis and visualization.")

    spark.stop()
    print("\n--- Visualizer Module Test Completed ---")

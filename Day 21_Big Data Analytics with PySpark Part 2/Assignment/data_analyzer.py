from pyspark.sql import DataFrame, SparkSession
import sys
import os

try:
    from data_saver import DataSaver
except ImportError:
    print("Warning: Could not import DataSaver class.")
    DataSaver = None

class CustomerDataAnalyzer:
    """
    Encapsulates methods for performing various SQL-based analyses on customer flight data.

    This class provides a structured way to run a series of predefined analyses
    on customer loyalty and flight activity DataFrames. It requires an active SparkSession
    for its operations.

    Attributes:
        spark (SparkSession): An active SparkSession instance used for all SQL queries.

    Methods:
        run_all_analyses(merged_df, flight_activity_df):
            Orchestrates the execution of all available analysis methods.
        analyze_...():
            A series of methods, each performing a specific analysis and returning a DataFrame.
    """


    def __init__(self, spark: SparkSession):
        """
        Initializes the CustomerDataAnalyzer with an active SparkSession.

        Parameters:
            spark (SparkSession): The SparkSession to be used for data analysis.
        """
        self.spark = spark

        if DataSaver:
            self.data_saver = DataSaver()
        else:
            self.data_saver = None


    def _register_temp_views(self, **kwargs: DataFrame):
        """
        Registers one or more DataFrames as temporary SQL views.

        This is a private helper method used to make DataFrames queryable via SQL.

        Parameters:
            **kwargs: A dictionary where keys are the desired view names (str)
                      and values are the corresponding Spark DataFrames.
        """
        for view_name, df in kwargs.items():
            df.createOrReplaceTempView(view_name)
            print(f" -> DataFrame '{view_name}' registered as a temporary view.")


    def analyze_avg_flights_per_customer_year(self) -> DataFrame:
        """
        Analyzes the average number of flights per customer for each year.

        Returns:
            DataFrame: A DataFrame with Loyalty_Number, Year, and Avg_Flights_Per_Year.
        """
        print("\n1. Analyzing the average number of flights per customer per year...")
        query = """
            SELECT
                Loyalty_Number,
                Year,
                AVG(Total_Flights) as Avg_Flights_Per_Year
            FROM flight_activity_view
            GROUP BY Loyalty_Number, Year
            ORDER BY Loyalty_Number, Year
        """
        result_df = self.spark.sql(query)
        print(" -> Average flights per customer result:")
        result_df.show(10)
        return result_df


    def analyze_points_by_loyalty_card(self) -> DataFrame:
        """
        Analyzes the distribution of loyalty points by loyalty card status.

        Returns:
            DataFrame: A DataFrame with Loyalty_Card, Total_Points_Accumulated,
                       and Avg_Points_Accumulated_Per_Record.
        """
        print("\n2. Analyzing loyalty points distribution by loyalty card status...")
        query = """
            SELECT
                Loyalty_Card,
                SUM(Points_Accumulated) as Total_Points_Accumulated,
                AVG(Points_Accumulated) as Avg_Points_Accumulated_Per_Record
            FROM merged_view
            WHERE Loyalty_Card IS NOT NULL
            GROUP BY Loyalty_Card
            ORDER BY Total_Points_Accumulated DESC
        """
        result_df = self.spark.sql(query)
        print(" -> Points distribution by loyalty card result:")
        result_df.show()
        return result_df


    def analyze_education_vs_flights(self) -> DataFrame:
        """
        Analyzes the relationship between customer education level and flight frequency.

        Returns:
            DataFrame: A DataFrame with Education, Avg_Total_Flights_Per_Record,
                       and Sum_Total_Flights.
        """
        print("\n3. Analyzing the relationship between customer education and number of flights...")
        query = """
            SELECT
                Education,
                AVG(Total_Flights) as Avg_Total_Flights_Per_Record,
                SUM(Total_Flights) as Sum_Total_Flights
            FROM merged_view
            WHERE Education IS NOT NULL
            GROUP BY Education
            ORDER BY Avg_Total_Flights_Per_Record DESC
        """
        result_df = self.spark.sql(query)
        print(" -> Education vs. number of flights analysis result:")
        result_df.show()
        return result_df


    def analyze_flight_trends_over_time(self) -> DataFrame:
        """
        Analyzes the monthly trend of total flight counts over time.

        Returns:
            DataFrame: A DataFrame with Year, Month, and Total_Flights_Per_Month.
        """
        print("\n4. Analyzing flight count trends over time (by Year and Month)...")
        query = """
            SELECT
                Year,
                Month,
                SUM(Total_Flights) as Total_Flights_Per_Month
            FROM flight_activity_view
            GROUP BY Year, Month
            ORDER BY Year, Month
        """
        result_df = self.spark.sql(query)
        print(" -> Flight trend analysis result:")
        result_df.show(24)
        return result_df


    def analyze_salary_vs_distance(self) -> DataFrame:
        """
        Fetches data to analyze the relationship between customer salary and flight distance.

        Returns:
            DataFrame: A sample DataFrame containing Salary and Distance columns.
        """
        print("\n5. Fetching data for Salary vs. Flight Distance analysis...")
        query = """
            SELECT
                Salary,
                Distance
            FROM merged_view
            WHERE Salary IS NOT NULL AND Distance > 0
            LIMIT 5000 
        """
        result_df = self.spark.sql(query)
        print(" -> Salary vs. Distance data (sample):")
        result_df.show(10)
        return result_df


    def analyze_points_exchange_rate(self) -> DataFrame:
        """
        Fetches data on redeemed points and their corresponding dollar value.

        Returns:
            DataFrame: A DataFrame with Points_Redeemed and Dollar_Cost_Points_Redeemed.
        """
        print("\n6. Fetching data for Points Exchange Rate analysis...")
        query = """
            SELECT
                Points_Redeemed,
                Dollar_Cost_Points_Redeemed
            FROM flight_activity_view
            WHERE Points_Redeemed > 0
        """
        result_df = self.spark.sql(query)
        print(" -> Redeemed Points vs. Dollar Cost data:")
        result_df.show(10)
        return result_df


    def analyze_salary_distribution_by_education(self) -> DataFrame:
        """
        Fetches salary and education data for plotting distribution comparisons (e.g., box plots).

        Returns:
            DataFrame: A DataFrame containing Education and Salary columns.
        """
        print("\n7. Fetching data for Salary distribution by Education...")
        query = """
            SELECT
                Education,
                Salary
            FROM merged_view
            WHERE Education IS NOT NULL AND Salary IS NOT NULL
        """
        result_df = self.spark.sql(query)
        print(" -> Salary and Education data (for box plot):")
        result_df.show(10)
        return result_df


    def analyze_salary_distribution(self) -> DataFrame:
        """
        Fetches all customer salaries to analyze the overall distribution.

        Returns:
            DataFrame: A single-column DataFrame of customer salaries.
        """
        print("\n8. Fetching data for overall Salary distribution...")
        query = """
            SELECT Salary 
            FROM merged_view TABLESAMPLE (10 PERCENT)
            WHERE Salary IS NOT NULL
        """
        result_df = self.spark.sql(query)
        print(" -> Salary data (for histogram):")
        result_df.show(10)
        return result_df


    def analyze_demographic_composition(self) -> DataFrame:
        """
        Analyzes the count of customers based on marital status within each education level.

        Returns:
            DataFrame: A DataFrame with Education, Marital_Status, and Customer_Count_Per_Segment.
        """
        print("\n9. Analyzing Marital Status composition per Education Level...")
        query = """
            SELECT
                Education,
                Marital_Status,
                COUNT(*) as Customer_Count_Per_Segment
            FROM merged_view
            WHERE Education IS NOT NULL AND Marital_Status IS NOT NULL
            GROUP BY Education, Marital_Status
            ORDER BY Education, Marital_Status
        """
        result_df = self.spark.sql(query)
        print(" -> Demographic Composition result:")
        result_df.show()
        return result_df


    def analyze_regional_activity(self) -> DataFrame:
        """
        Calculates the total number of flights for each province.

        Returns:
            DataFrame: A DataFrame showing Province and its Total_Flights_Per_Province.
        """
        print("\n10. Analyzing flight activity per Province...")
        query = """
            SELECT
                Province,
                SUM(Total_Flights) as Total_Flights_Per_Province
            FROM merged_view
            WHERE Province IS NOT NULL
            GROUP BY Province
            ORDER BY Total_Flights_Per_Province DESC
        """
        result_df = self.spark.sql(query)
        print(" -> Regional Activity result:")
        result_df.show()
        return result_df


    def analyze_redemption_value_by_region(self) -> DataFrame:
        """
        Calculates the total dollar value of redeemed points for each province.

        Returns:
            DataFrame: A DataFrame showing Province and its Total_Redemption_Value.
        """
        print("\n11. Analyzing points redemption value per Province...")
        query = """
            SELECT
                Province,
                SUM(Dollar_Cost_Points_Redeemed) as Total_Redemption_Value
            FROM merged_view
            WHERE Province IS NOT NULL AND Dollar_Cost_Points_Redeemed > 0
            GROUP BY Province
            ORDER BY Total_Redemption_Value DESC
        """
        result_df = self.spark.sql(query)
        print(" -> Points Redemption Value by Region result:")
        result_df.show()
        return result_df


    def analyze_financial_demographics(self) -> DataFrame:
        """
        Calculates the average customer salary grouped by marital status.

        Returns:
            DataFrame: A DataFrame with Marital_Status and Average_Salary.
        """
        print("\n12. Analyzing financial demographics by Marital Status...")
        query = """
            SELECT
                Marital_Status,
                AVG(Salary) as Average_Salary
            FROM merged_view
            WHERE Marital_Status IS NOT NULL AND Salary IS NOT NULL
            GROUP BY Marital_Status
            ORDER BY Average_Salary DESC
        """
        result_df = self.spark.sql(query)
        print(" -> Average Salary by Marital Status result:")
        result_df.show()
        return result_df


    def analyze_customer_composition_by_gender(self) -> DataFrame:
        """
        Counts the number of unique customers for each gender.

        Returns:
            DataFrame: A DataFrame with Gender and Number_of_Customers.
        """
        print("\n13. Analyzing customer composition by Gender...")
        query = """
            SELECT
                Gender,
                COUNT(DISTINCT Loyalty_Number) as Number_of_Customers
            FROM merged_view
            WHERE Gender IS NOT NULL
            GROUP BY Gender
        """
        result_df = self.spark.sql(query)
        print(" -> Customer Composition by Gender result:")
        result_df.show()
        return result_df
    

    def analyze_loyalty_tier_engagement(self) -> DataFrame:
        """
        Calculates the total distance flown by customers for each loyalty card tier.

        Returns:
            DataFrame: A DataFrame with Loyalty_Card and Total_Distance_Flown.
        """
        print("\n14. Analyzing loyalty tier engagement by total distance...")
        query = """
            SELECT
                Loyalty_Card,
                SUM(Distance) as Total_Distance_Flown
            FROM merged_view
            WHERE Loyalty_Card IS NOT NULL
            GROUP BY Loyalty_Card
            ORDER BY Total_Distance_Flown DESC
        """
        result_df = self.spark.sql(query)
        print(" -> Loyalty Tier Engagement result:")
        result_df.show()
        return result_df


    def run_all_analyses(self, merged_df: DataFrame, flight_activity_df: DataFrame) -> dict:
        """
        Runs the complete suite of analysis methods.

        This orchestrator method first registers the necessary DataFrames as temporary
        views and then calls each individual analysis method in sequence.

        Parameters:
            merged_df (DataFrame): The cleaned and merged DataFrame containing combined
                                   customer and flight data.
            flight_activity_df (DataFrame): The cleaned DataFrame containing only
                                            flight activity data.

        Returns:
            dict: A dictionary where keys are descriptive names of the analyses and
                  values are the resulting Spark DataFrames.
        """
        print("\n==================== Starting All SQL Analyses ====================")
        if flight_activity_df is None or merged_df is None:
            print(" -> ERROR: Required DataFrames for analysis are not available.")
            return {}

        self._register_temp_views(merged_view=merged_df, flight_activity_view=flight_activity_df)
    
        analysis_results = {
            "avg_flights_per_customer_year": self.analyze_avg_flights_per_customer_year(),
            "points_distribution_by_card": self.analyze_points_by_loyalty_card(),
            "education_vs_flights": self.analyze_education_vs_flights(),
            "flight_trends": self.analyze_flight_trends_over_time(),
            "salary_vs_distance": self.analyze_salary_vs_distance(),
            "points_exchange_rate": self.analyze_points_exchange_rate(),
            "salary_distribution_by_education": self.analyze_salary_distribution_by_education(),
            "overall_salary_distribution": self.analyze_salary_distribution(),
            "demographic_composition": self.analyze_demographic_composition(),
            "regional_activity": self.analyze_regional_activity(),
            "regional_redemption_value": self.analyze_redemption_value_by_region(),
            "financial_demographics": self.analyze_financial_demographics(),
            "gender_composition": self.analyze_customer_composition_by_gender(),
            "loyalty_tier_engagement": self.analyze_loyalty_tier_engagement()
        }
    
        print("==================== All SQL Analyses Completed ====================")

        if self.data_saver:
            analysis_output_dir = "data/analysis_results_csv"
            print(f"\n--- Saving all analysis results to '{analysis_output_dir}' (CSV Format) ---")
            self.data_saver.save_as_csv(analysis_results, analysis_output_dir)
        else:
            print("\nWarning: DataSaver not available. Skipping saving of analysis results.")

        return analysis_results


# if __name__ == '__main__':
#     """
#     Main execution block to test the CustomerDataAnalyzer class.
#     """
#     try:
#         from spark_utils import SparkManager
#         from data_loader import DataLoader
#     except ImportError as e:
#         print(f"Please ensure the spark_utils and data_loader modules are available: {e}")
#         sys.exit(1)

#     print("--- Starting Analyzer Class Test ---")
#     spark_manager = SparkManager()
#     spark = spark_manager.create_session("AnalyzerClassTest")
    
#     data_loader = DataLoader(spark)
    
#     cleaned_data_dir = "data/cleaned/"
#     cleaned_dataframes = data_loader.load_all_parquet_from_directory(cleaned_data_dir)
    
#     if not cleaned_dataframes:
#         print("ERROR: No cleaned data could be loaded. Please ensure 'data_cleaning.py' has been run.")
#         spark.stop()
#         sys.exit(1)
        
#     flight_df = cleaned_dataframes.get("customer_flight_activity")
#     loyalty_df = cleaned_dataframes.get("customer_loyalty_history")
    
#     if flight_df is None or loyalty_df is None:
#         print("ERROR: 'customer_flight_activity' or 'customer_loyalty_history' not found among cleaned data.")
#         spark.stop()
#         sys.exit(1)
    
#     print("\n--- Performing JOIN for analysis testing ---")
#     merged_df = flight_df.join(loyalty_df, on="Loyalty_Number", how="left")
#     print(" -> JOIN successful.")
    
#     # 1. Create an instance of the analyzer class
#     analyzer = CustomerDataAnalyzer(spark)
    
#     # 2. Run all analyses using the instance method
#     analysis_results = analyzer.run_all_analyses(merged_df, flight_df)
    
#     print("\n--- Sample Analysis Result ---")
#     if "loyalty_tier_engagement" in analysis_results:
#         print("Loyalty Tier Engagement:")
#         analysis_results["loyalty_tier_engagement"].show(5)

#     spark.stop()
#     print("\n--- Analyzer Class Test Completed ---")

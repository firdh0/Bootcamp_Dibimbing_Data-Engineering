from pyspark.sql import SparkSession

def initialize_spark() -> SparkSession:
    """
    Initializes and returns a SparkSession for the ETL process.

    This function creates a SparkSession with the specified application name
    and sets the Spark context log level to WARN to reduce log verbosity.

    Returns:
        SparkSession: The initialized SparkSession instance that can be used
        for executing Spark jobs and transformations.
    """
   
    print("Initializing Spark Session...")
    spark = SparkSession.builder.appName("ModularCustomerAnalyticsETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized successfully.")
    return spark
import os
import sys
from pyspark.sql import SparkSession

class SparkManager:
    """
    Manages the creation and configuration of a SparkSession.

    This class provides a standardized way to initialize a SparkSession with
    robust configurations to handle common environment issues, such as paths
    with spaces and specific Java/Python compatibility settings.

    Methods:
        create_session(app_name):
            Builds and returns a fully configured SparkSession.
    """


    def __init__(self):
        """
        Initializes the SparkManager.

        This constructor currently does not take any parameters but can be
        extended to accept custom configurations in the future.
        """
        pass


    def create_session(self, app_name: str = "SparkApplication") -> SparkSession:
        """
        Creates, configures, and returns a SparkSession.

        This method encapsulates several setup steps:
        1. Manually sets the SPARK_HOME environment variable to avoid issues with pathing.
        2. Configures the Python executable path for Spark workers.
        3. Adds necessary Java options for compatibility with newer JDK versions.

        Parameters:
            app_name (str): The name to assign to the Spark application.
                            Defaults to "SparkApplication".

        Returns:
            SparkSession: A configured SparkSession object ready for use.
        """
        print(f"\n--- Initializing Spark Session for App: '{app_name}' ---")
        
        # 1. Manually set SPARK_HOME to help Spark find itself, which often fails
        #    if the path contains spaces. This path points to the pyspark library
        #    location within your virtual environment.
        pyspark_path = os.path.join(os.path.dirname(sys.executable), '..', 'Lib', 'site-packages', 'pyspark')
        if os.path.isdir(pyspark_path):
            os.environ['SPARK_HOME'] = pyspark_path
            print(f" -> SPARK_HOME manually set to: {pyspark_path}")
        else:
            print(f" -> Warning: Could not find pyspark directory at expected path: {pyspark_path}")

        # 2. Set the Python path directly in the Spark configuration.
        #    This tells Spark which Python executable to use for its workers.
        python_executable_path = sys.executable
        print(f" -> Setting Python path for Spark to: {python_executable_path}")
        
        # 3. Add extraJavaOptions for Java compatibility.
        extra_java_options = (
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--enable-native-access=ALL-UNNAMED"
        )

        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.extraJavaOptions", extra_java_options) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.pyspark.python", python_executable_path) \
            .config("spark.pyspark.driver.python", python_executable_path) \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate()

# if __name__ == '__main__':
#     """
#     Main execution block to test the SparkManager class.
#     """
#     print("--- Starting SparkManager Class Test ---")

#     # 1. Create an instance of the manager
#     spark_manager = SparkManager()

#     # 2. Create a SparkSession using the instance method
#     spark = spark_manager.create_session(app_name="SparkManagerTest")
    
#     print(f"\n -> SparkSession '{spark.sparkContext.appName}' created successfully with Spark version {spark.version}")
    
#     # 3. Run a simple test operation
#     try:
#         test_df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
#         print("\n -> Test DataFrame created successfully:")
#         test_df.show()
#     except Exception as e:
#         print(f"\n -> An error occurred during the test operation: {e}")
#     finally:
#         # 4. Stop the SparkSession
#         spark.stop()
#         print("\n -> SparkSession stopped.")
    
#     print("\n--- SparkManager Class Test Completed ---")

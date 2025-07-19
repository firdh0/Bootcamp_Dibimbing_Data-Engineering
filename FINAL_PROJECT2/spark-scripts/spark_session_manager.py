from pyspark.sql import SparkSession

class SparkSessionManager:
    """
    Manages the lifecycle of a SparkSession configured for Google Cloud Storage and BigQuery integration.

    This class provides a simplified interface to create, retrieve, and stop a SparkSession 
    with preconfigured settings for GCP, including:
    - Google Cloud Storage (GCS) access via Hadoop connector.
    - Service Account authentication for GCP resources.
    - BigQuery integration support.
    - Legacy time parser policy to handle date parsing edge cases.

    Attributes:
        app_name (str): The name of the Spark application.
        spark (SparkSession): The initialized SparkSession object.

    Methods:
        _create_session() -> SparkSession:
            Creates and configures a SparkSession with GCS and BigQuery integration.
            Returns the initialized SparkSession.

        get_session() -> SparkSession:
            Returns the current SparkSession instance.

        stop_session() -> None:
            Stops the running SparkSession and releases resources.
    """


    def __init__(self, app_name: str) -> None:
        """
        Initializes the SparkSessionManager with the specified application name.

        Parameters:
            app_name (str): The name to assign to the Spark application.
        """
        self.app_name = app_name
        self.spark = self._create_session()


    def _create_session(self) -> SparkSession:
        """
        Creates and configures a SparkSession with GCS and BigQuery integration.

        Configuration includes:
        - Enabling Hadoop GCS connector.
        - Setting service account authentication with JSON keyfile.
        - Configuring project ID for GCP services.
        - Setting legacy time parser policy for compatibility.

        Returns:
            SparkSession: The initialized SparkSession instance.
        """
        print(f"Create a Spark session for the application: {self.app_name}")
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/secret/gcp_credentials.json") \
            .config("spark.hadoop.google.cloud.project.id", "gofood-465817") \
            .config("spark.hadoop.bq.project", "gofood-465817") \
            .getOrCreate()
        return spark


    def get_session(self) -> SparkSession:
        """
        Returns the active SparkSession.

        Returns:
            SparkSession: The current SparkSession instance.
        """
        return self.spark


    def stop_session(self):
        """
        Stops the SparkSession and releases all related resources.

        Logs:
            Prints a message indicating the session has been stopped.
        """
        if self.spark:
            print("⏹️ Stop the Spark session.")
            self.spark.stop()


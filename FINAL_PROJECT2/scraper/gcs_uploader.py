from google.cloud import storage
from io import StringIO
import pandas as pd

class GCSUploader:
    """
    Class for uploading Pandas DataFrames to Google Cloud Storage (GCS) as CSV files.

    This class provides an abstraction to simplify the process of saving 
    DataFrame objects directly to GCS buckets in CSV format. It handles 
    GCS client initialization, bucket access, and file uploading.

    Attributes:
        client (storage.Client): The Google Cloud Storage client instance.
        bucket_name (str): The name of the GCS bucket.
        bucket (storage.Bucket): The GCS bucket object.

    Methods:
        upload_df(df: pd.DataFrame, destination_blob_name: str) -> None:
            Uploads the given DataFrame to the specified location in the GCS bucket 
            as a CSV file.

    Raises:
        Exception: If the GCS bucket is not found or upload fails.
    """


    def __init__(self, bucket_name: str) -> None:
        """
        Initializes the GCSUploader with the specified bucket name.

        Parameters:
            bucket_name (str): The name of the GCS bucket where files will be uploaded.

        Raises:
            Exception: If the specified GCS bucket cannot be found or accessed.
        """
        self.client = storage.Client()
        self.bucket_name = bucket_name
        try:
            self.bucket = self.client.get_bucket(bucket_name)
        except Exception as e:
            print(f"❌ Failed to access GCS bucket '{bucket_name}'. Error: {e}")
            raise


    def upload_df(self, df: pd.DataFrame, destination_blob_name: str) -> None:
        """
        Uploads a Pandas DataFrame to Google Cloud Storage as a CSV file.

        This method converts the DataFrame to CSV format in memory 
        (without saving to local disk) and uploads it directly to the specified 
        GCS bucket and path.

        Parameters:
            df (pd.DataFrame): The DataFrame to be uploaded.
            destination_blob_name (str): The target file path in the GCS bucket 
                                         (e.g., 'folder/file.csv').

        Logs:
            Prints a success message if upload succeeds, or an error message if it fails.

        Raises:
            Exception: If the upload process fails.
        """
        try:
            blob = self.bucket.blob(destination_blob_name)
            
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            blob.upload_from_string(csv_buffer.getvalue(), 'text/csv')
            print(f" Successfully uploaded to gs://{self.bucket_name}/{destination_blob_name}")
        except Exception as e:
            print(f"❌ Failed to upload {destination_blob_name} to GCS: {e}")
            raise

from google.cloud import datastore
from datetime import datetime

class DatastoreManager:
    """
    Class for managing the storage and retrieval of the last review date 
    for each restaurant in Google Cloud Datastore.

    This class is used to keep track of the most recent review date 
    scraped from a restaurant's page, allowing the scraper to skip 
    previously collected reviews and only collect new data in the next run.

    Attributes:
        client (datastore.Client): The Google Cloud Datastore client instance.
        kind_name (str): The Datastore kind name where restaurant review dates are stored.

    Methods:
        get_last_scraped_timestamp(restaurant_name: str) -> datetime:
            Retrieves the last review date recorded for the given restaurant. 
            If not found, returns datetime.min.

        update_last_scraped_timestamp(restaurant_name: str, timestamp: datetime) -> None:
            Updates or inserts the last review date for the given restaurant in Datastore.

    Raises:
        Exception: If there is an issue interacting with Google Cloud Datastore.
    """


    def __init__(self, project_id: str, kind_name: str = 'RestaurantReviewBoundary') -> None:
        """
        Initializes the DatastoreManager with the specified project ID and kind name.

        Parameters:
            project_id (str): The Google Cloud project ID.
            kind_name (str): The Datastore kind name to be used. Defaults to 'RestaurantReviewBoundary'.
        """
        self.client = datastore.Client(project=project_id)
        self.kind_name = kind_name
        print(f"✅ DatastoreManager initialized for project '{project_id}', Kind '{kind_name}'.")


    def get_last_scraped_timestamp(self, restaurant_name: str) -> datetime:
        """
        Retrieves the last review date for a given restaurant from Datastore.

        This method is used to determine the most recent review date already 
        collected from the restaurant's page, so the scraper can continue from there.

        Parameters:
            restaurant_name (str): The unique identifier (key name) of the restaurant entity.

        Returns:
            datetime: The date of the last review that was recorded.
                      If no record is found, returns datetime.min to indicate no previous data.

        Logs:
            Prints status messages indicating whether data was found or defaults are used.

        Raises:
            Exception: If an error occurs while fetching data from Datastore.
        """
        key = self.client.key(self.kind_name, restaurant_name)
        try:
            entity = self.client.get(key)
            if entity and 'last_scraped_timestamp' in entity:
                print(f"  🔍 A deadline has been set for'{restaurant_name}': {entity['last_scraped_timestamp']}")
                timestamp_from_datastore = entity['last_scraped_timestamp']
                if timestamp_from_datastore.tzinfo is not None:
                    return timestamp_from_datastore.replace(tzinfo=None)
                return timestamp_from_datastore 
            else:
                print(f"  ℹ️ No time limit found for ‘{restaurant_name}’. Using datetime.min.")
                return datetime.min
        except Exception as e:
            print(f"  ❌ Error retrieving timestamp for ‘{restaurant_name}’ from Datastore: {e}")
            return datetime.min


    def update_last_scraped_timestamp(self, restaurant_name: str, timestamp: datetime) -> None:
        """
        Updates or inserts the last review date for a given restaurant in Datastore.

        This method stores the date of the most recent review found during the 
        latest scraping process, enabling incremental scraping in the future.

        Parameters:
            restaurant_name (str): The unique identifier (key name) of the restaurant entity.
            timestamp (datetime): The last review date to be stored.

        Logs:
            Prints status messages indicating the success or failure of the update operation.

        Raises:
            Exception: If an error occurs while updating data in Datastore.
        """
        key = self.client.key(self.kind_name, restaurant_name)
        entity = datastore.Entity(key=key)
        entity['last_scraped_timestamp'] = timestamp
        try:
            self.client.put(entity)
            print(f"  ✅ The deadline for ‘{restaurant_name}’ was updated in Datastore to: {timestamp}")
        except Exception as e:
            print(f"  ❌ Gagal memperbarui batas waktu untuk '{restaurant_name}' di Datastore: {e}")

import os
from pathlib import Path
from dotenv import load_dotenv

def get_jdbc_config() -> tuple:
    """
    Loads PostgreSQL database configuration from the .env file and returns 
    the JDBC URL and JDBC properties.

    This function reads environment variables defined in the .env file 
    that contain database credentials and connection details, and then 
    constructs the JDBC URL and properties required for connecting 
    to the database.

    Returns:
        tuple:
            - jdbc_url (str): The JDBC URL for the database connection.
            - jdbc_properties (dict): The JDBC properties including user, password, and driver.

    Raises:
        EnvironmentError: If one or more required environment variables are missing.
    """

    print("Loading database configuration from .env file...")
    dotenv_path = Path("/opt/app/.env") 
    load_dotenv(dotenv_path=dotenv_path)

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_container_name = os.getenv("POSTGRES_CONTAINER_NAME")
    postgres_dw_db = os.getenv("POSTGRES_DW_DB")
    
    jdbc_url = f"jdbc:postgresql://{postgres_container_name}/{postgres_dw_db}"
    jdbc_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }
    print("Database configuration loaded successfully.")
    return jdbc_url, jdbc_properties
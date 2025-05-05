# etl_project/scripts/utils/logger.py
import logging
from logging.handlers import RotatingFileHandler
import os

class Logger:
    """
    A logging utility class for managing and configuring loggers for the ETL pipeline.

    This class sets up a logger that writes logs to both a rotating file and the console.
    Logs are saved in a 'logs' directory located one level above the current file.
    It uses Python's built-in `logging` module with `RotatingFileHandler` to handle
    log rotation, ensuring that log files do not grow indefinitely.

    Methods:
        __init__():
            Initializes the Logger instance with a specified name and log level.

        _setup_logging(): 
            Configures the logger with file and console handlers.
            
        get_logger() -> logging.Logger:
            Returns the configured logger instance for use in other parts of the application.
    """

    def __init__(self, name: str = __name__, log_level: int = logging.DEBUG) -> None:
        """
        Initialize the Logger instance.

        Parameters:
            name (str): The name of the logger. Defaults to the module's name.
            log_level (int): Logging level to use (e.g., logging.DEBUG). Defaults to DEBUG.

        Returns:
            None
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self.log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        self._setup_logging()

    def _setup_logging(self) -> None:
        """
        Internal method to set up file and console handlers with formatting and rotation.

        - Creates the logs directory if it doesn't exist.
        - Sets up a rotating file handler (5 MB max, up to 5 backups).
        - Adds a console stream handler.
        - Applies consistent formatting to both handlers.

        Returns:
            None
        """
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        file_handler = RotatingFileHandler(
            filename=f'{self.log_dir}/etl_pipeline.log',
            maxBytes=1024 * 1024 * 5,  # 5 MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_logger(self) -> logging.Logger:
        """
        Get the configured logger instance.

        Returns:
            logging.Logger: The logger configured with file and console handlers.
        """
        return self.logger

# Contoh penggunaan
# if __name__ == "__main__":
#     logger = Logger(__name__).get_logger()
#     logger.debug("Ini adalah pesan debug.")
#     logger.info("Ini adalah pesan info.")
#     logger.warning("Ini adalah pesan peringatan.")
#     logger.error("Ini adalah pesan kesalahan.")
#     logger.critical("Ini adalah pesan kritikal.")
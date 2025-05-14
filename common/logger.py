import logging
import os
from datetime import datetime

class ETLLogger:
    def __init__(self, name):
        """
        Initializes a ETLLogger object.

        Args:
            name (str): The given name.

        Attributes:
            logger: A logging logger object.
        """

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self._set_file_handler()
        
    def _set_file_handler(self):
        """
        Sets up the file handler, which includes the following steps:
            1) Creates the logs directory, if it does not already exist.
            2) Links to a log file.
            3) Links to a formatter.
        """

        os.makedirs("logs", exist_ok=True)
        log_filename = f"logs/{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(self._get_formatter())
        self.logger.addHandler(file_handler)

    def _get_formatter(self):
        """
        Defines the formatter, which involves the following format:
            (asctime | levelname | name| message)
        """

        return logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
        )
    
    def get_logger(self) -> logging.Logger:
        """
        Returns the logger.

        Returns:
            logger: A logging logger object.
        """

        return self.logger

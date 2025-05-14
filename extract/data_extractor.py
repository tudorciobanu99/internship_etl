import pandas as pd
from common.database_connector import DatabaseConnector

class DataExtractor(DatabaseConnector):
    def fetch_api_information(self):
        """
        Fetches the API details from the extract.api_info table.

        Returns:
            api_info (DataFrame): DataFrame with corresponding table column
                names for easier ulterior handling.
        """

        query = """
            SELECT * FROM extract.api_info;
        """
        api_info = self.fetch_rows(query)
        columns = [desc[0] for desc in self.cursor.description]
        api_info = pd.DataFrame(api_info, columns = columns)
        return api_info

    def insert_initial_api_import_log(self, values:tuple):
        """
        Inserts the initial incomplete log in the extract.api_import_log table.
        Sets the start_time to the current time by default.

        Args:
            values (tuple): A 2-element tuple containing:
                country_id (int): The country ID.
                api_id (int): The API ID.

        Returns:
            log_id (int): The ID of the incomplete log record.
        """

        query = """
            INSERT INTO extract.api_import_log (country_id, api_id, start_time)
            VALUES (%s, %s, NOW())
            RETURNING id;
        """
        log_id = self.execute_query_and_return_id(query, values)
        self.logger.info(f"Incomplete API import log record with ID: {log_id} has been written.")
        return log_id

    def update_api_import_log(self, values:tuple):
        """
        Attempts to complete an initial incomplete log in the extract.api_import_log table.
        If successful, it updates the row with additional information.

        Args:
            values (tuple): A 6-element tuple containing:
                start_time (str): Timestamp corresponding to
                    the time the API request was sent.
                end_time (str): Timestamp corresponding to
                    the time the API response was processed.
                code_responde (str): The code response for the
                    given request.
                error_message (str): The error message if applicable.
                log_id (int): The ID of the initial incomplete log record.
        """

        log_id = values[-1]
        if log_id:
            update_query = """
                UPDATE extract.api_import_log
                SET start_time = %s, end_time = %s,
                code_response = %s, error_message = %s
                WHERE id = %s;
            """
            self.execute_query(update_query, values)
            self.logger.info(f"Incomplete API import log record with ID: {log_id} has been completed.")

    def find_created_date(self, import_dir_name, import_file_name):
        """
        Attempts to find the creation date of a record in the extract.import_log table.

        Args:
            import_dir_name (str): The directory name of the imported file.
            import_file_name (str): The name of the imported file.

        Returns:
            file_created_date (str): The creation date of the record, if available.
        """

        query = """
            SELECT file_created_date FROM extract.import_log
            WHERE import_directory_name = %s
            AND import_file_name = %s
            AND file_created_date IS NOT NULL
            ORDER BY file_created_date ASC
            LIMIT 1;
        """
        values = (import_dir_name, import_file_name)
        existing_record = self.fetch_rows(query, values)

        if existing_record:
            file_created_date = existing_record[0][0]
            return file_created_date

    def insert_initial_import_log(self, values:tuple):
        """
        Inserts the initial incomplete log in the extract.import_log table.

        Args:
            values (tuple): A 4-element tuple containing:
                batch_date (str): The batch date.
                country_id (int): The country ID.
                import_dir_name (str): The directory name of the imported file.
                import_file_name (str): The name of the imported file.
        """

        query = """
            INSERT INTO extract.import_log
            (batch_date, country_id, import_directory_name, import_file_name)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """
        log_id = self.execute_query_and_return_id(query, values)
        self.logger.info(f"Incomplete import log record with ID: {log_id} has been written.")
        return log_id

    def update_import_log(self, values:tuple):
        """
        Attempts to complete an initial incomplete log in the extract.import_log table.
        If successful, it updates the row with additional information.

        Args:
            values (tuple): A 6-element tuple containing:
                import_dir_name (str): The directory name of the imported file.
                import_file_name (str): The name of the imported file.
                file_created_date (str): The creation date of the file.
                file_last_modified_date (str): The latest date the file was modified.
                row_count (int): The number of rows contained in the file.
                log_id (int): The ID of the initial incomplete log record.
        """

        log_id = values[-1]
        if log_id:
            existing_created_date = self.find_created_date(values[0], values[1])
            if existing_created_date:
                values = values[:2] + (existing_created_date,) + values[3:]

            update_query = """
                UPDATE extract.import_log
                SET file_created_date = %s, file_last_modified_date = %s, row_count = %s
                WHERE id = %s;
            """
            self.execute_query(update_query, values[2:])
            self.logger.info(f"Incomplete import log record with ID: {log_id} has been completed.")

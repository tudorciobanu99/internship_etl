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

        Args:
            values (tuple): A 3-element tuple containing:
                country_id (int): The country ID.
                api_id (int): The API ID.
                start_time (str): Timestamp corresponding to the time
                    the API request was sent.
        """

        query = """
            INSERT INTO extract.api_import_log (country_id, api_id, start_time)
            VALUES (%s, %s, %s);
        """
        self.execute_query(query, values)

    def find_api_import_log(self, values:tuple):
        """
        Attempts to find an initial incomplete log in the extract.api_import_log table.

        Args:
            values (tuple): A 3-element tuple containing:
                country_id (int): The country ID.
                api_id (int): The API ID.
                start_time (str): Timestamp corresponding to the time
                    the API request was sent.

        Returns:
            log_id (int): The ID of the log, if successful.
        """

        query = """
            SELECT id
            FROM extract.api_import_log
            WHERE country_id = %s
            AND api_id = %s AND start_time = %s;
        """
        row = self.fetch_rows(query, values)
        if row:
            log_id = row[0]
            return log_id

    def update_api_import_log(self, values:tuple):
        """
        Attempts to complete an initial incomplete log in the extract.api_import_log table.
        If successful, it updates the row with additional information.

        Args:
            values (tuple): A 6-element tuple containing:
                country_id (int): The country ID.
                api_id (int): The API ID.
                start_time (str): Timestamp corresponding to
                    the time the API request was sent.
                end_time (str): Timestamp corresponding to
                    the time the API response was processed.
                code_responde (str): The code response for the
                    given request.
                error_message (str): The error message if applicable.
        """

        log_values = values[:3]
        log_id = self.find_api_import_log(log_values)

        if log_id:
            update_query = """
                UPDATE extract.api_import_log
                SET end_time = %s, code_response = %s, error_message = %s
                WHERE id = %s;
            """
            update_values = values[3:6]
            values = update_values + (log_id,)
            self.execute_query(update_query, values)

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
            VALUES (%s, %s, %s, %s);
        """
        self.execute_query(query, values)

    def find_import_log(self, values:tuple):
        """
        Attempts to find an initial incomplete log in the extract.import_log table.

        Args:
            values (tuple): A 4-element tuple containing:
                batch_date (str): The batch date.
                country_id (int): The country ID.
                import_dir_name (str): The directory name of the imported file.
                import_file_name (str): The name of the imported file.

        Returns:
            log_id (int): The ID of the log, if successful.
        """
        query = """
            SELECT id
            FROM extract.import_log
            WHERE batch_date = %s AND country_id = %s
            AND import_directory_name = %s
            AND import_file_name = %s
            AND file_last_modified_date IS NULL;
        """
        row = self.fetch_rows(query, values)
        if row:
            log_id = row[0]
            return log_id

    def update_import_log(self, values:tuple):
        """
        Attempts to complete an initial incomplete log in the extract.import_log table.
        If successful, it updates the row with additional information.

        Args:
            values (tuple): A 7-element tuple containing:
                batch_date (str): The batch date.
                country_id (int): The country ID.
                import_dir_name (str): The directory name of the imported file.
                import_file_name (str): The name of the imported file.
                file_created_date (str): The creation date of the file.
                file_last_modified_date (str): The latest date the file was modified.
                row_count (int): The number of rows contained in the file.
        """

        log_values = values[:4]
        log_id = self.find_import_log(log_values)
        if log_id:
            existing_created_date = self.find_created_date(values[2], values[3])
            if existing_created_date:
                values[4] = existing_created_date

            update_query = """
                UPDATE extract.import_log
                SET file_created_date = %s, file_last_modified_date = %s, row_count = %s
                WHERE id = %s;
            """
            update_values = values[4:] + (log_id,)
            self.execute_query(update_query, update_values)

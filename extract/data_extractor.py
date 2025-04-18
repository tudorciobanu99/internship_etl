import pandas as pd
from common.database_connector import DatabaseConnector

class DataExtractor(DatabaseConnector):
    def fetch_api_information(self):
        query = """
            SELECT * FROM extract.api_info;
        """
        api_info = self.fetch_rows(query)
        columns = [desc[0] for desc in self.cursor.description]
        api_info = pd.DataFrame(api_info, columns = columns)
        return api_info

    def insert_initial_api_import_log(self, country_id, api_id, start_time):
        query = """
            INSERT INTO extract.api_import_log (country_id, api_id, start_time)
            VALUES (%s, %s, %s);
        """
        values = (country_id, api_id, start_time)
        self.execute_query(query, values)

    def find_api_import_log(self, country_id, api_id, start_time):
        query = """
            SELECT id
            FROM extract.api_import_log
            WHERE country_id = %s
            AND api_id = %s AND start_time = %s;
        """
        values = (country_id, api_id, start_time)

        row = self.fetch_rows(query, values)
        if row:
            return row[0]

    def update_api_import_log(self, **params):
        country_id, api_id, start_time, end_time, code_response, error_message = (
            params.get("country_id"),
            params.get("api_id"),
            params.get("start_time"),
            params.get("end_time"),
            params.get("code_response"),
            params.get("error_message")
        )
        log_id = self.find_api_import_log(country_id, api_id, start_time)

        if log_id:
            update_query = """
                UPDATE extract.api_import_log
                SET end_time = %s, code_response = %s, error_message = %s
                WHERE id = %s;
            """
            values = (end_time, code_response, error_message, log_id)
            self.execute_query(update_query, values)

    def find_created_date(self, import_dir_name, import_file_name):
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

    def insert_initial_import_log(self, **params):
        batch_date, country_id, import_dir_name, import_file_name = (
            params.get("batch_date"),
            params.get("country_id"),
            params.get("import_dir_name"),
            params.get("import_file_name")
        )

        query = """
            INSERT INTO extract.import_log
            (batch_date, country_id, import_directory_name, import_file_name)
            VALUES (%s, %s, %s, %s);
        """
        values = (batch_date, country_id, import_dir_name, import_file_name)
        self.execute_query(query, values)

    def find_import_log(self, batch_date, country_id, import_dir_name, import_file_name):
        query = """
            SELECT id
            FROM extract.import_log
            WHERE batch_date = %s AND country_id = %s
            AND import_directory_name = %s
            AND import_file_name = %s
            AND file_last_modified_date IS NULL;
        """
        values = (batch_date, country_id, import_dir_name, import_file_name)
        row = self.fetch_rows(query, values)
        if row:
            return row[0]

    def update_import_log(self, **params):
        (batch_date, country_id, import_dir_name, import_file_name,
         file_created_date, file_last_modified_date, row_count) = (
            params.get("batch_date"),
            params.get("country_id"),
            params.get("import_dir_name"),
            params.get("import_file_name"),
            params.get("file_created_date"),
            params.get("file_last_modified_date"),
            params.get("row_count")
        )
        log_id = self.find_import_log(batch_date, country_id, import_dir_name, import_file_name)

        if log_id:
            existing_created_date = self.find_created_date(import_dir_name, import_file_name)
            if existing_created_date:
                file_created_date = existing_created_date

            update_query = """
                UPDATE extract.import_log
                SET file_created_date = %s, file_last_modified_date = %s, row_count = %s
                WHERE id = %s;
            """
            values = (file_created_date, file_last_modified_date, row_count, log_id)
            self.execute_query(update_query, values)

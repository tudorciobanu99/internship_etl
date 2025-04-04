import psycopg2 as pg, pandas as pd
class databaseconnection:
    def __init__(self, dbname, user, host, password, port):
        self.connection = pg.connect(
            database=dbname,
            user=user,
            host=host,
            password=password,
            port=port,
        )
        self.cursor = self.connection.cursor()
        print("Connection successful!")

    def fetch_rows(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows
    
    def fetch_api_information(self):
        api_info = self.fetch_rows("SELECT * FROM extract.api_info")
        columns = [desc[0] for desc in self.cursor.description]
        api_info = pd.DataFrame(api_info, columns = columns)
        return api_info
    
    def fetch_countries(self):
        countries = self.fetch_rows("SELECT * FROM extract.country")
        columns = [desc[0] for desc in self.cursor.description]
        countries = pd.DataFrame(countries, columns = columns)
        return countries
    
    def insert_initial_api_import_log(self, country_id, api_id, start_time):
        query = f"""
            INSERT INTO extract.api_import_log (country_id, api_id, start_time)
            VALUES (%s, %s, %s)
            """
        self.cursor.execute(query, (country_id, api_id, start_time))
        self.connection.commit()
        print('API import log has been added!')

    def update_api_import_log(self, country_id, api_id, start_time, end_time, code_response, error_message):
        find_id_query = f"""
            SELECT id
            FROM extract.api_import_log
            WHERE country_id = {country_id} AND api_id = {api_id} AND start_time = '{start_time}';
            """
        
        try:
            row = self.fetch_rows(find_id_query)
            if row:
                id = row[0]
                
                update_query = f"""
                    UPDATE extract.api_import_log
                    SET end_time = %s, code_response = %s, error_message = %s
                    WHERE id = %s;
                    """
                self.cursor.execute(update_query, (end_time, code_response, error_message, id))
                self.connection.commit()

                print('API import log has been updated!')
            else:
                print('No matching row found.')
        except Exception as e:
            print(f"An error occurred: {e}.")

    def insert_initial_import_log(self, batch_date, country_id, import_directory_name, import_file_name):
        existing_record = self.fetch_rows(f"""
                    SELECT file_created_date FROM extract.import_log
                    WHERE import_directory_name = '{import_directory_name}' AND import_file_name = '{import_file_name}'
                    LIMIT 1
                """
                )

        # Determine the file_created_date
        file_created_date = None
        if existing_record:
            file_created_date = existing_record[0][0]

        query = f"""
            INSERT INTO extract.import_log (batch_date, country_id, import_directory_name, import_file_name, file_created_date)
            VALUES (%s, %s, %s, %s, %s)
            """
        self.cursor.execute(query, (batch_date, country_id, import_directory_name, import_file_name, file_created_date))
        self.connection.commit()
        print('Import log has been added!') 

    def update_import_log(self, batch_date, country_id, import_directory_name, import_file_name, file_created_date, file_last_modified_date, row_count):
        find_id_query = f"""
            SELECT id
            FROM extract.import_log
            WHERE batch_date = '{batch_date}' AND country_id = {country_id} AND import_directory_name = '{import_directory_name}' AND import_file_name = '{import_file_name}'
            AND file_last_modified_date IS NULL;
            """
        
        try:
            row = self.fetch_rows(find_id_query)
            if row:
                id = row[0]
                
                existing_record = self.fetch_rows(f"""
                    SELECT file_created_date FROM extract.import_log
                    WHERE import_directory_name = '{import_directory_name}' AND import_file_name = '{import_file_name}' AND file_created_date IS NOT NULL
                    LIMIT 1
                """
                )

                # Determine the file_created_date
                if existing_record:
                    file_created_date = existing_record[0][0]
                
                update_query = f"""
                    UPDATE extract.import_log
                    SET file_created_date = %s, file_last_modified_date = %s, row_count = %s
                    WHERE id = %s;
                    """
                
                self.cursor.execute(update_query, (file_created_date, file_last_modified_date, row_count, id))
                self.connection.commit()

                print('Import log has been updated!')
            else:
                print('No matching row found.')
        except Exception as e:
            print(f"An error occurred: {e}.")


    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print("Connection terminated!")

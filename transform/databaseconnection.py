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
    
    def fetch_countries(self):
        countries = self.fetch_rows("SELECT * FROM extract.country")
        columns = [desc[0] for desc in self.cursor.description]
        countries = pd.DataFrame(countries, columns = columns)
        return countries
    
    def truncate_table(self, table_name):
        query = f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
        self.cursor.execute(query)
        self.connection.commit()
        print(f"{table_name} table has been truncated!")
    
    def insert_initial_transform_log(self, batch_date, country_id, initial_status):
        query = f"""
            INSERT INTO transform.transform_log (batch_date, country_id, status)
            VALUES (%s, %s, %s)
            """
        self.cursor.execute(query, (batch_date, int(country_id), initial_status))
        self.connection.commit()
        print('Tranform log record has been added!')
    
    def update_transform_log(self, batch_date, country_id, processed_directory_name, processed_file_name, row_count, status):
        find_id_query = f"""
            SELECT id
            FROM transform.transform_log
            WHERE batch_date = '{batch_date}' AND country_id = {int(country_id)}
            AND status = 'processing';
            """

        try:
            row = self.fetch_rows(find_id_query)
            if row:
                id = row[0][0]

                update_query = f"""
                    UPDATE transform.transform_log
                    SET processed_directory_name = %s, processed_file_name = %s, row_count = %s, status = %s
                    WHERE id = %s;
                    """

                self.cursor.execute(update_query, (processed_directory_name, processed_file_name, row_count, status, id))
                self.connection.commit()

                print('Transform log record has been updated!')
            else:
                print('No matching row found.')
        except Exception as e:
            print(f"An error occurred: {e}.")

    def insert_weather_data(self, country_id, date, weather_code, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed):
        query = f"""
            INSERT INTO transform.weather_data_import (country_id, date, weather_code, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (int(country_id), date, weather_code, float(mean_temperature), float(mean_surface_pressure), float(precipitation_sum), float(relative_humidity), float(wind_speed)))
        self.connection.commit()
        print('Weather data record has been added!')

    def insert_covid_data(self, country_id, date, confirmed_cases, deaths, recovered):
        query = f"""
            INSERT INTO transform.covid_data_import (country_id, date, confirmed_cases, deaths, recovered)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (int(country_id), date, int(confirmed_cases), int(deaths), int(recovered)))
        self.connection.commit()
        print('Covid data record has been added!')

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print("Connection terminated!")
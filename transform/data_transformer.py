import psycopg2 as pg
from database_connector import DatabaseConnector

class DataTransformer(DatabaseConnector):
    def insert_initial_transform_log(self, batch_date, country_id, initial_status):
        query = """
            INSERT INTO transform.transform_log (batch_date, country_id, status)
            VALUES (%s, %s, %s)
            """
        self.cursor.execute(query, (batch_date, int(country_id), initial_status))
        self.connection.commit()

    def find_transform_log(self, batch_date, country_id):
        query = f"""
            SELECT id
            FROM transform.transform_log
            WHERE batch_date = '{batch_date}' AND country_id = {int(country_id)}
            AND status = 'processing';
            """
        try:
            row = self.fetch_rows(query)
            if row:
                return row[0][0]
            return None
        except pg.Error:
            return None

    def update_transform_log(self, **params):
        batch_date = params.get('batch_date')
        country_id = params.get('country_id')
        p_dir_name = params.get('p_dir_name')
        p_file_name = params.get('p_file_name')
        row_count = params.get('row_count')
        status = params.get('status')

        log_id = self.find_transform_log(batch_date, country_id)
        if log_id:
            update_query = """
                UPDATE transform.transform_log
                SET processed_directory_name = %s, processed_file_name = %s, row_count = %s, status = %s
                WHERE id = %s;
                """
            values = (p_dir_name, p_file_name, row_count, status, log_id)
            self.cursor.execute(update_query, values)
            self.connection.commit()

    def fetch_country_details(self, country_id):
        country_query = """
            SELECT code, latitude, longitude
            FROM extract.country
            WHERE id = %s
            """
        self.cursor.execute(country_query, (int(country_id),))
        country_details = self.cursor.fetchone()
        return country_details

    def insert_weather_data(self, **params):
        country_id = params.get('country_id')
        date = params.get('date')
        weather_code = params.get('weather_code')
        weather_description = params.get('weather_description')
        mean_temperature = params.get('mean_temperature')
        mean_surface_pressure = params.get('mean_surface_pressure')
        precipitation_sum = params.get('precipitation_sum')
        relative_humidity = params.get('relative_humidity')
        wind_speed = params.get('wind_speed')
        country_code, latitude, longitude = self.fetch_country_details(country_id)

        query = """
            INSERT INTO transform.weather_data_import (
                country_id, country_code, latitude, longitude, date, weather_code, weather_description,
                mean_temperature, mean_surface_pressure, precipitation_sum,
                relative_humidity, wind_speed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        values = (
            int(country_id), country_code, float(latitude), float(longitude), date,
            str(weather_code), str(weather_description), float(mean_temperature),
            float(mean_surface_pressure), float(precipitation_sum),
            float(relative_humidity), float(wind_speed)
        )
        self.cursor.execute(query, values)
        self.connection.commit()

    def insert_covid_data(self, **params):
        country_id = params.get('country_id')
        date = params.get('date')
        confirmed_cases = params.get('confirmed_cases')
        deaths = params.get('deaths')
        recovered = params.get('recovered')
        country_code, latitude, longitude = self.fetch_country_details(country_id)

        query = """
            INSERT INTO transform.covid_data_import (
                country_id, country_code, latitude, longitude, date,
                confirmed_cases, deaths, recovered
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
        values = (
            int(country_id), country_code, float(latitude), float(longitude),
            date, int(confirmed_cases), int(deaths), int(recovered)
        )
        self.cursor.execute(query, values)
        self.connection.commit()

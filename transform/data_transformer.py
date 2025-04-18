from common.database_connector import DatabaseConnector
class DataTransformer(DatabaseConnector):
    def insert_initial_transform_log(self, batch_date, country_id, initial_status):
        query = """
            INSERT INTO transform.transform_log (batch_date, country_id, status)
            VALUES (%s, %s, %s);
        """
        values = (batch_date, country_id, initial_status)
        self.execute_query(query, values)

    def find_transform_log(self, batch_date, country_id):
        query = """
            SELECT id
            FROM transform.transform_log
            WHERE batch_date = %s AND country_id = %s
            AND status = %s;
        """
        values = (batch_date, country_id, "processing")
        row = self.fetch_rows(query, values)
        if row:
            return row[0][0]

    def update_transform_log(self, **params):
        batch_date, country_id, p_dir_name, p_file_name, row_count, status = (
            params.get("batch_date"),
            params.get("country_id"),
            params.get("p_dir_name"),
            params.get("p_file_name"),
            params.get("row_count"),
            params.get("status")
        )
        log_id = self.find_transform_log(batch_date, country_id)

        if log_id:
            update_query = """
                UPDATE transform.transform_log
                SET processed_directory_name = %s, processed_file_name = %s, row_count = %s, status = %s
                WHERE id = %s;
            """
            values = (p_dir_name, p_file_name, row_count, status, log_id)
            self.execute_query(update_query, values)

    def insert_weather_data(self, **params):
        (country_id, date, weather_code, weather_description, mean_temperature,
         mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed) = (
            params.get("country_id"),
            params.get("date"),
            params.get("weather_code"),
            params.get("weather_description"),
            params.get("mean_temperature"),
            params.get("mean_surface_pressure"),
            params.get("precipitation_sum"),
            params.get("relative_humidity"),
            params.get("wind_speed")
        )
        country_code, latitude, longitude = self.fetch_country_details(country_id)
        if all([country_code, latitude, longitude]):
            query = """
            INSERT INTO transform.weather_data_import (
                country_id, country_code, latitude, longitude, date, weather_code, weather_description,
                mean_temperature, mean_surface_pressure, precipitation_sum,
                relative_humidity, wind_speed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                int(country_id), country_code, float(latitude), float(longitude), date,
                weather_code, weather_description, mean_temperature,
                mean_surface_pressure, precipitation_sum,
                relative_humidity, wind_speed
            )
            self.execute_query(query, values)

    def insert_covid_data(self, **params):
        country_id, date, confirmed_cases, deaths, recovered = (
            params.get("country_id"),
        params.get("date"),
        params.get("confirmed_cases"),
        params.get("deaths"),
        params.get("recovered")
        )
        country_code, latitude, longitude = self.fetch_country_details(country_id)

        if all([country_code, latitude, longitude]):
            query = """
                INSERT INTO transform.covid_data_import (
                    country_id, country_code, latitude, longitude, date,
                    confirmed_cases, deaths, recovered
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                int(country_id), country_code, float(latitude), float(longitude),
                date, confirmed_cases, deaths, recovered
            )
            self.execute_query(query, values)

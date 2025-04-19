from common.database_connector import DatabaseConnector
class DataTransformer(DatabaseConnector):
    def insert_initial_transform_log(self, values:tuple):
        """
        Inserts the initial incomplete log in the extract.import_log table.

        Args:
            values (tuple): A 3-element tuple containing:
                batch_date (str): The batch date.
                country_id (int): The country ID.
                status (int): Initial status of the record.
        """

        query = """
            INSERT INTO transform.transform_log (batch_date, country_id, status)
            VALUES (%s, %s, %s);
        """
        self.execute_query(query, values)

    def find_transform_log(self, values:tuple):
        """
        Attempts to find an initial incomplete log in the extract.import_log table.

        Args:
            values (tuple): A 2-element tuple containing:
                batch_date (str): The batch date.
                country_id (int): The country ID.

        Returns:
            log_id (int): The ID of the log, if successful.
        """

        query = """
            SELECT id
            FROM transform.transform_log
            WHERE batch_date = %s AND country_id = %s
            AND status = %s;
        """
        row = self.fetch_rows(query, values)
        if row:
            log_id = row[0][0]
            return log_id

    def update_transform_log(self, values:tuple):
        """
        Attempts to complete an initial incomplete log in the extract.import_log table.
        If successful, it updates the row with additional information.

        Args:
            values (tuple): A 6-element tuple containing:
                batch_date (str): The batch date.
                country_id (int): The country ID.
                p_dir_name (str): The directory the processed file is moved to.
                p_file_name (str): The name of the processed file.
                row_count (int): The number of rows of the processed file.
                status (str): Generally, either processed or error.
        """

        log_values = values[:2] + ("processing",)
        log_id = self.find_transform_log(log_values)
        if log_id:
            update_query = """
                UPDATE transform.transform_log
                SET processed_directory_name = %s, processed_file_name = %s, row_count = %s, status = %s
                WHERE id = %s;
            """
            update_values = values[2:] + (log_id,)
            self.execute_query(update_query, update_values)

    def insert_weather_data(self, values:tuple):
        """
        Inserts the weather data extracted from a processed file.

        Args:
            values (tuple): A 9-element tuple containing:
                country_id (int): The country ID.
                date (str): The given date for which the weather was extracted.
                weather_code (str): The weather for that date.
                weather_description (str): The description given the weather code.
                mean_temperature (float): The mean temperature.
                mean_surface_pressure (float): The mean surface pressure.
                precipitation_sum (float): The precipitation sum.
                relative_humidity (float): The relative humidity.
                wind_speed (float): The wind speed.
        """

        country_id = values[0]
        country_code, name, latitude, longitude = self.fetch_country_details(country_id)
        if all([country_code, name, latitude, longitude]):
            query = """
            INSERT INTO transform.weather_data_import (
                country_id, country_code, latitude, longitude, date, weather_code, weather_description,
                mean_temperature, mean_surface_pressure, precipitation_sum,
                relative_humidity, wind_speed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            data = (int(country_id), country_code, float(latitude), float(longitude)) + values[1:]
            self.execute_query(query, data)

    def insert_covid_data(self, values:tuple):
        """
        Inserts the COVID-19 data extracted from a processed file.

        Args:
            values (tuple): A 9-element tuple containing:
                country_id (int): The country ID.
                date (str): The given date for which the weather was extracted.
                confirmed_cases (int): The number of confirmed cases.
                deaths (int): The number of deaths.
                recovered (int): The number of recovered patients.
        """

        country_id = values[0]
        country_code, name, latitude, longitude = self.fetch_country_details(country_id)
        if all([country_code, name, latitude, longitude]):
            query = """
                INSERT INTO transform.covid_data_import (
                    country_id, country_code, latitude, longitude, date,
                    confirmed_cases, deaths, recovered
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            data = (int(country_id), country_code, float(latitude), float(longitude)) + values[1:]
            self.execute_query(query, data)

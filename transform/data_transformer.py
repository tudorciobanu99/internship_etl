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

        Returns:
            log_id (int): The ID of the incomplete log record.
        """

        query = """
            INSERT INTO transform.transform_log (batch_date, country_id, status)
            VALUES (%s, %s, %s)
            RETURNING id;
        """
        log_id = self.execute_query_and_return_id(query, values)
        self.logger.info(f"Incomplete transform log record with ID: {log_id} has been written.")
        return log_id

    def update_transform_log(self, values:tuple):
        """
        Attempts to complete an initial incomplete log in the extract.import_log table.
        If successful, it updates the row with additional information.

        Args:
            values (tuple): A 5-element tuple containing:
                p_dir_name (str): The directory the processed file is moved to.
                p_file_name (str): The name of the processed file.
                row_count (int): The number of rows of the processed file.
                status (str): Generally, either processed or error.
                log_id (int): The ID of the incomplete log record.
        """

        log_id = values[-1]
        if log_id:
            update_query = """
                UPDATE transform.transform_log
                SET processed_directory_name = %s, processed_file_name = %s, row_count = %s, status = %s
                WHERE id = %s;
            """
            self.execute_query(update_query, values)
            self.logger.info(f"Incomplete transform log record with ID: {log_id} has been completed.")

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

        query = """
        INSERT INTO transform.weather_data_import (
            country_id, date, weather_code, weather_description,
            mean_temperature, mean_surface_pressure, precipitation_sum,
            relative_humidity, wind_speed
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        self.execute_query(query, values)
        self.logger.info(f"Weather data for country_id: {values[0]} and date: {values[1]} \
                         has been added to the staging area.")

    def insert_covid_data(self, values:tuple):
        """
        Inserts the COVID-19 data extracted from a processed file.

        Args:
            values (tuple): A 5-element tuple containing:
                country_id (int): The country ID.
                date (str): The given date for which the weather was extracted.
                confirmed_cases (int): The number of confirmed cases.
                deaths (int): The number of deaths.
                recovered (int): The number of recovered patients.
        """

        query = """
            INSERT INTO transform.covid_data_import (
                country_id, date, confirmed_cases,
                deaths, recovered
            )
            VALUES (%s, %s, %s, %s, %s);
        """
        self.execute_query(query, values)
        self.logger.info(f"COVID data for country_id: {values[0]} and date: {values[1]} \
                         has been added to the staging area.")

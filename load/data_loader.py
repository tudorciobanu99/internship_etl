from common.database_connector import DatabaseConnector
class DataLoader(DatabaseConnector):
    def merge_dim_country(self):
        """
        Merges (UPSERTs) country details from extract.country into load.dim_country
        based on the countries actually used in the transform tables. A MD5 hash value
        created from the concatenation of all values of a given record is used to
        check whether a record in the load.dim_country table must be updated or not.
        """

        query = """
            MERGE INTO load.dim_country AS target
            USING (
                SELECT DISTINCT
                    ec.id,
                    ec.code,
                    ec.name,
                    ec.latitude,
                    ec.longitude,
                    md5(
                        ec.id || '|' ||
                        ec.code || '|' ||
                        ec.name || '|' ||
                        ec.latitude || '|' ||
                        ec.longitude
                    ) AS hash_value
                FROM extract.country ec
                INNER JOIN (
                    SELECT country_id FROM transform.covid_data_import
                    UNION
                    SELECT country_id FROM transform.weather_data_import
                ) AS used_ids
                ON ec.id = used_ids.country_id
            ) AS source
            ON target.country_id = source.id
            WHEN MATCHED AND target.hash_value != source.hash_value THEN
                UPDATE SET
                    country_code = source.code,
                    country_name = source.name,
                    latitude = source.latitude,
                    longitude = source.longitude,
                    hash_value = source.hash_value
            WHEN NOT MATCHED THEN
                INSERT (country_id, country_code, country_name, latitude, longitude, hash_value)
                VALUES (source.id, source.code, source.name,
                  source.latitude, source.longitude, source.hash_value);
        """
        self.execute_query(query)
        self.logger.info("Merging operation on table dim_country was completed successfully.")

    def merge_dim_date(self):
        """
        Merges (aka. UPSERTs) the date details from the staging tables in the
        transform schema (weather_data_import, covid_data_import) with the load.dim_date
        dimension table. A MD5 hash value created from the concatenation of all values
        of a given record is used to check whether a record in the load.dim_date table
        must be updated or not.
        """

        query = """
            MERGE INTO load.dim_date AS target
            USING (
                SELECT DISTINCT
                    date,
                    TO_CHAR(date, 'YYYYMMDD')::BIGINT AS date_id,
                    EXTRACT(YEAR FROM date) AS year,
                    EXTRACT(MONTH FROM date) AS month,
                    EXTRACT(DAY FROM date) AS day,
                    EXTRACT(DOW FROM date) AS day_of_week,
                    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                    md5(
                        TO_CHAR(date, 'YYYYMMDD') || '|' ||
                        EXTRACT(YEAR FROM date) || '|' ||
                        EXTRACT(MONTH FROM date) || '|' ||
                        EXTRACT(DAY FROM date) || '|' ||
                        EXTRACT(DOW FROM date) || '|' ||
                        CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'TRUE' ELSE 'FALSE' END
                    ) AS hash_value
                FROM (
                    SELECT date FROM transform.covid_data_import
                    UNION
                    SELECT date FROM transform.weather_data_import
                ) AS combined_dates
            ) AS source
            ON target.date_id = source.date_id
            WHEN MATCHED AND target.hash_value != source.hash_value THEN
                UPDATE SET
                    date = source.date,
                    year = source.year,
                    month = source.month,
                    day = source.day,
                    day_of_week = source.day_of_week,
                    is_weekend = source.is_weekend,
                    hash_value = source.hash_value
            WHEN NOT MATCHED THEN
                INSERT (date_id, date, year, month, day, day_of_week, is_weekend, hash_value)
                VALUES (source.date_id, source.date, source.year, source.month, source.day, source.day_of_week,
                source.is_weekend, source.hash_value);
        """
        self.execute_query(query)
        self.logger.info("Merging operation on table dim_date was completed successfully.")

    def merge_dim_weather_description(self):
        """
        Merges (aka. UPSERTs) the weather description details from the staging tables in the
        transform schema (weather_data_import, covid_data_import) with the
        load.dim_weather_description dimension table. A MD5 hash value created from the
        concatenation of all values of a given record is used to check whether a record in
        the load.dim_weather_description table must be updated or not.
        """

        query = """
            MERGE INTO load.dim_weather_code AS target
            USING (
                SELECT DISTINCT
                    weather_code,
                    weather_description,
                    md5(weather_code || '|' || weather_description) AS hash_value
                FROM transform.weather_data_import
            ) AS source
            ON target.weather_code = source.weather_code
            WHEN MATCHED AND target.hash_value != source.hash_value THEN
                UPDATE SET
                    description = source.weather_description,
                    hash_value = source.hash_value
            WHEN NOT MATCHED THEN
                INSERT (weather_code, description, hash_value)
                VALUES (source.weather_code, source.weather_description, source.hash_value);
        """
        self.execute_query(query)
        self.logger.info("Merging operation on table dim_weather_description was completed successfully.")

    def merge_fact_covid(self):
        """
        Merges (aka. UPSERTs) the covid details from the staging tables in the
        transform schema (weather_data_import, covid_data_import) with the load.fact_covid_data
        fact table. A MD5 hash value created from the concatenation of all values
        of a given record is used to check whether a record in the load.fact_covid_data table
        must be updated or not.
        """

        query = """
            MERGE INTO load.fact_covid_data AS target
            USING (
                SELECT
                    c.country_id,
                    d.date_id,
                    t.confirmed_cases,
                    t.deaths,
                    t.recovered,
                    md5(
                        c.country_id || '|' ||
                        d.date_id || '|' ||
                        t.confirmed_cases || '|' ||
                        t.deaths || '|' ||
                        t.recovered
                    ) AS hash_value
                FROM transform.covid_data_import t
                JOIN load.dim_country c ON t.country_id = c.country_id
                JOIN load.dim_date d ON t.date = d.date
            ) AS source
            ON target.country_id = source.country_id AND target.date_id = source.date_id
            WHEN MATCHED AND target.hash_value != source.hash_value THEN
                UPDATE SET
                    confirmed_cases = source.confirmed_cases,
                    deaths = source.deaths,
                    recovered = source.recovered,
                    hash_value = source.hash_value,
                    updatedAt = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (country_id, date_id, confirmed_cases, deaths, recovered, hash_value, createdAt, updatedAt)
                VALUES (source.country_id, source.date_id, source.confirmed_cases, source.deaths,
                source.recovered, source.hash_value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        self.execute_query(query)
        self.logger.info("Merging operation on fact_covid_data was completed successfully.")

    def merge_fact_weather(self):
        """
        Merges (aka. UPSERTs) the weather details from the staging tables in the
        transform schema (weather_data_import, covid_data_import) with the load.fact_weather_data
        fact table. A MD5 hash value created from the concatenation of all values of a given
        record is used to check whether a record in the load.fact_weather_data table must be
        updated or not.
        """

        query = """
            MERGE INTO load.fact_weather_data AS target
            USING (
                SELECT
                    c.country_id,
                    d.date_id,
                    t.weather_code,
                    t.mean_temperature,
                    t.mean_surface_pressure,
                    t.precipitation_sum,
                    t.relative_humidity,
                    t.wind_speed,
                    md5(
                        c.country_id || '|' ||
                        d.date_id || '|' ||
                        t.weather_code || '|' ||
                        t.mean_temperature || '|' ||
                        t.mean_surface_pressure || '|' ||
                        t.precipitation_sum || '|' ||
                        t.relative_humidity || '|' ||
                        t.wind_speed
                    ) AS hash_value
                FROM transform.weather_data_import t
                JOIN load.dim_country c ON t.country_id = c.country_id
                JOIN load.dim_date d ON t.date = d.date
            ) AS source
            ON target.country_id = source.country_id AND target.date_id = source.date_id
            WHEN MATCHED AND target.hash_value != source.hash_value THEN
                UPDATE SET
                    weather_code = source.weather_code,
                    mean_temperature = source.mean_temperature,
                    mean_surface_pressure = source.mean_surface_pressure,
                    precipitation_sum = source.precipitation_sum,
                    relative_humidity = source.relative_humidity,
                    wind_speed = source.wind_speed,
                    hash_value = source.hash_value,
                    updatedAt = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (country_id, date_id, weather_code, mean_temperature, mean_surface_pressure,
                precipitation_sum, relative_humidity, wind_speed, hash_value, createdAt, updatedAt)
                VALUES (source.country_id, source.date_id, source.weather_code, source.mean_temperature,
                source.mean_surface_pressure, source.precipitation_sum, source.relative_humidity, source.wind_speed,
                source.hash_value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        self.execute_query(query)
        self.logger.info("Merging operation on fact_weather_data was completed successfully.")


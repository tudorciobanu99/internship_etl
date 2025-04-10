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
    
    def merge_dim_country(self):
        query = f"""
            MERGE INTO load.dim_country AS target
            USING (
                SELECT DISTINCT country_id, country_code, latitude, longitude FROM transform.covid_data_import
                UNION
                SELECT DISTINCT country_id, country_code, latitude, longitude FROM transform.weather_data_import
            ) AS source
            ON target.country_id = source.country_id
            WHEN MATCHED THEN
                UPDATE SET country_code = source.country_code, latitude = source.latitude, longitude = source.longitude
            WHEN NOT MATCHED THEN
                INSERT (country_id, country_code, latitude, longitude)
                VALUES (source.country_id, source.country_code, source.latitude, source.longitude);
        """

        self.cursor.execute(query)
        self.connection.commit()
        print("Data merged successfully in dim_country!")

    def merge_dim_date(self):
        query = f"""
            MERGE INTO load.dim_date AS target
            USING (
                SELECT DISTINCT 
                    date,
                    TO_CHAR(date, 'YYYYMMDD')::BIGINT AS date_id
                FROM (
                    SELECT date FROM transform.covid_data_import
                    UNION
                    SELECT date FROM transform.weather_data_import
                ) AS combined_dates
            ) AS source
            ON target.date_id = source.date_id
            WHEN NOT MATCHED THEN
                INSERT (date_id, date, year, month, day, day_of_week, is_weekend)
                VALUES (
                    source.date_id,
                    source.date,
                    EXTRACT(YEAR FROM source.date),
                    EXTRACT(MONTH FROM source.date),
                    EXTRACT(DAY FROM source.date),
                    TO_CHAR(source.date, 'Day'),
                    CASE WHEN EXTRACT(DOW FROM source.date) IN (0, 6) THEN TRUE ELSE FALSE END
                );
        """
        self.cursor.execute(query)
        self.connection.commit()
        print("Data merged successfully in dim_date!")

    def merge_fact_covid(self):
        query = f"""
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
                JOIN load.dim_country c ON t.country_code = c.country_code
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
                VALUES (source.country_id, source.date_id, source.confirmed_cases, source.deaths, source.recovered, source.hash_value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        self.cursor.execute(query)
        self.connection.commit()
        print("Data merged successfully in fact_covid!")

    def merge_fact_weather(self):
        query = f"""
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
                JOIN load.dim_country c ON t.country_code = c.country_code
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
                INSERT (country_id, date_id, weather_code, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed, hash_value, createdAt, updatedAt)
                VALUES (source.country_id, source.date_id, source.weather_code, source.mean_temperature, source.mean_surface_pressure, source.precipitation_sum, source.relative_humidity, source.wind_speed, source.hash_value, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
        """
        self.cursor.execute(query)
        self.connection.commit()
        print("Data merged successfully in fact_weather_data!")

    def merge_dim_weather_description(self):
        query = f"""
            MERGE INTO load.dim_weather_code AS target
            USING (
                SELECT DISTINCT weather_code, weather_description FROM transform.weather_data_import
            ) AS source
            ON target.weather_code = source.weather_code
            WHEN MATCHED THEN
                UPDATE SET description = source.weather_description
            WHEN NOT MATCHED THEN
                INSERT (weather_code, description)
                VALUES (source.weather_code, source.weather_description);
        """
        self.cursor.execute(query)
        self.connection.commit()
        print("Data merged successfully in dim_weather_description!")

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print("Connection terminated!")


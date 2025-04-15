import psycopg2 as pg
import pandas as pd

class DatabaseConnector:
    def __init__(self, **db_config):
        self.connection = pg.connect(**db_config)
        self.cursor = self.connection.cursor()

    def fetch_rows(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def add_country(self, **params):
        code = params.get('code')
        name = params.get('name')
        latitude = params.get('latitude')
        longitude = params.get('longitude')
        capital = params.get('capital')

        query = """
        INSERT INTO extract.country (code, name, latitude, longitude, capital)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (code) DO UPDATE
        SET name = EXCLUDED.name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            capital = EXCLUDED.capital;
        """
        self.cursor.execute(query, (code, name, latitude, longitude, capital))
        self.connection.commit()

    def fetch_countries(self):
        countries = self.fetch_rows("SELECT * FROM extract.country")
        columns = [desc[0] for desc in self.cursor.description]
        countries = pd.DataFrame(countries, columns = columns)
        return countries

    def truncate_table(self, table_name):
        query = f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
        self.cursor.execute(query)
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

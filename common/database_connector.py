from psycopg2 import connect, Error
from pandas import DataFrame

class DatabaseConnector:
    def __init__(self, **db_config):
        self.connection = connect(**db_config)
        self.cursor = self.connection.cursor()

    def execute_query(self, query, values=None):
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except Error:
            self.rollback_transaction()

    def fetch_rows(self, query, values=None):
        try:
            if values:
                self.cursor.execute(query, values)
            else:
                self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except Error:
            return []

    def add_country(self, **params):
        code, name, latitude, longitude, capital = (
            params.get('code'),
            params.get('name'),
            params.get('latitude'),
            params.get('longitude'),
            params.get('capital')
        )

        query = """
            INSERT INTO extract.country (code, name, latitude, longitude, capital)
            VALUES (%s, %s, %s, %s, %s);
        """
        values = (code, name, latitude, longitude, capital)
        self.execute_query(query, values)

    def fetch_countries(self):
        query = """
            SELECT * FROM extract.country;
        """
        countries = self.fetch_rows(query)
        columns = [desc[0] for desc in self.cursor.description]
        countries = DataFrame(countries, columns=columns)
        return countries

    def fetch_country_details(self, country_id):
        country_query = """
            SELECT code, latitude, longitude
            FROM extract.country
            WHERE id = %s
        """
        try:
            self.cursor.execute(country_query, (country_id,))
            country_details = self.cursor.fetchone()
            return country_details
        except Error:
            return [None] * 3

    def truncate_table(self, table_name):
        query = """
            TRUNCATE TABLE %s RESTART IDENTITY;
        """
        values = (table_name,)
        self.execute_query(query, values)

    def rollback_transaction(self):
        self.connection.rollback()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

import psycopg2 as pg

class DatabaseConnection:
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

    def fetch_data(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
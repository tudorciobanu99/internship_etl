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
    
    
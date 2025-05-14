import re
from psycopg2 import connect, Error
from pandas import DataFrame
from common.logger import ETLLogger
class DatabaseConnector:
    def __init__(self, **db_config):
        """
        Initialize the DatabaseConnector object.

        Args:
            **db_config (dict): PostgreSQL database connection parameters.
                Keys should include:
                    dbname (str): The name of the database.
                    user (str): The username for authentication.
                    host (str): The database server host.
                    password (str): The password for authentication.
                    port (int): The port number.

        Attributes:
            connection: A live connection to the database.
            cursor: A cursor object associated with the connection.
            logger: A logger instance with the proper
                parametrization done by a ETLLogger object.
        """

        self.connection = connect(**db_config)
        self.cursor = self.connection.cursor()

        etl_logger = ETLLogger(self.__class__.__name__)
        self.logger = etl_logger.get_logger()
        self.logger.info("Connection to the database was established!")

    def execute_query(self, query, values=None):
        """
        Executes a query and commits it to the database.

        Args:
            query (str): The SQL query to be executed. %s placeholders
                are expected to be bound to variables.
            values (tuple): Tuple containing the variables to be bound
                to the placeholders.
        """

        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except Error:
            self.rollback_transaction()

    def fetch_rows(self, query, values=None):
        """
        Fetches all rows of a query result.

        Args:
            query (str): The SQL query to be executed. %s placeholders
                are expected to be bound to variables.
            values (tuple): Tuple containing the variables to be bound
                to the placeholders.

        Returns:
            rows (list of tuples): The rows corresponding to the query.
        """

        try:
            if values:
                self.cursor.execute(query, values)
            else:
                self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except Error:
            self.rollback_transaction()

    def add_country(self, values:tuple):
        """
        Adds a country to the extract.country table.

        Args:
            values (tuple): A 4-element tuple containing:
                code (str): ISO code of the country.
                name (str): Country name.
                latitude (float): The corresponding latitude.
                longitude (float): The corresponding longitude.
        """

        query = """
            INSERT INTO extract.country (code, name, latitude, longitude)
            VALUES (%s, %s, %s, %s);
        """
        self.execute_query(query, values)

    def fetch_countries(self):
        """
        Extracts all the countries in the extract.country table.

        Returns:
            countries (DataFrame): DataFrame with corresponding table column
                names for easier ulterior handling.
        """

        query = """
            SELECT * FROM extract.country;
        """
        countries = self.fetch_rows(query)
        columns = [desc[0] for desc in self.cursor.description]
        countries = DataFrame(countries, columns=columns)
        return countries

    def execute_query_and_return_id(self, query, values:tuple):
        """
        Executes a query, returns the ID of the inserted record
        and commits it to the database.

        Args:
            query (str): The SQL query to be executed. %s placeholders
                are expected to be bound to variables.
            values (tuple): Tuple containing the variables to be bound
                to the placeholders.

        Returns:
            inserted_id (int): The ID of the inserted record.
        """

        try:
            self.cursor.execute(query, values)
            inserted_id = self.cursor.fetchone()[0]
            self.connection.commit()
            return inserted_id
        except Error:
            self.rollback_transaction()

    def truncate_table(self, table_name):
        """
        Truncates a table and resets the primary key sequence.

        Args:
            table_name (str): The name of table to be truncated.
        """

        if not re.fullmatch(r"[a-zA-Z0-9_.]+", table_name):
            raise ValueError("Invalid table name!")

        query = f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
        
        self.execute_query(query)
        self.logger.warning(f"Table {table_name} has been truncated!")

    def rollback_transaction(self):
        """
        Roll back to the start of any pending transaction.
        """

        self.logger.warning("Rolling back the transaction!")
        self.connection.rollback()

    def close_connection(self):
        """
        Closes the cursor and connection.
        """

        self.logger.info("Closing current connection!")
        self.cursor.close()
        self.connection.close()

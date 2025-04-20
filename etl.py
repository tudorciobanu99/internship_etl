import os
from datetime import datetime
from dotenv import load_dotenv
from extract.weather_api import WeatherAPI
from extract.covid_api import CovidAPI
from extract.extract import e_routine
from extract.data_extractor import DataExtractor
from transform.transform import t_routine
from transform.data_transformer import DataTransformer
from load.load import l_routine
from load.data_loader import DataLoader

def initialize_database_objects(**db_config):
    """
    Initializes a DataExtractor, DataTransformer and DataLoader object.

    Args:
        **db_config (dict): PostgreSQL database connection parameters.
                Keys should include:
                    dbname (str): The name of the database.
                    user (str): The username for authentication.
                    host (str): The database server host.
                    password (str): The password for authentication.
                    port (int): The port number.
    """

    e_db = DataExtractor(**db_config)
    t_db = DataTransformer(**db_config)
    l_db = DataLoader(**db_config)
    return e_db, t_db, l_db

if __name__ == "__main__":
    # Load the PostgreSQL database connection parameters from a .env file,
    # located in the root directory of the project.
    load_dotenv('database.env')

    # Constructing a db_config dictionary from the extracted parameters from
    # the .env file.
    db_config = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "host": os.environ.get("HOST"),
        "password": os.environ.get("PASSWORD"),
        "port": int(os.environ.get("PORT")),
    }

    # Initialize the required database objects for the ETL.
    e_db, t_db, l_db = initialize_database_objects(**db_config)

    # The batch date is by default chosen as the current date.
    # The date is the corresponding day but in the year 2022.
    batch_date = datetime.now().strftime("%Y-%m-%d")
    date = batch_date.replace("2025", "2022")

    # Fetches the information about the APIs.
    api_info = e_db.fetch_api_information()

    # Initialize the respective weather API object.
    weather_api_info = api_info[api_info["api_name"] == "Weather API"]
    weather_api = WeatherAPI(
        api_id=weather_api_info["id"].values[0],
        base_url=weather_api_info["api_base_url"].values[0]
    )

    # Initialize the respective COVID-19 API object.
    covid_api_info = api_info[api_info["api_name"] == "COVID API"]
    covid_api = CovidAPI(
        api_id=covid_api_info["id"].values[0],
        base_url=covid_api_info["api_base_url"].values[0]
    )

    # Fetch the countries that are going to be used for data extraction.
    countries = e_db.fetch_countries()

    print('ETL process begins...')

    # The extract process of the ETL.
    e_routine(weather_api, covid_api, e_db, countries, date, batch_date)

    # The transform process of the ETL.
    t_routine(countries, t_db)

    # The load process of the ETL.
    l_routine(l_db)

    print('ETL process has been completed!')

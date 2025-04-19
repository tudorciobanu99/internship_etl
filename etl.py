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
    e_db = DataExtractor(**db_config)
    t_db = DataTransformer(**db_config)
    l_db = DataLoader(**db_config)
    return e_db, t_db, l_db

if __name__ == "__main__":
    # My PostgreSQL database connection details:
    load_dotenv('database.env')

    db_config = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "host": os.environ.get("HOST"),
        "password": os.environ.get("PASSWORD"),
        "port": int(os.environ.get("PORT")),
        }

    # Initialize database objects
    e_db, t_db, l_db = initialize_database_objects(**db_config)

    # Date for ETL process
    batch_date = datetime.now().strftime("%Y-%m-%d")
    date = batch_date.replace("2025", "2022")

    # Fetch API information
    api_info = e_db.fetch_api_information()

    weather_api_info = api_info[api_info["api_name"] == "Weather API"]
    weather_api = WeatherAPI(
        api_id=weather_api_info["id"].values[0],
        base_url=weather_api_info["api_base_url"].values[0],
    )

    covid_api_info = api_info[api_info["api_name"] == "COVID API"]
    covid_api = CovidAPI(
        api_id=covid_api_info["id"].values[0],
        base_url=covid_api_info["api_base_url"].values[0],
    )

    # Extract countries
    countries = e_db.fetch_countries()

    # Extract data from APIs
    e_routine(weather_api, covid_api, e_db, countries, date, batch_date)

    # Transform data
    t_routine(countries, t_db)

    # Load data
    l_routine(l_db)

    print('ETL process completed successfully!')

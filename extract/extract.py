# IMPORTS
from APIWrapper import APIWrapper
from DatabaseConnection import DatabaseConnection
import pandas as pd
import requests, datetime, os, json
from dotenv import load_dotenv
from helper_functions import *

if __name__ == "__main__":
    load_dotenv('../database_password.env')

    my_db = DatabaseConnection(
        dbname="etl",
        user="postgres",
        host="localhost",
        password=os.environ.get("db_password"),
        port=5432,
    )

    # Fetching the API information from the database
    api_info = my_db.fetch_data("SELECT * FROM extract.api_info")
    columns = my_db.fetch_data("SELECT column_name FROM information_schema.columns WHERE table_name = 'api_info'")
    columns = [col[0] for col in columns]
    api_info = pd.DataFrame(api_info, columns = columns)

    weather_api_info = api_info[api_info["api_name"] == "Weather API"]
    weather_api = APIWrapper(
        api_id=weather_api_info["id"].values[0],
        base_url=weather_api_info["api_base_url"].values[0],
    )

    covid_api_info = api_info[api_info["api_name"] == "COVID API"]
    covid_api = APIWrapper(
        api_id=covid_api_info["id"].values[0],
        base_url=covid_api_info["api_base_url"].values[0],
    )

    now = datetime.datetime.now()
    batch_date = now.strftime("%Y-%m-%d")
    batch_date = "2025-04-03"

    # Fetching the countries and fields from the database
    countries = my_db.fetch_data("SELECT id, code, latitude, longitude FROM extract.country")
    countries = pd.DataFrame(countries, columns=["id", "code", "latitude", "longitude"])

    # Preparing parameters for the apis
    start_date = "2023-01-03"
    end_date = "2023-01-03"
    weather_params = prepare_weather_params(countries, start_date, end_date)
    covid_params = prepare_covid_params(countries, start_date)

    # Fields to be extracted from the APIs
    weather_fields = [
        "daily/weather_code",
        "daily/temperature_2m_mean",
        "daily/surface_pressure_mean",
        "daily/relative_humidity_2m_mean",
        "daily/time"
    ]
    weather_error_field = "reason"

    covid_fields = [
        "data/confirmed",
        "data/deaths",
        "data/recovered",
        "data/active",
        "data/date"
    ]
    covid_error_field = "error"

    # Connecting to the APIs and fetching data
    weather_import_log_data = weather_api.fetch_data(
        countries=countries["code"].tolist(),
        fields=weather_fields,
        error_field=weather_error_field,
        **weather_params
    )
    covid_import_log_data = covid_api.fetch_data(
        countries=countries["code"].tolist(),
        fields=covid_fields,
        error_field=covid_error_field,
        **covid_params
    )

    # Saving the data to JSON files
    weather_api.save_data('../raw/weather_data', 'weather_data.json', batch_date)
    covid_api.save_data('../raw/covid_data', 'covid_data.json', batch_date)

    # Save the api import log data to the database
    save_api_import_log(weather_import_log_data, my_db)
    save_api_import_log(covid_import_log_data, my_db)

    # Save the import log data to the database
    import_directory_name = "../raw/weather_data"
    import_file_name = "weather_data.json"
    save_import_log(batch_date, weather_import_log_data, import_directory_name, import_file_name, my_db)

    import_directory_name = "../raw/covid_data"
    import_file_name = "covid_data.json"
    save_import_log(batch_date, covid_import_log_data, import_directory_name, import_file_name, my_db)

    my_db.close_connection()
    









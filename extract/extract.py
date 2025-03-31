# IMPORTS
from APIWrapper import APIWrapper
from database_connection import DatabaseConnection
import pandas as pd
import requests

if __name__ == "__main__":
    my_db = DatabaseConnection(
        dbname="etl",
        user="postgres",
        host="localhost",
        password="Darkmaster1999",
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

    # Fetching the countries and fields from the database
    countries = my_db.fetch_data("SELECT id, code, latitude, longitude FROM extract.country")
    countries = pd.DataFrame(countries, columns=["id", "code", "latitude", "longitude"])

    # Preparation of the API parameters
    weather_params = {
        country["code"]: {
            "latitude": float(country["latitude"]),
            "longitude": float(country["longitude"]),
            "start_date": "2023-01-06",
            "end_date": "2023-01-06",
            "daily": ",".join(["weather_code", "temperature_2m_mean", "surface_pressure_mean", "relative_humidity_2m_mean"]),
            "timezone": "Europe/Berlin",
        }
        for _, country in countries.iterrows()
    }

    covid_params = {
        country["code"]: {
            "iso": country["code"],
            "date": "2023-01-06",
        }
        for _, country in countries.iterrows()
    }

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
    time_weather = weather_api.save_data('../raw/weather_data', 'weather_data.json')
    time_covid = covid_api.save_data('../raw/covid_data', 'covid_data.json')

    my_db.close_connection()
    









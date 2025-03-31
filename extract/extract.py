# IMPORTS
from APIWrapper import APIWrapper
import requests, pandas as pd, psycopg2 as pg

if __name__ == "__main__":

    # Database connection
    connection = pg.connect(
        database = "etl",
        user = "postgres",
        host = "localhost",
        password = "Darkmaster1999",
        port = "5432",
    )

    cursor = connection.cursor()
    print("Connection successful!")

    cursor.execute("SELECT id, code, latitude, longitude FROM extract.country;")
    rows = cursor.fetchall()

    print(rows)

    columns = ["id", "code", "latitude", "longitude"]
    df = pd.DataFrame(rows, columns=columns)
    
    codes = df['code'].tolist()
    latitudes = df['latitude'].tolist()
    longitudes = df['longitude'].tolist()

    country_params = {
        code: {
            "latitude": float(lat),
            "longitude": float(lon),
            "start_date": "2022-05-13",
            "end_date": "2022-05-13",
            "hourly": ",".join(["temperature_2m", "relative_humidity_2m", "weather_code", "surface_pressure"]),
            "timezone": "Europe/Berlin"
        }
        for code, lat, lon in zip(codes, latitudes, longitudes)
    }

    # print(country_params)

    weather_api = APIWrapper('1', "https://historical-forecast-api.open-meteo.com/v1/forecast")
    error_field = "reason"
    import_log = weather_api.fetch_data(codes, ["hourly/temperature_2m", "hourly/relative_humidity_2m", "hourly/weather_code", "hourly/surface_pressure"], error_field,  **country_params)
    print(import_log)
    # complete_url = weather_api.get_endpoint(**country_params['USA'])
    # response = requests.get(complete_url)
    # x = response.json()
    # print(x)

    # covid_api = APIWrapper('2', "https://covid-api.com/api/reports/total")
    # params = {
    #     "country": {
    #         "iso": "DEU",
    #         "date": "2022-05-13",
    #     }
    # }

    # fields = ["data/confirmed", "data/deaths", "data/recovered"]
    # error_field = "error"
    # import_log = covid_api.fetch_data(['country'], fields, error_field, **params)
    # print(import_log)



    # country_coordinates = {
    #     'Moldova': {'latitude': 47.4116, 'longitude': 28.3699},
    #     'Germany': {'latitude': 52.5200, 'longitude': 13.4050},
    #     'France': {'latitude': 48.8566, 'longitude': 2.3522}
    # }

    # country_params = {
    #     country: {
    #         "latitude": coords['latitude'],
    #         "longitude": coords['longitude'],
    #         "start_date": "2022-05-13s",
    #         "end_date": "2022-05-13",
    #         "hourly": ",".join(["temperature_2m", "relative_humidity_2m", "weather_code", "surface_pressure"]),
    #         "timezone": "Europe/Berlin"
    #     }
    #     for country, coords in country_coordinates.items()
    # }

    # weather_api = APIWrapper('1', "https://historical-forecast-api.open-meteo.com/v1/forecast")

    # fields = ["hourly/temperature_2m", "hourly/relative_humidity_2m", "hourly/weather_code", "hourly/surface_pressure"]
    # weather_api.fetch_data(country_coordinates, fields, **country_params)

    # covid_api = APIWrapper('2', "https://covid-api.com/api/reports/total")
    # country = "DE"
    # params = {
    #     country: {
    #         "iso": "DE",
    #         "date": "2022-05-13",
    #     }
    # }

    # complete_url = covid_api.get_endpoint(**params['DE'])
    # response = requests.get(complete_url)
    # x = response.json()
    # print(x)

    # fields = ["data/confirmed", "data/deaths", "data/recovered"]
    # covid_api.fetch_data([country], fields, **params)









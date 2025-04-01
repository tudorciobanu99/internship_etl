# IMPORTS
from APIWrapper import APIWrapper
from database_connection import DatabaseConnection
import pandas as pd
import requests, datetime, os, json

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

    batch_date = '2023-01-06'

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
    # weather_api.save_data('../raw/weather_data', 'weather_data.json')
    # covid_api.save_data('../raw/covid_data', 'covid_data.json')

    def save_api_import_log(import_log_data, db):
        api_id = import_log_data["api_id"]
        countries = import_log_data["countries"]
        start_time = import_log_data["start_time"]
        end_time = import_log_data["end_time"]
        code_response = import_log_data["response_codes"]
        error_message = import_log_data["error_messages"]

        countries_id = db.fetch_data(
            f"""
            SELECT id FROM extract.country WHERE code IN ({', '.join(f"'{country}'" for country in countries)})
            """
        )
        countries_id = [int(country[0]) for country in countries_id]

        for country, code, error in zip(countries_id, code_response, error_message):
            query = f"""
                INSERT INTO extract.api_import_log (country_id, api_id, start_time, end_time, code_response, error_message)
                VALUES ({country}, {int(api_id)}, '{start_time}', '{end_time}', {code}, '{error}')
                """
            db.cursor.execute(query)
            db.connection.commit()

    # Save the import log data to the database
    #save_api_import_log(weather_import_log_data, my_db)
    #save_api_import_log(covid_import_log_data, my_db)

    def get_json_row_count(file_path, country_code):
        try:
            # Open and load the JSON file
            with open(file_path, 'r') as file:
                data = json.load(file)

            # Count the total number of rows
            row_count = sum(1 for date, countries in data.items() if country_code in countries)
            return row_count
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return 0
        except json.JSONDecodeError:
            print(f"Invalid JSON format in file: {file_path}")
        return 0

    # Save the import log data to the database
    def save_import_log(batch_date, import_log_data, import_directory_name, import_file_name, db):
        countries = import_log_data["countries"]
        api_id = import_log_data["api_id"]


        countries_id = db.fetch_data(
            f"""
            SELECT id FROM extract.country WHERE code IN ({', '.join(f"'{country}'" for country in countries)})
            """
        )
        countries_id = [int(country[0]) for country in countries_id]

        existing_record = db.fetch_data(
        f"""
        SELECT file_created_date FROM extract.import_log
        WHERE import_directory_name = '{import_directory_name}' AND import_file_name = '{import_file_name}'
        LIMIT 1
        """
        )

        now = datetime.datetime.now()
        file_last_modified_date = now.strftime("%Y-%m-%d")

        # Determine the file_created_date
        if existing_record:
            file_created_date = existing_record[0][0]
        else:
            file_created_date = file_last_modified_date

        for i in range(len(countries_id)):
            query = f"""
                INSERT INTO extract.import_log (
                    batch_date, country_id, import_directory_name, import_file_name,
                    file_created_date, file_last_modified_date, row_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            row_count = get_json_row_count(os.path.join(import_directory_name, import_file_name), countries[i])
        # Execute the query with parameterized values
            db.cursor.execute(query, (
                batch_date, countries_id[i], import_directory_name, import_file_name,
                file_created_date, file_last_modified_date, row_count
            ))
        db.connection.commit()

    import_directory_name = "../raw/weather_data"
    import_file_name = "weather_data.json"
    save_import_log(batch_date, weather_import_log_data, import_directory_name, import_file_name, my_db)

    my_db.close_connection()
    









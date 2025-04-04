# IMPORTS
from covid_api import covid_api
from weather_api import weather_api
from databaseconnection import databaseconnection
import os, json
from dotenv import load_dotenv

def save_to_json(data, import_directory_name, import_file_name):
        if not os.path.exists(import_directory_name):
            os.makedirs(import_directory_name)

        file_path = os.path.join(import_directory_name, import_file_name)

        with open(file_path, 'w') as outfile:
            json.dump(data, outfile, indent=4)
        print(f"Data has been saved to {file_path}!")

def get_json_row_count(import_directory_name, import_file_name, country_code):
    try:
        file_path = os.path.join(import_directory_name, import_file_name)

        with open(file_path, 'r') as file:
            data = json.load(file)
        
        total_rows = 0
        if not data:
            print(f"Empty dictionary! Total rows: {total_rows}")
        else:
            if isinstance(data[country_code], str):
                total_rows += len(data[country_code].split('\n'))  # Count lines in string
            else:
                for sub_key, sub_content in data[country_code].items():
                    if isinstance(sub_content, (dict, list)):
                        row_count = len(sub_content)
                        total_rows += row_count
                    else:
                        total_rows += 1
            print(f"Total rows for {country_code}: {total_rows}")
        return total_rows
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return 0
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {file_path}")
    return 0

def routine(weather_api, covid_api, my_db, countries, weather_import_directory_name, covid_import_directory_name, weather_import_file_name, covid_import_file_name):
        try:
            for index, country in countries.iterrows():
                response, start_time = weather_api.send_request(country, date)
                my_db.insert_initial_api_import_log(int(country['id']), int(weather_api.api_id), start_time) 
                end_time, code_response, error_message = weather_api.get_response(response, country)
                my_db.update_api_import_log(int(country['id']), int(weather_api.api_id), start_time, end_time, code_response, error_message)

            for index, country in countries.iterrows():
                response, start_time = covid_api.send_request(country, date)
                my_db.insert_initial_api_import_log(int(country['id']), int(covid_api.api_id), start_time) 
                end_time, code_response, error_message = covid_api.get_response(response, country)
                my_db.update_api_import_log(int(country['id']), int(covid_api.api_id), start_time, end_time, code_response, error_message)


            for index, country in countries.iterrows():
                my_db.insert_initial_import_log(batch_date, int(country['id']), weather_import_directory_name.lstrip('../'), weather_import_file_name)
                my_db.insert_initial_import_log(batch_date, int(country['id']), covid_import_directory_name.lstrip('../'), covid_import_file_name)
            
            save_to_json(weather_api.data, weather_import_directory_name, weather_import_file_name)
            save_to_json(covid_api.data, covid_import_directory_name, covid_import_file_name)

            for index, country in countries.iterrows():
                weather_row_count = get_json_row_count(weather_import_directory_name, weather_import_file_name, country['code'])
                covid_row_count = get_json_row_count(covid_import_directory_name, covid_import_file_name, country['code'])
                my_db.update_import_log(batch_date, int(country['id']), weather_import_directory_name.lstrip('../'), weather_import_file_name, batch_date, batch_date, weather_row_count)
                my_db.update_import_log(batch_date, int(country['id']), covid_import_directory_name.lstrip('../'), covid_import_file_name, batch_date, batch_date, covid_row_count)
        except Exception as e:
            print(f"An error occurred: {e}")
            my_db.rollback_transaction()
        
        my_db.close_connection()

if __name__ == "__main__":
    load_dotenv('../database_password.env')

    my_db = databaseconnection(
        dbname="etl",
        user="postgres",
        host="localhost",
        password=os.environ.get("db_password"),
        port=5432,
    )

    api_info = my_db.fetch_api_information()

    weather_api_info = api_info[api_info["api_name"] == "Weather API"]
    weather_api = weather_api(
        api_id=weather_api_info["id"].values[0],
        base_url=weather_api_info["api_base_url"].values[0],
    )

    covid_api_info = api_info[api_info["api_name"] == "COVID API"]
    covid_api = covid_api(
        api_id=covid_api_info["id"].values[0],
        base_url=covid_api_info["api_base_url"].values[0],
    )

    batch_date = "2025-04-04"
    date = batch_date.replace("2025", "2022")

    countries = my_db.fetch_countries()
    weather_import_directory_name = '../raw/weather_data'
    covid_import_directory_name = '../raw/covid_data'
    weather_import_file_name = 'weather_data' + '_' + batch_date + '.json'
    covid_import_file_name = 'covid_data' + '_' + batch_date + '.json'
    
    routine(weather_api, covid_api, my_db, countries, weather_import_directory_name, covid_import_directory_name, weather_import_file_name, covid_import_file_name)


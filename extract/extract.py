import os
import json
from datetime import datetime
from dotenv import load_dotenv
from covid_api import CovidAPI
from weather_api import WeatherAPI
from data_extractor import DataExtractor

def save_to_json(data, import_dir_name, import_file_name):
    if not os.path.exists(import_dir_name):
        os.makedirs(import_dir_name)

    file_path = os.path.join(import_dir_name, import_file_name)
    with open(file_path, 'w', encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)

def get_json_row_count(import_directory_name, import_file_name):
    try:
        file_path = os.path.join(import_directory_name, import_file_name)
        with open(file_path, 'r', encoding="utf-8") as file:
            data = json.load(file)

        total_rows = 0
        if data:
            if isinstance(data, str):
                total_rows += 1
            else:
                for _, sub_content in data.items():
                    if isinstance(sub_content, (dict, list)):
                        row_count = len(sub_content)
                        total_rows += row_count
                    else:
                        total_rows += 1
        return total_rows
    except FileNotFoundError:
        return 0
    except json.JSONDecodeError:
        return 0

def routine(w_api:WeatherAPI, c_api:CovidAPI, db:DataExtractor, countries, **params):
    date = params['date']
    batch_date = params['batch_date']
    w_import_dir_name = params['weather_import_dir_name']
    c_import_dir_name = params['covid_import_dir_name']
    w_import_file_name = params['weather_import_file_name']
    c_import_file_name = params['covid_import_file_name']

    try:
        for _, country in countries.iterrows():
            response, start_time = w_api.send_request(country, date)
            db.insert_initial_api_import_log(int(country['id']), int(w_api.api_id), start_time)
            end_time, code_resp, error_message, resp_body = w_api.get_response(response)
            u_api_params = {
                'country_id': int(country['id']),
                'api_id': int(w_api.api_id),
                'start_time': start_time,
                'end_time': end_time,
                'code_response': code_resp,
                'error_message': error_message
            }
            db.update_api_import_log(**u_api_params)

            w_file_name = w_import_file_name + '_' + country['code'] + '_' + batch_date + '.json'
            i_import_params = {
                'batch_date': batch_date,
                'country_id': int(country['id']),
                'import_dir_name': w_import_dir_name.lstrip('../'),
                'import_file_name': w_file_name
            }
            db.insert_initial_import_log(**i_import_params)
            save_to_json(resp_body, w_import_dir_name, w_file_name)
            w_row_count = get_json_row_count(w_import_dir_name, w_file_name)
            u_import_params = {
                'batch_date': batch_date,
                'country_id': int(country['id']),
                'import_dir_name': w_import_dir_name.lstrip('../'),
                'import_file_name': w_file_name,
                'file_created_date': batch_date,
                'file_last_modified_date': batch_date,
                'row_count': w_row_count
            }
            db.update_import_log(**u_import_params)

            response, start_time = c_api.send_request(country, date)
            db.insert_initial_api_import_log(int(country['id']), int(c_api.api_id), start_time)
            end_time, code_resp, error_message, resp_body = c_api.get_response(response)
            u_api_params = {
                'country_id': int(country['id']),
                'api_id': int(c_api.api_id),
                'start_time': start_time,
                'end_time': end_time,
                'code_response': code_resp,
                'error_message': error_message
            }
            db.update_api_import_log(**u_api_params)

            c_file_name = c_import_file_name + '_' + country['code'] + '_' + batch_date + '.json'
            i_import_params = {
                'batch_date': batch_date,
                'country_id': int(country['id']),
                'import_dir_name': c_import_dir_name.lstrip('../'),
                'import_file_name': c_file_name
            }
            db.insert_initial_import_log(**i_import_params)
            save_to_json(resp_body, c_import_dir_name, c_file_name)
            c_row_count = get_json_row_count(c_import_dir_name, c_file_name)
            u_import_params = {
                'batch_date': batch_date,
                'country_id': int(country['id']),
                'import_dir_name': c_import_dir_name.lstrip('../'),
                'import_file_name': c_file_name,
                'file_created_date': batch_date,
                'file_last_modified_date': batch_date,
                'row_count': c_row_count
            }
            db.update_import_log(**u_import_params)

        print('Routine completed successfully!')
    except Exception:
        print('An error occurred during the routine. Rolling back the transaction.')
        db.rollback_transaction()
    db.close_connection()

if __name__ == "__main__":
    load_dotenv('database_password.env')

    db_config = {
        "dbname": "etl",
        "user": "postgres",
        "host": "localhost",
        "password": os.environ.get("db_password"),
        "port": 5432,
        }
    my_db = DataExtractor(**db_config)

    api_info = my_db.fetch_api_information()

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

    batch_date = datetime.now().strftime("%Y-%m-%d")
    date = batch_date.replace("2025", "2022")

    countries = my_db.fetch_countries()
    WHEATHER_IMPORT_DIR_NAME = 'data/raw/weather_data'
    COVID_IMPORT_DIR_NAME = 'data/raw/covid_data'
    WEATHER_IMPORT_FILE_NAME = 'weather_data'
    COVID_IMPORT_FILE_NAME = 'covid_data'

    params = {
        'batch_date' : batch_date,
        'date' : date,
        'weather_import_dir_name': WHEATHER_IMPORT_DIR_NAME,
        'covid_import_dir_name': COVID_IMPORT_DIR_NAME,
        'weather_import_file_name': WEATHER_IMPORT_FILE_NAME,
        'covid_import_file_name': COVID_IMPORT_FILE_NAME
    }

    routine(weather_api, covid_api, my_db, countries, **params)

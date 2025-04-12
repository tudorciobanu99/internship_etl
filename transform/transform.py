import os
import shutil
import json
import csv
from transform.data_transformer import DataTransformer
from extract.extract import get_json_row_count

def list_all_files_from_directory(directory):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def get_file_details(file):
    country_code = file.split('/')[-1].split('_')[2]
    batch_date = file.split('/')[-1].split('_')[3].split('.')[0]
    return country_code, batch_date

def open_file(file):
    data = None
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def move_file(file, dir_name, file_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    shutil.move(file, dir_name + file_name)

def get_weather_description(weather_code, csv_file_path='weather_description/wmo_code_4677.csv'):
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['Weather Code'] == weather_code:
                    return row['Description']
        return None
    except (FileNotFoundError, KeyError):
        return None

def process_weather_file(file, country_code, batch_date, countries, db):
    status = 'error'
    file_name = file.split('/')[-1]
    p_dir_name = 'data/error/weather_data/'

    if country_code in countries['code'].values:
        country_id = countries[countries['code'] == country_code]['id'].values[0]

        try:
            db.insert_initial_transform_log(batch_date, country_id, 'processing')

            data = open_file(file)
            try:
                date = data['daily']['time'][0]
                weather_code = data['daily']['weather_code'][0]
                mean_temperature = data['daily']['temperature_2m_mean'][0]
                mean_surface_pressure = data['daily']['surface_pressure_mean'][0]
                precipitation_sum = data['daily']['precipitation_sum'][0]
                relative_humidity = data['daily']['relative_humidity_2m_mean'][0]
                wind_speed = data['daily']['wind_speed_10m_mean'][0]
                weather_description = get_weather_description(str(weather_code))
                if not weather_description:
                    weather_description = "Unknown"

                i_params = {
                    'country_id': country_id,
                    'date': date,
                    'weather_code': weather_code,
                    'weather_description': weather_description,
                    'mean_temperature': mean_temperature,
                    'mean_surface_pressure': mean_surface_pressure,
                    'precipitation_sum': precipitation_sum,
                    'relative_humidity': relative_humidity,
                    'wind_speed': wind_speed
                }
                db.insert_weather_data(**i_params)
                status = 'processed'
                p_dir_name = 'data/processed/weather_data/'

                move_file(file, p_dir_name, file_name)
            except KeyError:
                move_file(file, p_dir_name, file_name)

            row_count = get_json_row_count(p_dir_name, file_name)
            u_params = {
                'batch_date': batch_date,
                'country_id': country_id,
                'p_dir_name': p_dir_name,
                'p_file_name': file_name,
                'row_count': row_count,
                'status': status
            }
            db.update_transform_log(**u_params)

        except Exception:
            db.rollback_transaction()

def process_covid_file(file, country_code, batch_date, countries, db):
    status = 'error'
    file_name = file.split('/')[-1]
    p_dir_name = 'data/error/covid_data/'

    if country_code in countries['code'].values:
        country_id = countries[countries['code'] == country_code]['id'].values[0]

        try:
            db.insert_initial_transform_log(batch_date, country_id, 'processing')

            data = open_file(file)
            try:
                date = data['data']['date']
                confirmed_cases = data['data']['confirmed_diff']
                deaths = data['data']['deaths_diff']
                recovered = data['data']['recovered_diff']

                i_params = {
                    'country_id': country_id,
                    'date': date,
                    'confirmed_cases': confirmed_cases,
                    'deaths': deaths,
                    'recovered': recovered
                }
                db.insert_covid_data(**i_params)
                status = 'processed'
                p_dir_name = 'data/processed/covid_data/'

                move_file(file, p_dir_name, file_name)
            except KeyError:
                move_file(file, p_dir_name, file_name)

            row_count = get_json_row_count(p_dir_name, file_name)
            u_params = {
                'batch_date': batch_date,
                'country_id': country_id,
                'p_dir_name': p_dir_name,
                'p_file_name': file_name,
                'row_count': row_count,
                'status': status
            }
            db.update_transform_log(**u_params)

        except Exception:
            db.rollback_transaction()

def t_routine(countries, db: DataTransformer):
    files_weather = list_all_files_from_directory('data/raw/weather_data')
    files_covid = list_all_files_from_directory('data/raw/covid_data')

    db.truncate_table('transform.weather_data_import')
    db.truncate_table('transform.covid_data_import')

    for file in files_weather:
        country_code, batch_date = get_file_details(file)
        process_weather_file(file, country_code, batch_date, countries, db)

    for file in files_covid:
        country_code, batch_date = get_file_details(file)
        process_covid_file(file, country_code, batch_date, countries, db)

    db.close_connection()

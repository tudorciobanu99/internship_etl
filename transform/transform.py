import sys, os, json, datetime, shutil, csv
from databaseconnection import databaseconnection
from dotenv import load_dotenv
sys.path.append('../extract')
from extract import get_json_row_count

def list_all_files_from_directory(directory):
    files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def get_file_details(file):
    country_code = file.split('/')[-1].split('_')[2]
    batch_date = file.split('/')[-1].split('_')[3].split('.')[0]
    return country_code, batch_date

def open_file(file):
    data = None
    with open(file, 'r') as f:
        data = json.load(f)
    return data

def get_weather_description(weather_code, csv_file_path='../weather_description/wmo_code_4677.csv'):
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['Weather Code'] == weather_code:
                    return row['Description']
    except FileNotFoundError:
        print(f"CSV file not found at {csv_file_path}.")
    except KeyError as e:
        print(f"Key error: {e}. Check the CSV file headers.")
    except Exception as e:
        print(f"An error occurred: {e}.")
    
    return None

def process_weather_file(file, country_code, batch_date, countries, db):
    status = 'error'
    file_name = file.split('/')[-1]
    processed_directory_name = '../data/error/weather_data/'

    if country_code not in countries['code'].values:
        print(f"Country code {country_code} not found in countries list.")
    else:
        country_id = countries[countries['code'] == country_code]['id'].values[0]
        print(f"Country ID: {country_id}")
        
        try:
            db.insert_initial_transform_log(batch_date, country_id, 'processing')

            data = open_file(file)

            if not data:
                print(f"No data found in file: {file}")
            elif isinstance(data, str):
                print(f"Invalid expected data format in file: {file}")
            else:
                try:
                    date = data['daily']['time'][0]
                    weather_code = data['daily']['weather_code'][0]
                    mean_temperature = data['daily']['temperature_2m_mean'][0]
                    mean_surface_pressure = data['daily']['surface_pressure_mean'][0]
                    precipitation_sum = data['daily']['precipitation_sum'][0]
                    relative_humidity = data['daily']['relative_humidity_2m_mean'][0]
                    wind_speed = data['daily']['wind_speed_10m_mean'][0]
                    
                    weather_description = get_weather_description(str(weather_code), '../weather_description/wmo_code_4677.csv')

                    if not weather_description:
                        print(f"Weather description not found for code {weather_code}.")
                        weather_description = "Unknown"

                    print('Ready to insert data into the database!')
                    db.insert_weather_data(country_id, date, weather_code, weather_description, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed)  
                    status = 'processed'
                    processed_directory_name = '../data/processed/weather_data/'
                    
                    shutil.move(file, processed_directory_name + file_name)

                except KeyError as e:
                    print(f"Key error: {e}")
                    print(f"Failed to process data for file: {file}")

                    shutil.move(file, processed_directory_name + file_name)

            row_count = get_json_row_count(processed_directory_name, file_name)
            db.update_transform_log(batch_date, country_id, '../data/processed/weather_data/'.strip('../'), file_name, row_count, status)

        except Exception as e:
                print(f"An error occurred: {e}")
                db.connection.rollback()
                    
        print('\n')

def process_covid_file(file, country_code, batch_date, countries, db):
    status = 'error'
    file_name = file.split('/')[-1]
    processed_directory_name = '../data/error/covid_data/'

    if country_code not in countries['code'].values:
        print(f"Country code {country_code} not found in countries list.")
    else:
        country_id = countries[countries['code'] == country_code]['id'].values[0]
        print(f"Country ID: {country_id}")
        
        try:
            db.insert_initial_transform_log(batch_date, country_id, 'processing')

            data = open_file(file)

            if not data:
                print(f"No data found in file: {file}")
            elif isinstance(data, str):
                print(f"Invalid expected data format in file: {file}")
            else:
                try:
                    date = data['data']['date']
                    confirmed_cases = data['data']['confirmed_diff']
                    deaths = data['data']['deaths_diff']
                    recovered = data['data']['recovered_diff']

                    print('Ready to insert data into the database!')
                    db.insert_covid_data(country_id, date, confirmed_cases, deaths, recovered)  
                    status = 'processed'
                    processed_directory_name = '../data/processed/covid_data/'
                    
                    shutil.move(file, processed_directory_name + file_name)

                except KeyError as e:
                    print(f"Key error: {e}")
                    print(f"Failed to process data for file: {file}")

                    shutil.move(file, processed_directory_name + file_name)

            row_count = get_json_row_count(processed_directory_name, file_name)
            db.update_transform_log(batch_date, country_id, '../data/processed/covid_data/'.strip('../'), file_name, row_count, status)

        except Exception as e:
                print(f"An error occurred: {e}")
                db.connection.rollback()
                    
        print('\n')



def routine(countries, db):
    files_weather = list_all_files_from_directory('../data/raw/weather_data')
    files_covid = list_all_files_from_directory('../data/raw/covid_data')

    db.truncate_table('transform.weather_data_import')
    db.truncate_table('transform.covid_data_import')

    for file in files_weather:
        print('\n')
        print(f"Processing weather file: {file}")
        country_code, batch_date = get_file_details(file)
        process_weather_file(file, country_code, batch_date, countries, db)

    for file in files_covid:
        print('\n')
        print(f"Processing COVID file: {file}")
        country_code, batch_date = get_file_details(file)
        process_covid_file(file, country_code, batch_date, countries, db)
        
    my_db.close_connection()
    print('Routine completed successfully!')

if __name__ == "__main__":
    load_dotenv('../database_password.env')

    my_db = databaseconnection(
        dbname="etl",
        user="postgres",
        host="localhost",
        password=os.environ.get("db_password"),
        port=5432,
    )

    countries = my_db.fetch_countries()

    routine(countries, my_db)
from databaseconnection import databaseconnection
import os, json, datetime
from dotenv import load_dotenv

def list_all_files_from_directory(directory):
    files = []
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def save_to_json(data, import_directory_name, import_file_name):
        if not os.path.exists(import_directory_name):
            os.makedirs(import_directory_name)

        file_path = os.path.join(import_directory_name, import_file_name)

        with open(file_path, 'w') as outfile:
            json.dump(data, outfile, indent=4)
        print(f"Data has been saved to {file_path}!")

def get_json_row_count(import_directory_name, import_file_name):
    try:
        file_path = os.path.join(import_directory_name, import_file_name)

        with open(file_path, 'r') as file:
            data = json.load(file)
        
        total_rows = 0
        if not data:
            print(f"Empty dictionary! Total rows: {total_rows}")
        else:
            if isinstance(data, str):
                total_rows += 1
            else:
                for sub_key, sub_content in data.items():
                    if isinstance(sub_content, (dict, list)):
                        row_count = len(sub_content)
                        total_rows += row_count
                    else:
                        total_rows += 1
            print(f"Total rows: {total_rows}")
        return total_rows
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return 0
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {file_path}")
    return 0

def routine(countries, db):
    files_weather = list_all_files_from_directory('../raw/weather_data')
    files_covid = list_all_files_from_directory('../raw/covid_data')

    for file in files_weather:
        print('\n')
        print(f"Processing weather file: {file}")

        status = 'error'
        
        country_code = file.split('/')[-1].split('_')[2]
        batch_date = file.split('/')[-1].split('_')[3].split('.')[0]

        if country_code not in countries['code'].values:
            print(f"Country code {country_code} not found in countries list.")
        else:
            country_id = countries[countries['code'] == country_code]['id'].values[0]
            print(f"Country code: {country_code}")
            print(f"Country ID: {country_id}")

            db.insert_initial_transform_log(batch_date, country_id, 'processing')

            with open(file, 'r') as f:
                data = json.load(f)

            if not data:
                print(f"No data found in file: {file}")
            elif isinstance(data, str):
                print(f"Invalid expected data format in file: {file}")
            else:
                weather_data_subset = []
                try:
                    date = data['daily']['time']
                    weather_code = data['daily']['weather_code']
                    mean_temperature = data['daily']['temperature_2m_mean']
                    mean_surface_pressure = data['daily']['surface_pressure_mean']
                    precipitation_sum = data['daily']['precipitation_sum']
                    relative_humidity = data['daily']['relative_humidity_2m_mean']
                    wind_speed = data['daily']['windspeed_10m_mean']

                    weather_data_subset.append(date, weather_code, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed)
                    status = 'processed'
                except KeyError as e:
                    print(f"Key error: {e}")
                
                if status == 'processed' and len(weather_data_subset) > 0:
                    print('Ready to insert data into the database!')

                    processed_directory = '../processed/weather_data'
                    processed_file_name = f"_{country_code}_{batch_date}_{status}.json"

                    date, weather_code, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed = weather_data_subset

                    new_data = {
                        'date': date,
                        'weather_code': weather_code,
                        'mean_temperature': mean_temperature,
                        'mean_surface_pressure': mean_surface_pressure,
                        'precipitation_sum': precipitation_sum,
                        'relative_humidity': relative_humidity,
                        'wind_speed': wind_speed
                    }

                    save_to_json(new_data, processed_directory, processed_file_name)
                    
                    db.update_transform_log(batch_date, country_id, processed_directory.strip('../'), processed_file_name, len(weather_data_subset), status)
                    db.insert_weather_data(country_id, date, weather_code, mean_temperature, mean_surface_pressure, precipitation_sum, relative_humidity, wind_speed)
                else:
                    print(f"Failed to process data for file: {file}")
                    processed_directory = '../error/weather_data'
                    processed_file_name = f"_{country_code}_{batch_date}_{status}.json"

                    row_count = get_json_row_count('../raw/weather_data', file.split('/')[-1])
                    
                    save_to_json(data, processed_directory, processed_file_name)
                    db.update_transform_log(batch_date, country_id, processed_directory.strip('../'), processed_file_name, row_count, status)
                    
        print('\n')

    for file in files_covid:
        print('\n')
        print(f"Processing COVID file: {file}")

        status = 'error'
        
        country_code = file.split('/')[-1].split('_')[2]
        batch_date = file.split('/')[-1].split('_')[3].split('.')[0]

        if country_code not in countries['code'].values:
            print(f"Country code {country_code} not found in countries list.")
        else:
            country_id = countries[countries['code'] == country_code]['id'].values[0]
            print(f"Country code: {country_code}")
            print(f"Country ID: {country_id}")

            db.insert_initial_transform_log(batch_date, country_id, 'processing')

            with open(file, 'r') as f:
                data = json.load(f)

            if not data:
                print(f"No data found in file: {file}")
            elif isinstance(data, str):
                print(f"Invalid expected data format in file: {file}")
            else:
                covid_data_subset = []
                try:
                    date = data['data']['date']
                    confirmed_cases = data['data']['confirmed_diff']
                    deaths = data['data']['deaths_diff']
                    recovered = data['data']['recovered_diff']

                    covid_data_subset.append(date, confirmed_cases, deaths, recovered)
                    status = 'processed'
                except KeyError as e:
                    print(f"Key error: {e}")
                
                if status == 'processed' and len(covid_data_subset) > 0:
                    print('Ready to insert data into the database!')

                    processed_directory = '../processed/covid_data'
                    processed_file_name = f"_{country_code}_{batch_date}_{status}.json"

                    date, confirmed_cases, deaths, recovered = covid_data_subset

                    new_data = {
                        'date': date,
                        'confirmed_cases': confirmed_cases,
                        'deaths': deaths,
                        'recovered': recovered
                    }

                    save_to_json(new_data, processed_directory, processed_file_name)
                    
                    db.update_transform_log(batch_date, country_id, processed_directory.strip('../'), processed_file_name, len(covid_data_subset), status)
                    db.insert_covid_data(country_id, date, confirmed_cases, deaths, recovered)
                else:
                    print(f"Failed to process data for file: {file}")
                    processed_directory = '../error/covid_data'
                    processed_file_name = f"_{country_code}_{batch_date}_{status}.json"

                    row_count = get_json_row_count('../raw/covid_data', file.split('/')[-1])
                    
                    save_to_json(data, processed_directory, processed_file_name)
                    db.update_transform_log(batch_date, country_id, processed_directory.strip('../'), processed_file_name, row_count, status)
                    
        print('\n')
        
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
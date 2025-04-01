import json, datetime, os

def prepare_weather_params(countries, start_date, end_date):
    weather_params = {
        country["code"]: {
            "latitude": float(country["latitude"]),
            "longitude": float(country["longitude"]),
            "start_date": start_date,
            "end_date": "2023-01-06",
            "daily": ",".join(["weather_code", "temperature_2m_mean", "surface_pressure_mean", "relative_humidity_2m_mean"]),
            "timezone": "Europe/Berlin",
        }
        for _, country in countries.iterrows()
    }
    return weather_params

def prepare_covid_params(countries, date):
    covid_params = {
        country["code"]: {
            "iso": country["code"],
            "date": date,
        }
        for _, country in countries.iterrows()
    }
    return covid_params

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
    print('API import log has been written!')

def get_json_row_count(file_path, country_code, batch_date):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        row_count = sum(1 for date, countries in data.items() if date == batch_date and country_code in countries)
        return row_count
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return 0
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {file_path}")
    return 0

def save_import_log(batch_date, import_log_data, import_directory_name, import_file_name, db):
    countries = import_log_data["countries"]

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

        row_count = get_json_row_count(os.path.join(import_directory_name, import_file_name), countries[i], batch_date)
        # Execute the query with parameterized values
        db.cursor.execute(query, (
            batch_date, countries_id[i], import_directory_name, import_file_name,
            file_created_date, file_last_modified_date, row_count
        ))
    db.connection.commit()
    print('Import log has been successfully written!')
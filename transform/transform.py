from transform.data_transformer import DataTransformer
from common.utils import (
    get_json_row_count, open_file, move_file, list_all_files_from_directory,
    get_weather_description, check_expected_format
)

def process_weather_file(file, countries, db):
    """
    Processes a raw file containg the extracted data from the Weather API.
    For each given file containg weather data, the process follows the scheme:
        1) Checks if the file has the expected format, otherwise a record in the
            transform.transform_log table is added with a NULL batch date and NULL
            country ID. The file is moved to the error directory.
        2) Checks whether the country_code is already present in the extract.country
            table, otherwise a record in the transform.transform_log table is added
            with the batch date but a NULL country ID. The file is moved to the error
            directory.
        3) If the data from the file can be parsed properly, it is inserted in the
            weather_data_import table. Otherwise, the data in the file is untouched.
        4) The file is moved to its corresponding directory depending on its status.

    Args:
        file (str): The complete name of the file to be processed.
        countries (DataFrame): A DataFrame used for validating whether the file contains
            a valid country code from the extract.country table.
        db (DataTransformer object)
    """

    file_name = file.split("/")[-1]
    p_dir_name = "data/error/weather_data/"
    status = "error"

    result = check_expected_format(file)
    if result:
        country_code, batch_date = result
        filtered = countries[countries["code"] == country_code]

        if not filtered.empty:
            country_id = filtered["id"].values[0]
            try:
                log_id = db.insert_initial_transform_log((batch_date, int(country_id), "ongoing"))

                data = open_file(file)
                try:
                    date = data["daily"]["time"][0]
                    weather_code = data["daily"]["weather_code"][0]
                    mean_temperature = data["daily"]["temperature_2m_mean"][0]
                    mean_surface_pressure = data["daily"]["surface_pressure_mean"][0]
                    precipitation_sum = data["daily"]["precipitation_sum"][0]
                    relative_humidity = data["daily"]["relative_humidity_2m_mean"][0]
                    wind_speed = data["daily"]["wind_speed_10m_mean"][0]
                    weather_description = get_weather_description(str(weather_code)) or "Unknown"

                    insert_values = (
                        int(country_id), date, str(weather_code), str(weather_description),
                        float(mean_temperature), float(mean_surface_pressure),
                        float(precipitation_sum), float(relative_humidity), float(wind_speed)
                    )
                    db.insert_weather_data(insert_values)

                    status = "processed"
                    p_dir_name = "data/processed/weather_data/"
                except KeyError:
                    pass

                move_file(file, p_dir_name, file_name)
                row_count = get_json_row_count(p_dir_name, file_name)
                db.update_transform_log((p_dir_name, file_name, row_count, status, log_id))

            except Exception:
                log_id = db.insert_initial_transform_log((batch_date, int(country_id), status))
                move_file(file, p_dir_name, file_name)
                db.update_transform_log((p_dir_name, file_name, 0, status, log_id))
        else:
            log_id = db.insert_initial_transform_log((batch_date, None, status))
            move_file(file, p_dir_name, file_name)
            db.update_transform_log((p_dir_name, file_name, 0, status, log_id))
    else:
        log_id = db.insert_initial_transform_log((None, None, status))
        move_file(file, p_dir_name, file_name)
        db.update_transform_log((p_dir_name, file_name, 0, status, log_id))

def process_covid_file(file, countries, db):
    """
    Processes a raw file containg the extracted data from the COVID-19 API.
    For each given file containg COVID-19 data, the process follows the scheme:
        1) Checks if the file has the expected format, otherwise a record in the
            transform.transform_log table is added with a NULL batch date and NULL
            country ID. The file is moved to the error directory.
        2) Checks whether the country_code is already present in the extract.country
            table, otherwise a record in the transform.transform_log table is added
            with the batch date but a NULL country ID. The file is moved to the error
            directory.
        3) If the data from the file can be parsed properly, it is inserted in the
            covid_data_import table. Otherwise, the data in the file is untouched.
        4) The file is moved to its corresponding directory depending on its status.

    Args:
        file (str): The complete name of the file to be processed.
        countries (DataFrame): A DataFrame used for validating whether the file contains
            a valid country code from the extract.country table.
        db (DataTransformer object)
    """

    file_name = file.split("/")[-1]
    p_dir_name = "data/error/covid_data/"
    status = "error"

    result = check_expected_format(file)
    if result:
        country_code, batch_date = result
        filtered = countries[countries["code"] == country_code]

        if not filtered.empty:
            country_id = filtered["id"].values[0]
            try:
                log_id = db.insert_initial_transform_log((batch_date, int(country_id), "ongoing"))

                data = open_file(file)
                try:
                    date = data["data"]["date"]
                    confirmed_cases = data["data"]["confirmed_diff"]
                    deaths = data["data"]["deaths_diff"]
                    recovered = data["data"]["recovered_diff"]

                    insert_values = (int(country_id), date, int(confirmed_cases),
                                     int(deaths), int(recovered))
                    db.insert_covid_data(insert_values)

                    status = "processed"
                    p_dir_name = "data/processed/covid_data/"
                except KeyError:
                    pass

                move_file(file, p_dir_name, file_name)
                row_count = get_json_row_count(p_dir_name, file_name)
                db.update_transform_log((p_dir_name, file_name, row_count, status, log_id))

            except Exception:
                log_id = db.insert_initial_transform_log((batch_date, int(country_id), status))
                move_file(file, p_dir_name, file_name)
                db.update_transform_log((p_dir_name, file_name, 0, status, log_id))
        else:
            log_id = db.insert_initial_transform_log((batch_date, None, status))
            move_file(file, p_dir_name, file_name)
            db.update_transform_log((p_dir_name, file_name, 0, status, log_id))
    else:
        log_id = db.insert_initial_transform_log((None, None, status))
        move_file(file, p_dir_name, file_name)
        db.update_transform_log((p_dir_name, file_name, 0, status, log_id))

def t_routine(countries, db: DataTransformer):
    """
    Attempts to complete the transform part of the ETL.
    The process follows the scheme:
        1) The files are listed from the raw directories associated with
            each both weather and COVID-19 data.
        2) The two tables weather_data_import and covid_data_import from
            the transform schema are truncated.
        3) For each kind of file, the file name is analysed, and processed
            according to the logic specified in the process functions above.

    Args:
        countries (DataFrame): DataFrame created based on the extract.country table.
        db (DataTransformer object)
    """

    files_weather = list_all_files_from_directory("data/raw/weather_data")
    files_covid = list_all_files_from_directory("data/raw/covid_data")

    db.truncate_table("transform.weather_data_import")
    db.truncate_table("transform.covid_data_import")

    for file in files_weather:
        process_weather_file(file, countries, db)

    for file in files_covid:
        process_covid_file(file, countries, db)

    db.close_connection()

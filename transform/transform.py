from transform.data_transformer import DataTransformer
from transform.validation import WeatherData, CovidData
from common.utils import (
    open_file, move_file, list_all_files_from_directory,
    get_weather_description, check_expected_format
)
from pydantic import ValidationError

def process_weather_file(file, countries, db:DataTransformer):
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
            log_id = db.insert_initial_transform_log((batch_date, int(country_id), "ongoing"))
            try:
                raw_data = open_file(file)
                parsed_data = WeatherData(**raw_data)
                daily = parsed_data.daily
                weather_description = get_weather_description(daily.weather_code[0]) or "Unknown"

                insert_values = (
                    int(country_id), daily.time[0], daily.weather_code[0], weather_description,
                    daily.temperature_2m_mean[0], daily.surface_pressure_mean[0],
                    daily.precipitation_sum[0], daily.relative_humidity_2m_mean[0],
                    daily.wind_speed_10m_mean[0]
                )
                db.insert_weather_data(insert_values)

                status = "processed"
                p_dir_name = "data/processed/weather_data/"
            except (ValidationError, KeyError, TypeError) as e:
                db.logger.warning(f"Transformation has failed for weather data belonging to \
                                   {country_id}: {e}")
                pass

            move_file(file, p_dir_name, file_name)
            db.update_transform_log((p_dir_name, file_name, 1, status, log_id))
        else:
            log_id = db.insert_initial_transform_log((batch_date, None, status))
            move_file(file, p_dir_name, file_name)
            db.update_transform_log((p_dir_name, file_name, 0, status, log_id))
    else:
        log_id = db.insert_initial_transform_log((None, None, status))
        move_file(file, p_dir_name, file_name)
        db.update_transform_log((p_dir_name, file_name, 0, status, log_id))

def process_covid_file(file, countries, db:DataTransformer):
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
            log_id = db.insert_initial_transform_log((batch_date, int(country_id), "ongoing"))

            try:
                raw_data = open_file(file)
                parsed_data = CovidData(**raw_data)
                data = parsed_data.data

                insert_values = (int(country_id), data.date, data.confirmed_diff,
                                    data.deaths_diff, data.recovered_diff)
                db.insert_covid_data(insert_values)

                status = "processed"
                p_dir_name = "data/processed/covid_data/"
            except (ValidationError, KeyError, TypeError) as e:
                db.logger.warning(f"Transformation has failed for COVID data belonging to \
                                {country_id}: {e}")
                pass

            move_file(file, p_dir_name, file_name)
            db.update_transform_log((p_dir_name, file_name, 1, status, log_id))
            
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

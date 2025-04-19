from transform.data_transformer import DataTransformer
from common.utils import (
    get_json_row_count, open_file, move_file, list_all_files_from_directory,
    get_file_details, get_weather_description
)

def process_weather_file(file, country_code, batch_date, countries, db):
    """
    Processes a raw file containg the extracted data from the Weather API.
    For each given file containg weather data, the process follows the scheme:
        1) Checks if it is a valid file containing a country code already
            present in the extract.country table. Otherwise is moved straight
            to the error directory associated with the weather data.
        2) If it is validated, an initial uncomplete record in the transform_log
            table is added.
        3) If the data from the file can be parsed properly, it is inserted in the
            weather_data_import table. Otherwise, the data in the file is untouched.
        4) The file is moved to its corresponding directory depending on its status.
        5) The initial umcomplete record in the transform_log table is updated
            with the additional information.

    Args:
        file (str): The complete name of the file to be processed.
        country_code (str): The country code extracted from the file name.
        batch_date (str): The batch date extracted from the file name.
        countries (DataFrame): A DataFrame used for validating whether the file contains
            a valid country code from the extract.country table.
        db (DataTransformer object)
    """

    file_name = file.split("/")[-1]
    status = "error"
    p_dir_name = "data/error/weather_data/"

    if country_code in countries["code"].values:
        country_id = countries[countries["code"] == country_code]["id"].values[0]

        try:
            log_values = (batch_date, int(country_id), "processing")
            db.insert_initial_transform_log(log_values)

            data = open_file(file)
            try:
                date = data["daily"]["time"][0]
                weather_code = data["daily"]["weather_code"][0]
                mean_temperature = data["daily"]["temperature_2m_mean"][0]
                mean_surface_pressure = data["daily"]["surface_pressure_mean"][0]
                precipitation_sum = data["daily"]["precipitation_sum"][0]
                relative_humidity = data["daily"]["relative_humidity_2m_mean"][0]
                wind_speed = data["daily"]["wind_speed_10m_mean"][0]
                weather_description = get_weather_description(str(weather_code))
                if not weather_description:
                    weather_description = "Unknown"

                insert_values = (int(country_id), date, str(weather_code),
                                 str(weather_description),float(mean_temperature),
                                 float(mean_surface_pressure), float(precipitation_sum),
                                 float(relative_humidity),float(wind_speed))
                db.insert_weather_data(insert_values)

                status = "processed"
                p_dir_name = "data/processed/weather_data/"
            except KeyError:
                pass

            move_file(file, p_dir_name, file_name)

            row_count = get_json_row_count(p_dir_name, file_name)
            update_values = (batch_date, int(country_id), p_dir_name,
                             file_name, row_count, status)
            db.update_transform_log(update_values)

        except Exception:
            move_file(file, p_dir_name, file_name)
            db.rollback_transaction()

def process_covid_file(file, country_code, batch_date, countries, db):
    """
    Processes a raw file containg the extracted data from the COVID-19 API.
    For each given file containg weather data, the process follows the scheme:
        1) Checks if it is a valid file containing a country code already
            present in the extract.country table. Otherwise is moved straight
            to the error directory associated with the COVID-19 data.
        2) If it is validated, an initial uncomplete record in the transform_log
            table is added.
        3) If the data from the file can be parsed properly, it is inserted in the
            weather_data_import table. Otherwise, the data in the file is untouched.
        4) The file is moved to its corresponding directory depending on its status.
        5) The initial umcomplete record in the transform_log table is updated
            with the additional information.

    Args:
        file (str): The complete name of the file to be processed.
        country_code (str): The country code extracted from the file name.
        batch_date (str): The batch date extracted from the file name.
        countries (DataFrame): A DataFrame used for validating whether the file contains
            a valid country code from the extract.country table.
        db (DataTransformer object)
    """

    status = "error"
    file_name = file.split("/")[-1]
    p_dir_name = "data/error/covid_data/"

    if country_code in countries["code"].values:
        country_id = countries[countries["code"] == country_code]["id"].values[0]

        try:
            log_values = (batch_date, int(country_id), "processing")
            db.insert_initial_transform_log(log_values)

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
            update_values = (batch_date, int(country_id), p_dir_name,
                             file_name, row_count, status)
            db.update_transform_log(update_values)

        except Exception:
            move_file(file, p_dir_name, file_name)
            db.rollback_transaction()

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
        country_code, batch_date = get_file_details(file)
        if all([country_code, batch_date]):
            process_weather_file(file, country_code, batch_date, countries, db)

    for file in files_covid:
        country_code, batch_date = get_file_details(file)
        if all([country_code, batch_date]):
            process_covid_file(file, country_code, batch_date, countries, db)

    db.close_connection()

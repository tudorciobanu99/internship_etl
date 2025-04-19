from extract.data_extractor import DataExtractor
from extract.covid_api import CovidAPI
from extract.weather_api import WeatherAPI
from common.utils import save_to_json, get_json_row_count, today

def e_routine(w_api:WeatherAPI, c_api:CovidAPI, db:DataExtractor, countries, date, batch_date):
    """
    Attempts to complete the extract part of the ETL.
    For each given country and API, the process follows the scheme:
        1) An API call is made.
        2) An initial API import log record is created.
        3) The API response is extracted.
        4) The initial API import log record is updated based on the response.
        5) An initial import log record is created.
        6) The response body is saved to a .json file.
        7) THe initial import log record is updated accordingly.

    Args:
        w_api (WeatherAPI object)
        c_api (CovidAPI object)
        db (DataExtractor object)
        countries (DataFrame): DataFrame created based on the extract.country table.
        date (str): A given date for extraction.
        batch_date (str): Generally the date at which the extraction occurs.
    """

    w_imp_dir_name = "data/raw/weather_data"
    c_imp_dir_name = "data/raw/covid_data"
    w_imp_file_name = "weather_data"
    c_imp_file_name = "covid_data"

    file_created_date = today()
    file_last_modified_date = today()

    try:
        for _, country in countries.iterrows():
            latitude, longitude = country["latitude"], country["longitude"]
            response, start_time = w_api.send_request(latitude, longitude, date)
            db.insert_initial_api_import_log((int(country["id"]), int(w_api.api_id), start_time))
            end_time, code_resp, error_message, resp_body = w_api.get_response(response)
            api_params = (int(country["id"]), int(w_api.api_id),
                          start_time, end_time, code_resp, error_message)
            db.update_api_import_log(api_params)

            w_file_name = w_imp_file_name + "_" + country["code"] + "_" + batch_date + ".json"
            db.insert_initial_import_log((batch_date, int(country["id"]),
                                          w_imp_dir_name, w_file_name))
            save_to_json(resp_body, w_imp_dir_name, w_file_name)
            w_row_count = get_json_row_count(w_imp_dir_name, w_file_name)
            import_params = (batch_date, int(country["id"]), w_imp_dir_name, w_file_name,
                             file_created_date, file_last_modified_date, w_row_count)
            db.update_import_log(import_params)

            response, start_time = c_api.send_request(country["code"], date)
            db.insert_initial_api_import_log((int(country["id"]), int(c_api.api_id), start_time))
            end_time, code_resp, error_message, resp_body = c_api.get_response(response)
            api_params = (int(country["id"]), int(c_api.api_id),
                          start_time, end_time, code_resp, error_message)
            db.update_api_import_log(api_params)

            c_file_name = c_imp_file_name + "_" + country["code"] + "_" + batch_date + ".json"
            db.insert_initial_import_log((batch_date, int(country["id"]),
                                          c_imp_dir_name, c_file_name))
            save_to_json(resp_body, c_imp_dir_name, c_file_name)
            c_row_count = get_json_row_count(c_imp_dir_name, c_file_name)
            import_params = (batch_date, int(country["id"]), c_imp_dir_name, c_file_name,
                             file_created_date, file_last_modified_date, c_row_count)
            db.update_import_log(import_params)
    except Exception:
        db.rollback_transaction()

    db.close_connection()

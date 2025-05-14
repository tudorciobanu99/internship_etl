from extract.data_extractor import DataExtractor
from extract.covid_api import CovidAPI
from extract.weather_api import WeatherAPI
from common.utils import save_to_json, today, get_row_count, is_valid_date

def e_routine(w_api:WeatherAPI, c_api:CovidAPI, db:DataExtractor, countries, date):
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
    """

    W_IMP_DIRNAME = "data/raw/weather_data"
    C_IMP_DIRNAME = "data/raw/covid_data"

    file_created_date = today()
    file_last_modified_date = today()
    
    for _, country in countries.iterrows():
        if not is_valid_date(date):
            print(f"Invalid date: {date} for country {country['code']}. Skipping imports for this country.")
            continue
        
        try:
            api_log_id = db.insert_initial_api_import_log((int(country["id"]), int(w_api.api_id)))

            latitude, longitude = country["latitude"], country["longitude"]
            response, start_time = w_api.send_request(latitude, longitude, date)
            end_time, code_resp, error_message, resp_body = w_api.get_response(response)

            api_params = (start_time, end_time, code_resp, error_message, int(api_log_id))
            db.update_api_import_log(api_params)

            w_filename = "w_" + country["code"] + "_" + date + ".json"
            log_id = db.insert_initial_import_log((date, int(country["id"]),
                                        W_IMP_DIRNAME, w_filename))
            save_to_json(resp_body, W_IMP_DIRNAME, w_filename)
            w_row_count = get_row_count(W_IMP_DIRNAME, w_filename, code_resp, "w")
            import_params = (W_IMP_DIRNAME, w_filename, file_created_date,
                            file_last_modified_date, w_row_count, int(log_id))
            db.update_import_log(import_params)

            api_log_id = db.insert_initial_api_import_log((int(country["id"]), int(c_api.api_id)))

            response, start_time = c_api.send_request(country["code"], date)
            end_time, code_resp, error_message, resp_body = c_api.get_response(response)

            api_params = (start_time, end_time, code_resp, error_message, int(api_log_id))
            db.update_api_import_log(api_params)

            c_file_name = "c_" + country["code"] + "_" + date + ".json"
            log_id = db.insert_initial_import_log((date, int(country["id"]),
                                        C_IMP_DIRNAME, c_file_name))
            save_to_json(resp_body, C_IMP_DIRNAME, c_file_name)
            c_row_count = get_row_count(C_IMP_DIRNAME, c_file_name, code_resp, "c")
            import_params = (C_IMP_DIRNAME, c_file_name, file_created_date,
                            file_last_modified_date, c_row_count, log_id)
            db.update_import_log(import_params)
        except Exception as e:
            db.logger.warning(f"Error extracting data for country {country['code']}: {str(e)}")
            db.rollback_transaction()

    db.close_connection()

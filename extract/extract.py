from extract.data_extractor import DataExtractor
from extract.covid_api import CovidAPI
from extract.weather_api import WeatherAPI
from common.utils import save_to_json, get_json_row_count, today

def e_routine(w_api:WeatherAPI, c_api:CovidAPI, db:DataExtractor, countries, date, batch_date):
    w_import_dir_name = "data/raw/weather_data"
    c_import_dir_name = "data/raw/covid_data"
    w_import_file_name = "weather_data"
    c_import_file_name = "covid_data"

    file_created_date = today()
    file_last_modified_date = today()

    try:
        for _, country in countries.iterrows():
            response, start_time = w_api.send_request(country, date)
            db.insert_initial_api_import_log(int(country["id"]), int(w_api.api_id), start_time)
            end_time, code_resp, error_message, resp_body = w_api.get_response(response)
            u_api_params = {
                "country_id": int(country["id"]),
                "api_id": int(w_api.api_id),
                "start_time": start_time,
                "end_time": end_time,
                "code_response": code_resp,
                "error_message": error_message
            }
            db.update_api_import_log(**u_api_params)

            w_file_name = w_import_file_name + "_" + country["code"] + "_" + batch_date + ".json"
            i_import_params = {
                "batch_date": batch_date,
                "country_id": int(country["id"]),
                "import_dir_name": w_import_dir_name,
                "import_file_name": w_file_name
            }
            db.insert_initial_import_log(**i_import_params)
            save_to_json(resp_body, w_import_dir_name, w_file_name)
            w_row_count = get_json_row_count(w_import_dir_name, w_file_name)
            u_import_params = {
                "batch_date": batch_date,
                "country_id": int(country["id"]),
                "import_dir_name": w_import_dir_name,
                "import_file_name": w_file_name,
                "file_created_date": file_created_date,
                "file_last_modified_date": file_last_modified_date,
                "row_count": w_row_count
            }
            db.update_import_log(**u_import_params)

            response, start_time = c_api.send_request(country, date)
            db.insert_initial_api_import_log(int(country["id"]), int(c_api.api_id), start_time)
            end_time, code_resp, error_message, resp_body = c_api.get_response(response)
            u_api_params = {
                "country_id": int(country["id"]),
                "api_id": int(c_api.api_id),
                "start_time": start_time,
                "end_time": end_time,
                "code_response": code_resp,
                "error_message": error_message
            }
            db.update_api_import_log(**u_api_params)

            c_file_name = c_import_file_name + "_" + country["code"] + "_" + batch_date + ".json"
            i_import_params = {
                "batch_date": batch_date,
                "country_id": int(country["id"]),
                "import_dir_name": c_import_dir_name,
                "import_file_name": c_file_name
            }
            db.insert_initial_import_log(**i_import_params)
            save_to_json(resp_body, c_import_dir_name, c_file_name)
            c_row_count = get_json_row_count(c_import_dir_name, c_file_name)
            u_import_params = {
                "batch_date": batch_date,
                "country_id": int(country["id"]),
                "import_dir_name": c_import_dir_name,
                "import_file_name": c_file_name,
                "file_created_date": file_created_date,
                "file_last_modified_date": file_last_modified_date,
                "row_count": c_row_count
            }
            db.update_import_log(**u_import_params)
    except Exception:
        db.rollback_transaction()

    db.close_connection()

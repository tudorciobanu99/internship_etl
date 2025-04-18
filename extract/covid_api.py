import requests
from common.utils import timestamp
class CovidAPI:
    def __init__(self, api_id, base_url):
        self.base_url = base_url
        self.api_id = api_id

    def get_endpoint(self, **params):
        query_string = "&".join([f"{key}={value}" for key, value in params.items()])
        return f"{self.base_url}?{query_string}"

    def prepare_covid_params(self, country, date):
        covid_params = {
                "iso": country["code"],
                "date": date,
            }
        return covid_params

    def send_request(self, country, date):
        params = self.prepare_covid_params(country, date)
        complete_url = self.get_endpoint(**params)

        start_time = timestamp()
        try:
            response = requests.get(complete_url, timeout=10)
            return response, start_time
        except requests.exceptions.RequestException:
            return None, start_time

    def get_response(self, response):
        code_response = 404
        error_message = ""
        end_time = ""
        response_body = None

        try:
            json_response = response.json()
            end_time = timestamp()
            code_response = response.status_code
            response_body = json_response

            if isinstance(json_response.get("error"), dict):
                error_message = ', '.join(f"{', '.join(value)}"
                                          for _, value in json_response["error"].items())

        except requests.exceptions.JSONDecodeError:
            error_text = response.text
            end_time = timestamp()
            response_body = error_text
            error_message = "Not found"

        return end_time, code_response, error_message, response_body

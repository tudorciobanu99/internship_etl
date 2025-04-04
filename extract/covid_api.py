import requests, time

class covid_api:
    def __init__(self, api_id, base_url):
        self.base_url = base_url
        self.api_id = api_id
        self.data = {}

    def get_endpoint(self, **params):
        query_string = '&'.join([f'{key}={value}' for key, value in params.items()])
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

        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        try:
            response = requests.get(complete_url)
            return response, start_time
        except requests.exceptions.RequestException as e:
            return None, start_time
    
    def get_response(self, response, country):
        code_response = 404
        error_message = ''
        end_time = ''

        response_body = None 
        
        try:
            json_response = response.json()
            end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            code_response = response.status_code
            response_body = json_response

            if isinstance(json_response.get('error'), dict):
                error_message = ', '.join(f"{', '.join(value)}" for key, value in json_response['error'].items())

            if code_response == 200:
                print(f"Valid response received for country: {country['code']}")
            elif 400 <= code_response < 500:
                print(f"Client error occurred. HTTP Response Code: {code_response}. Error details: {error_message}.")
            elif 500 <= code_response < 600:
                print(f"Server error occurred. HTTP Response Code: {code_response}. Error details: {error_message}.")
            else:
                print(f"Unexpected error occurred. HTTP Response Code: {code_response}. Error details: {error_message}.")
        except Exception as e:
            error_text = response.text
            end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            response_body = error_text
            error_message = "Not Found"

        print(f"Country: {country['code']}, HTTP Response Code: {code_response}, Error Message: {error_message}")
        
        return end_time, code_response, error_message, response_body





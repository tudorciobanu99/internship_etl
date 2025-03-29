# IMPORTS
import requests, json, time, datetime

# WRAPPER CLASS FOR INTERACTING WITH THE APIs
class APIWrapper:
    def __init__(self, api_id, base_url):
        self.base_url = base_url
        self.api_id = api_id
        self.data = {}

    def get_endpoint(self, **params):
        query_string = '&'.join([f'{key}={value}' for key, value in params.items()])
        return f"{self.base_url}?{query_string}"

    def fetch_data(self, countries, fields, **params):
        log_entries = []

        for country in countries:
            complete_url = self.get_endpoint(**params[country])

            start_time = time.time()
            response = requests.get(complete_url)
            x = response.json()

            code_response = response.status_code

            error_message = 'None.'
            reason = x.get("reason", "Reason not provided.")

            if code_response == 200:
                print(f"Country = {country}")
                # print(f"Full API Response: {x}")

                self.data[country] = {}
                
                for field in fields:
                    dir, field = field.split('/')
                    if field in x[dir]:
                        self.data[country][field] = x[dir][field]
                    else:
                        print(f"Field '{field}' not found in the response.")
            elif 400 <= code_response < 500:
                error_message = f"Client error occurred. HTTP Response Code: {code_response}. Error details: {reason}."
                print(error_message)
            elif 500 <= code_response < 600:
                error_message = f"Server error occurred. HTTP Response Code: {code_response}. Error details: {reason}."
                print(error_message)
            else:
                error_message = f"Unexpected error occurred. HTTP Response Code: {code_response}. Error details: {reason}."
                print(error_message)

            end_time = time.time()

            log_entries.append(self.api_import_log(start_time, end_time, self.api_id, '1', code_response, error_message))
        
        self.save_import_log(log_entries)
        self.save_data()

    def api_import_log(self, start_time, end_time, country_id, api_id, code_response, error_message):
        log_entry = {
            "start_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
            "end_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)),
            "country_id": country_id,
            "api_id": api_id,
            "http_response_code": code_response,
            "error_message": error_message
        }
        return log_entry
    
    def save_import_log(self, log_entries):
        now = datetime.datetime.now()
        formatted_time = now.strftime("%Y_%m_%d_%H_%M")
        file_name = f"import_log_{formatted_time}.json"

        with open('logs/' + file_name, 'w') as outfile:
            json.dump(log_entries, outfile, indent=4)

        print("Import log has been saved to " + file_name + "!")


    def save_data(self):
        now = datetime.datetime.now()
        formatted_time = now.strftime("%Y_%m_%d_%H_%M")
        file_name = f"{self.api_id}_{formatted_time}.json"

        with open('raw/' + file_name, 'w') as outfile:
            json.dump(self.data, outfile, indent=4)

        print("Data has been saved to " + file_name + "!")

if __name__ == "__main__":

    country_coordinates = {
        'Moldova': {'latitude': 47.4116, 'longitude': 28.3699},
        'Germany': {'latitude': 52.5200, 'longitude': 13.4050},
        'France': {'latitude': 48.8566, 'longitude': 2.3522}
    }

    weather_api = APIWrapper('1', "https://historical-forecast-api.open-meteo.com/v1/forecast")

    country_params = {
        country: {
            "latitude": coords['latitude'],
            "longitude": coords['longitude'],
            "start_date": "2025-03-13",
            "end_date": "2025-03-13",
            "hourly": ",".join(["temperature_2m", "relative_humidity_2m", "weather_code", "surface_pressure"]),
            "timezone": "Europe/Berlin"
        }
        for country, coords in country_coordinates.items()
    }

    fields = ["hourly/temperature_2m", "hourly/relative_humidity_2m", "hourly/weather_code", "hourly/surface_pressure"]
    weather_api.fetch_data(country_coordinates, fields, **country_params)





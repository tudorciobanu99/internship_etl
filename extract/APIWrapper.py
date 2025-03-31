import requests, json, time, datetime, os

# WRAPPER CLASS FOR INTERACTING WITH THE APIs
class APIWrapper:
    def __init__(self, api_id, base_url):
        self.base_url = base_url
        self.api_id = api_id
        self.data = {}

    def get_endpoint(self, **params):
        query_string = '&'.join([f'{key}={value}' for key, value in params.items()])
        return f"{self.base_url}?{query_string}"

    def fetch_data(self, countries, fields, error_field, **params):
        code_response = []
        error_message = []

        start_time = time.time()

        for country in countries:
            complete_url = self.get_endpoint(**params[country])
            error_msg = ''
            code_resp = 404

            try:
                response = requests.get(complete_url)
                x = response.json()

                code_resp = response.status_code
                if isinstance(x.get(error_field), list):
                    error_msg = ', '.join(x[error_field])
                elif isinstance(x.get(error_field), dict):
                    error_msg = ', '.join(f"{key}: {', '.join(value)}" for key, value in x[error_field].items())
                else:
                    error_msg = x.get(error_field)

                if code_resp == 200:
                    self.data[country] = {}
                
                    for field in fields:
                        dir, field = field.split('/')
                        if field in x[dir]:
                            self.data[country][field] = x[dir][field]
                        else:
                            print(f"Field '{field}' not found in the response.")
                elif 400 <= code_resp < 500:
                    print(f"Client error occurred. HTTP Response Code: {code_response}. Error details: {error_msg}.")
                elif 500 <= code_resp < 600:
                    print(f"Server error occurred. HTTP Response Code: {code_response}. Error details: {error_msg}.")
                else:
                    print(f"Unexpected error occurred. HTTP Response Code: {code_response}. Error details: {error_msg}.")
            except requests.exceptions.RequestException as e:
                error_msg = f"Not Found"
            
            code_response.append(code_resp)
            error_message.append(error_msg or '')
            print(f"Country: {country}, HTTP Response Code: {code_resp}, Error Message: {error_msg}")

        end_time = time.time()

        import_log_data = {
            "start_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
            "end_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)),
            "countries": countries,
            "api_id": self.api_id,
            "response_codes": code_response,
            "error_messages": error_message
        }
        return import_log_data

    def save_data(self, import_directory_name, import_file_name):
        now = datetime.datetime.now()
        formatted_time = now.strftime("%Y_%m_%d_%H_%M")
        
        file_path = os.path.join(import_directory_name, import_file_name)

        try:
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            print(f"File {file_path} not found.")
            existing_data = {}
        
        existing_data.update(self.data)
        with open(file_path, 'w') as outfile:
            json.dump(existing_data, outfile, indent=4)
        
        print(f"Data has been saved to {file_path}!")
        return formatted_time



        
    

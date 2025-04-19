import requests
from common.utils import timestamp
class CovidAPI:
    def __init__(self, api_id, base_url):
        """
        Initializes the CovidAPI object.

        Args:
            api_id (int): The ID of the API.
            base_url (str): The base URL to the API.

        Attributes:
            api_id (int): The ID of the API.
            base_url (str): The base URL to the API.
        """

        self.base_url = base_url
        self.api_id = api_id

    def get_endpoint(self, **params):
        """
        Completes the endpoint by joining the base URL and
            the query parameters.

        Args:
            **params (dict): The relevant query parameters.
                It includes the following keys:
                    iso (str): ISO code for a given country.
                    date (str): A given date.

        Returns:
            endpoint (str): The completed endpoint.
        """

        query_string = "&".join([f"{key}={value}" for key, value in params.items()])
        endpoint = f"{self.base_url}?{query_string}"
        return endpoint

    def prepare_covid_params(self, country, date):
        """
        Prepares the query parameters required to complete
            the endpoint.

        Args:
            country (str): ISO code for a given country.
            date (str): A given date.

        Returns:
            covid_params (dict): A dictionary with the above keys.
        """

        covid_params = {
                "iso": country["code"],
                "date": date,
            }
        return covid_params

    def send_request(self, country, date):
        """
        Sends a request to the complete endpoint.

        Args:
            country (str): ISO code for a given country.
            date (str): A given date.

        Returns:
            response (requests.Response object)
            start_time (str): Timestamp corresponding to
                the time the API request was sent.
        """

        params = self.prepare_covid_params(country, date)
        complete_url = self.get_endpoint(**params)

        start_time = timestamp()
        try:
            response = requests.get(complete_url, timeout=10)
            return response, start_time
        except requests.exceptions.RequestException:
            return None, start_time

    def get_response(self, response):
        """
        Handles the response from the API request.

        Args:
            response (requests.Response object)

        Returns:
            end_time (str): Timestamp corresponding to
                the time the API response was processed.
            code_responde (str): The code response for the
                given request.
            error_message (str): The error message if applicable.
            response_body (str or Promise object): A string or anything
                that can be represented by JSON.
        """

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

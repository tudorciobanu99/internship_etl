import requests
from common.utils import timestamp
class WeatherAPI:
    def __init__(self, api_id, base_url):
        """
        Initializes the WeatherAPI object.

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
                    latitude (float): Latitude of a given country.
                    longitude (float): Longitude of a given country.
                    start_date (str): The start date of interest.
                    end_date (str): The end date of interest.
                    daily (list): List of all parameters that can be
                        extracted daily.
                    timezone (str): Timezone of interest (for consistency).

        Returns:
            endpoint (str): The completed endpoint.
        """

        query_string = "&".join([f"{key}={value}" for key, value in params.items()])
        endpoint = f"{self.base_url}?{query_string}"
        return endpoint

    def prepare_weather_params(self, latitude, longitude, date):
        """
        Prepares the query parameters required to complete
            the endpoint.

        Args:
            latitude (float): The latitude of a given country.
            longitude (float): The longitude of a given country.
            date (str): Date of interest.

        Returns:
            weather_params (dict): A dictionary with the above keys.
                See the get_endpoint(**params) function.
        """

        all_params = ["weather_code", "temperature_2m_mean", "surface_pressure_mean",
                      "relative_humidity_2m_mean", "temperature_2m_max", "temperature_2m_min",
                      "apparent_temperature_max", "apparent_temperature_min", "rain_sum",
                      "showers_sum", "snowfall_sum", "precipitation_sum", "precipitation_hours",
                      "precipitation_probability_max", "sunrise", "sunset", "daylight_duration",
                      "uv_index_max", "sunshine_duration", "uv_index_clear_sky_max",
                      "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
                      "shortwave_radiation_sum", "et0_fao_evapotranspiration",
                      "apparent_temperature_mean", "cape_mean", "cape_max",
                      "cape_min", "cloud_cover_mean", "cloud_cover_max", "cloud_cover_min",
                      "dew_point_2m_mean", "dew_point_2m_max", "dew_point_2m_min",
                      "surface_pressure_max", "surface_pressure_min", "updraft_max",
                      "visibility_mean", "visibility_min", "visibility_max",
                      "winddirection_10m_dominant", "wind_gusts_10m_mean",
                      "wind_speed_10m_mean", "wind_speed_10m_min", "wind_gusts_10m_min",
                      "soil_temperature_7_to_28cm_mean", "soil_temperature_28_to_100cm_mean",
                      "soil_temperature_0_to_7cm_mean", "soil_temperature_0_to_100cm_mean",
                      "soil_moisture_7_to_28cm_mean", "soil_moisture_28_to_100cm_mean",
                      "soil_moisture_0_to_7cm_mean", "soil_moisture_0_to_10cm_mean",
                      "soil_moisture_0_to_100cm_mean", "vapour_pressure_deficit_max",
                      "wet_bulb_temperature_2m_min", "wet_bulb_temperature_2m_max",
                      "wet_bulb_temperature_2m_mean", "pressure_msl_min",
                      "pressure_msl_max", "pressure_msl_mean",
                      "snowfall_water_equivalent_sum", "relative_humidity_2m_min",
                      "relative_humidity_2m_max", "precipitation_probability_min",
                      "precipitation_probability_mean", "leaf_wetness_probability_mean",
                      "growing_degree_days_base_0_limit_50", "et0_fao_evapotranspiration_sum"]
        weather_params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date,
            "end_date": date,
            "daily": ",".join(all_params),
            "timezone": "Europe/Berlin",
        }
        return weather_params

    def send_request(self, latitude, longitude, date):
        """
        Prepares the query parameters required to complete
            the endpoint.

        Args:
            latitude (float): The latitude of a given country.
            longitude (float): The longitude of a given country.
            date (str): Date of interest.

        Returns:
            response (requests.Response object)
            start_time (str): Timestamp corresponding to
                the time the API request was sent.
        """
        params = self.prepare_weather_params(latitude, longitude, date)
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
        error_message = "Not Found"
        end_time = ""
        response_body = None

        try:
            json_response = response.json()
            end_time = timestamp()
            code_response = response.status_code
            response_body = json_response
            error_message = json_response.get('reason') or ""
        except requests.exceptions.JSONDecodeError:
            error_text = response.text
            end_time = timestamp()
            response_body = error_text

        return end_time, code_response, error_message, response_body

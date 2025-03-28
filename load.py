import requests, json, time, datetime, os
from dotenv import load_dotenv

load_dotenv("api_key.env")
api_key = os.getenv('OPENWEATHER_API_KEY')

base_url = 'http://api.openweathermap.org/data/2.5/weather?'

city_names = ['London', 'New York', 'Tokyo']

weather_data = {}
error_messages = []
code_responses = []

start_time = time.time()

for city in city_names:
    complete_url = base_url + "appid=" + api_key + "&q=" + city
    response = requests.get(complete_url)
    x = response.json()

    if x['cod'] != '404':
        y = x['main']
        current_temperature = y['temp']
        current_pressure = y['pressure']
        current_humidity = y['humidity']
        z = x['weather']
        weather_description = z[0]['description']
        print(f"City = {city}")
        print(f"Temperature (Celsius) = {round(current_temperature-273.15, 2)}\n Atmospheric pressure (hPa) = {current_pressure}\n Humidity (%) = {current_humidity}\n Description = {weather_description}\n")
        
        weather_data[city] = {
            "Temperature (Celsius)": round(current_temperature - 273.15, 2),
            "Atmospheric Pressure (hPa)": current_pressure,
            "Humidity (%)": current_humidity,
            "Description": weather_description
        }
        code_responses.append(f"cod key is {x['cod']}. {city} found!\n")
    else:
        print('City Not Found')
        error_messages.append(f"cod key is 404. {city} not found!\n")

end_time = time.time()

now = datetime.datetime.now()
formatted_time = now.strftime("%Y_%m_%d_%H_%M")
file_name = f"current_weather_{formatted_time}.json"

with open('raw/' + file_name, 'w') as outfile:
    json.dump(weather_data, outfile, indent=4)

print("Weather data has been saved to " + file_name + "!")

print(f"API import started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
print(f"API import ended at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
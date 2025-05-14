from pydantic import BaseModel, ConfigDict
from typing import List

class DailyWeatherData(BaseModel):
    time: List[str]
    weather_code: List[int]
    temperature_2m_mean: List[float]
    surface_pressure_mean: List[float]
    precipitation_sum: List[float]
    relative_humidity_2m_mean: List[float]
    wind_speed_10m_mean: List[float]

    model_config = ConfigDict(extra="ignore")

class WeatherData(BaseModel):
    daily: DailyWeatherData

    model_config = ConfigDict(extra="ignore")

class DailyCovidData(BaseModel):
    date: str
    confirmed_diff: int
    deaths_diff: int
    recovered_diff: int

    model_config = ConfigDict(extra="ignore")

class CovidData(BaseModel):
    data: DailyCovidData

    model_config = ConfigDict(extra="ignore")
    
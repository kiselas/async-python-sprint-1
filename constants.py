import logging
from typing import Dict

LOGGING_LEVEL = logging.INFO
PATH_TO_RESPONSES = "responses/"
PATH_TO_OUTPUTS = "outputs/"
OUTPUT_CSV_FILE = "aggregated_weather_data.csv"
TASKS_TIMEOUT = 60
MAX_DATE_FIELDS = 5

PATH_FROM_INPUT = "./../examples/response.json"
PATH_TO_OUTPUT = "./../examples/output.json"

INPUT_FORECAST_PATH = "forecasts"
INPUT_DATE_PATH = "date"

INPUT_HOURS_PATH = "hours"
INPUT_HOUR_PATH = "hour"
INPUT_TEMPERATURE_PATH = "temp"
INPUT_CONDITION_PATH = "condition"
INPUT_DAY_HOURS_START = 9
INPUT_DAY_HOURS_END = 19
INPUT_DAY_SUITABLE_CONDITIONS = [
    "clear",
    "partly-cloudy",
    "cloudy",
    "overcast",
    # "drizzle",
    # "light-rain",
    # "rain",
    # "moderate-rain",
    # "heavy-rain",
    # "continuous-heavy-rain",
    # "showers",
    # "wet-snow",
    # "light-snow",
    # "snow",
    # "snow-showers",
    # "hail",
    # "thunderstorm",
    # "thunderstorm-with-rain",
    # "thunderstorm-with-hail"
]

OUTPUT_RAW_DATA_KEY = "raw_data"
OUTPUT_DAYS_KEY = "days"
DEFAULT_OUTPUT_RESULT: Dict[str, list] = {
    OUTPUT_DAYS_KEY: [],
    # OUTPUT_RAW_DATA_KEY: None,
}

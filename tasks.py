import csv
import json
import logging
import multiprocessing
from collections import defaultdict
from statistics import mean
from typing import Any, Dict, List, Union

from constants import (
    MAX_DATE_FIELDS,
    OUTPUT_CSV_FILE,
    PATH_TO_OUTPUTS,
    PATH_TO_RESPONSES,
)
from exceptions import CreateCSVHeadersError
from external.analyzer import analyze_json, dump_data, load_data
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name

TIME_STARTS_WITH = 9
TIME_ENDS_WITH = 19
AVG_FOR_DAYS = 5

logs_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=logs_format, level=logging.DEBUG, datefmt="%H:%M:%S")

good_weather_types = ["clear", "partly-cloudy", "cloudy"]


class DataFetchingTask:
    """Получаем данные о погоде с API"""

    def __init__(self, city: str):
        self.city = city

    def fetch_weather_data(self):
        """Функция отвечает за сам запрос и
        обработку ошибок во время запроса"""
        url = get_url_by_city_name(self.city)

        try:
            weather_data = YandexWeatherAPI.get_forecasting(url)
        except Exception:
            logging.error(f"Something went wrong"
                          f" with request for city {self.city}")
            return {}

        return weather_data

    def run(self):
        """Получаем данные и кладем в json файл
        в директорию для response"""
        # Получение информации о погоде
        city_weather_data = self.fetch_weather_data()

        # Имена файлов уникальны, нет смысла использовать lock
        with open(f"{PATH_TO_RESPONSES}{self.city}.json", "w") as file:
            # Сериализуем данные в формате JSON и записываем в файл
            json.dump(city_weather_data, file)


class DataCalculationTask(multiprocessing.Process):
    """Достаем нужные нам данные из json, полученных с API"""

    def __init__(self, city: str):
        super().__init__()
        self.city: str = city
        self.input_path: str = f"{PATH_TO_RESPONSES}{self.city}.json"
        self.output_path: str = f"{PATH_TO_OUTPUTS}{self.city}.json"

    def run(self):
        data = load_data(self.input_path)
        data = analyze_json(data)

        dump_data(data, self.output_path)


class DataAggregationTask:
    """Собираем вместе собранные данные для дальнейшего анализа.
    Выысчитываем средние показатели и пишем в csv"""

    def __init__(self, city: str, lock, fieldnames: List[str]):
        self.city: str = city
        self.lock = lock
        self.fieldnames: List[str] = fieldnames

    def write_data_to_csv(self) -> None:
        """Функция формирует структуру csv файла
         и записывает все имеющиеся данные"""
        with open(f"{PATH_TO_OUTPUTS}{self.city}.json") as city_data_file:
            days_data = json.load(city_data_file).get("days", [])

        logging.info(f"Start write aggregated data "
                     f"for city {self.city} to csv")
        with open(OUTPUT_CSV_FILE, "a", newline="") as csv_file:
            try:
                writer = csv.DictWriter(csv_file, self.fieldnames)
                # Запись данных в CSV файл
                tmp_row: Dict[str, Any] = \
                    {"Город/день": self.city,
                     "Температура и осадки": "Температура, среднее"}
                if not days_data:
                    return
                for day_data in days_data:
                    date = day_data["date"]
                    tmp_row[date] = day_data.get("temp_avg", "")

                tmp_row["Среднее"] = self.aggregate_city_avg_tmp(days_data)
                tmp_row["Рейтинг"] = ""

                # блокируем, так как для каждого города
                # должно быть записано две строки подряд
                self.lock.acquire()
                # записываем данные в файл
                writer.writerow(tmp_row)

                precipitations_row: Dict[str, Any] = \
                    {"Город/день": "",
                     "Температура и осадки": "Без осадков, часов"}

                for day_data in days_data:
                    date = day_data["date"]
                    precipitations_row[date] = \
                        day_data.get("relevant_cond_hours", "")

                precipitations_row["Среднее"] = \
                    self.aggregate_city_good_weather_hours(days_data)
                precipitations_row["Рейтинг"] = ""

                writer.writerow(precipitations_row)
                self.lock.release()
            except Exception as e:
                logging.exception(e, exc_info=True)
                if self.lock.locked():
                    self.lock.release()

    @staticmethod
    def aggregate_city_avg_tmp(
            days_data: List[Dict[str, Any]],
    ) -> Union[int, float]:
        """Получаем среднюю температуру за все дни"""
        tmp_list: List[float] = \
            [value["temp_avg"] for value in days_data if value.get("temp_avg")]
        avg_tmp = round(mean(tmp_list), 1) if tmp_list else 0
        return avg_tmp

    @staticmethod
    def aggregate_city_good_weather_hours(
            days_data: List[Dict[str, Any]],
    ) -> Union[int, float]:
        """Получаем среднее колличество часов хорошей погоды за все дни"""
        weather_hours: List[int] = \
            [value["relevant_cond_hours"]
             for value in days_data if value.get("relevant_cond_hours")]
        avg_good_weather_hours = \
            round(mean(weather_hours), 1) if weather_hours else 0
        return avg_good_weather_hours

    @staticmethod
    def make_csv_with_headers(available_cities) -> List[str]:
        """Подготавливаем csv файл для дальнейшего заполнения данными"""
        for city in available_cities:
            path_to_city_data: str = f"{PATH_TO_OUTPUTS}{city}.json"
            with open(path_to_city_data) as file:
                city_data: Dict[str, Any] = json.load(file)
                if not city_data.get("days"):
                    continue
                dates: List[str] = [day_data["date"]
                                    for day_data in city_data["days"]]
                if len(dates) == MAX_DATE_FIELDS:
                    all_dates = dates
                    break
        if not all_dates:
            raise CreateCSVHeadersError("Can't find all dates for CSV headers")

        fieldnames: List[str] = \
            ["Город/день", "Температура и осадки"] + \
            all_dates + \
            ["Среднее", "Рейтинг"]
        with open(OUTPUT_CSV_FILE, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Запись заголовков в CSV файл
            writer.writeheader()
        logging.info("CSV file with headers successfully created")
        return fieldnames


class DataAnalyzingTask:
    """Класс для анализа агреггированных данных.
    Считаем рейтинг, выводим лучшие города для поездки"""

    def __init__(self):
        self.aggregated_data = self.read_csv_file()
        self.cities_rating = self.calculate_rating()

    @staticmethod
    def read_csv_file():
        result = []
        with open(OUTPUT_CSV_FILE) as file:
            reader = csv.DictReader(file)
            for row in reader:
                result.append(row)
        return result

    def calculate_rating(self):
        data_for_rating: defaultdict = defaultdict(dict)
        for i in range(0, len(self.aggregated_data), 2):
            city = self.aggregated_data[i]["Город/день"]
            avg_tmp = self.aggregated_data[i]["Среднее"]
            avg_good_weather = self.aggregated_data[i + 1]["Среднее"]
            data_for_rating[city] = {"avg_tmp": avg_tmp,
                                     "avg_good_weather": avg_good_weather}

        sorted_data = \
            sorted(data_for_rating.items(),
                   key=lambda x: x[1]["avg_tmp"] + x[1]["avg_good_weather"],
                   reverse=True)
        city_rating = {}
        for num, city_data in enumerate(sorted_data, start=1):
            city_rating[city_data[0]] = num

        return city_rating

    def update_csv(self):
        with open(OUTPUT_CSV_FILE) as file:
            reader = csv.DictReader(file)
            data = []
            for row in reader:
                data.append(row)

        for row in data:
            if row["Город/день"]:
                row["Рейтинг"] = self.cities_rating[row["Город/день"]]

        with open(OUTPUT_CSV_FILE, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def print_best_cities(self):
        print("Топ-3 самых благоприятных города для поездки: \n")

        count = 0
        for key in self.cities_rating:
            print(key)
            count += 1
            if count == 3:
                break

    def run(self):
        self.update_csv()
        self.print_best_cities()

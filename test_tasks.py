import multiprocessing
import os
import csv

import pytest

from constants import OUTPUT_CSV_FILE, PATH_TO_OUTPUTS, PATH_TO_RESPONSES
from main import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
)


@pytest.fixture(autouse=True)
def create_directories(tmp_path):
    directory1 = PATH_TO_RESPONSES
    directory2 = PATH_TO_OUTPUTS

    # Создаем директории, если они не существуют
    os.makedirs(directory1, exist_ok=True)
    os.makedirs(directory2, exist_ok=True)


def test_data_fetching_task():
    city = "MOSCOW"
    task = DataFetchingTask(city)
    task.run()
    assert os.path.exists(f"{PATH_TO_RESPONSES}{city}.json")
    os.remove(f"{PATH_TO_RESPONSES}{city}.json")


def test_data_calculation_task():
    city = "MOSCOW"
    task = DataFetchingTask(city)
    task.run()
    task = DataCalculationTask(city)
    task.run()

    assert os.path.exists(f"{PATH_TO_OUTPUTS}")
    os.remove(f"{PATH_TO_OUTPUTS}{city}.json")
    os.remove(f"{PATH_TO_RESPONSES}{city}.json")


def test_data_aggregation_task():
    city = "MOSCOW"
    task = DataFetchingTask(city)
    task.run()
    task = DataCalculationTask(city)
    task.run()
    lock = multiprocessing.Lock()
    fieldnames = DataAggregationTask.make_csv_with_headers([city])
    task = DataAggregationTask(city, lock, fieldnames)
    task.write_data_to_csv()
    assert os.path.exists(f"{PATH_TO_OUTPUTS}{city}.json")
    with open(OUTPUT_CSV_FILE, "r") as file:
        reader = csv.reader(file)
        rows = [row for row in reader]
    assert rows[-2][0] == city  # Проверка последней записи в CSV
    os.remove(f"{PATH_TO_OUTPUTS}{city}.json")
    os.remove(f"{PATH_TO_RESPONSES}{city}.json")
    os.remove(OUTPUT_CSV_FILE)


def test_aggregate_city_avg_tmp():
    days_data = [
        {"temp_avg": 10},
        {"temp_avg": 15},
        {"temp_avg": 20},
    ]
    avg_tmp = DataAggregationTask.aggregate_city_avg_tmp(days_data)
    assert avg_tmp == 15


def test_aggregate_city_avg_tmp_empty():
    days_data = []
    avg_tmp = DataAggregationTask.aggregate_city_avg_tmp(days_data)
    assert avg_tmp == 0


def test_aggregate_city_good_weather_hours():
    days_data = [
        {"relevant_cond_hours": 5},
        {"relevant_cond_hours": 7},
        {"relevant_cond_hours": 3},
    ]
    avg_good_weather_hours = \
        DataAggregationTask.aggregate_city_good_weather_hours(days_data)
    assert avg_good_weather_hours == 5


def test_aggregate_city_good_weather_hours_empty():
    days_data = []
    avg_good_weather_hours = \
        DataAggregationTask.aggregate_city_good_weather_hours(days_data)
    assert avg_good_weather_hours == 0

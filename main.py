import logging
import multiprocessing
import os
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)

from constants import LOGGING_LEVEL, MAX_THREAD_WORKERS, \
    TASKS_TIMEOUT, PATH_TO_RESPONSES, PATH_TO_OUTPUTS
from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES

cities = CITIES.keys()


def get_available_cities():
    """Извлекаем список городов, по которым удалось получить данные"""
    file_list = os.listdir("responses")
    available_cities = [os.path.splitext(file)[0] for file in file_list]
    return available_cities


if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=LOGGING_LEVEL, datefmt="%H:%M:%S")
    logging.debug(f"All cities list: {cities}")

    if not os.path.isdir(PATH_TO_RESPONSES):
        os.mkdir(PATH_TO_RESPONSES)
    if not os.path.isdir(PATH_TO_OUTPUTS):
        os.mkdir(PATH_TO_OUTPUTS)

    # создаем список классов, которые в отдельных потоках получат данные с API
    task_list = [DataFetchingTask(city) for city in cities]
    with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as pool:
        futures = [pool.submit(lambda task: task.run(), task)
                   for task in task_list]
        done, not_done = wait(futures, timeout=TASKS_TIMEOUT)
        [future.cancel() for future in not_done]

    logging.info("Got data for cities from API")
    logging.info("Start DataCalculationTask for each available city")

    available_cities = get_available_cities()

    process_task_list = []
    for city in available_cities:
        # подготавливаем список задач
        task = DataCalculationTask(city)
        process_task_list.append(task)

    for task in process_task_list:
        task.start()
    for task in process_task_list:
        task.join()

    logging.info("All DataCalculationTask successfully finished")

    fieldnames = DataAggregationTask.make_csv_with_headers(available_cities)

    manager = multiprocessing.Manager()
    file_lock = manager.Lock()

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(DataAggregationTask(
                city,
                file_lock,
                fieldnames,
            ).write_data_to_csv)
            for city in available_cities
        ]
        done, not_done = wait(futures)
        [future.cancel() for future in not_done]

    logging.info("Start data analyzing task")

    DataAnalyzingTask().run()

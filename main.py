from logger import logger
import multiprocessing
import os
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)

from constants import TASKS_TIMEOUT, PATH_TO_RESPONSES, PATH_TO_OUTPUTS
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
    return [os.path.splitext(file)[0] for file in file_list]


if __name__ == "__main__":
    logger.debug(f"All cities list: {cities}")

    if not os.path.isdir(PATH_TO_RESPONSES):
        os.mkdir(PATH_TO_RESPONSES)
    if not os.path.isdir(PATH_TO_OUTPUTS):
        os.mkdir(PATH_TO_OUTPUTS)

    # создаем список классов, которые в отдельных потоках получат данные с API
    task_list = [DataFetchingTask(city) for city in cities]
    with ThreadPoolExecutor() as pool:
        futures = [pool.submit(lambda task: task.run(), task) for task in task_list]
        done, not_done = wait(futures, timeout=TASKS_TIMEOUT)
        [future.cancel() for future in not_done]

    logger.info("Got data for cities from API")
    logger.info("Start DataCalculationTask for each available city")

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

    logger.info("All DataCalculationTask successfully finished")

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

    logger.info("Start data analyzing task")

    DataAnalyzingTask().run()

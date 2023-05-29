import json
from http import HTTPStatus
from urllib.request import urlopen

from exceptions import RequestAPIError, JSONDecodeError

ERR_MESSAGE_TEMPLATE = "Unexpected error: {error}"


class YandexWeatherAPI:
    """
    Base class for requests
    """
    @staticmethod
    def __do_req(url: str) -> str:
        """Base request method"""
        try:
            with urlopen(url) as response:
                resp_body = response.read().decode("utf-8")
                data = json.loads(resp_body)
            if response.status != HTTPStatus.OK:
                raise RequestAPIError(
                    "Error during execute request. {}: {}".format(
                        resp_body.status, resp_body.reason,
                    ),
                )
            return data
        except json.JSONDecodeError as ex:
            raise JSONDecodeError(ERR_MESSAGE_TEMPLATE.format(error=ex))
        except RequestAPIError as ex:
            raise RequestAPIError(ERR_MESSAGE_TEMPLATE.format(error=ex))

    @staticmethod
    def get_forecasting(url: str):
        """
        :param url: url_to_json_data as str
        :return: response data as json
        """
        return YandexWeatherAPI.__do_req(url)

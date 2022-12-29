import datetime
import time
import typing
import urllib.parse

import pendulum
import requests
import singer

LOGGER = singer.get_logger()
BASE_URL = "https://ssapi.shipstation.com/"
TIMEZONE = "America/Los_Angeles"
PAGE_SIZE = 100


def prepare_datetime(dt: datetime.datetime) -> str:
    # ShipStation requests must be in Pacific timezone
    timezone = pendulum.timezone(TIMEZONE)  # type: ignore
    converted = timezone.convert(dt).strftime("%Y-%m-%d %H:%M:%S")
    return converted


class ShipStationClient:
    def __init__(self, config):
        self.username = config["api_key"]
        self.password = config["api_secret"]

    def make_request(self, url: str, params: dict) -> requests.Response:
        LOGGER.info("Making request to %s with query parameters %s", url, params)
        response = requests.get(url, params=params, auth=(self.username, self.password))
        return response

    @staticmethod
    def rate_limit(response: requests.Response):
        # Respect API rate limits
        if int(response.headers["X-Rate-Limit-Remaining"]) < 1:
            # Buffer of 1 second
            wait_seconds = int(response.headers["X-Rate-Limit-Reset"]) + 1
            LOGGER.info(
                "Waiting for %s seconds to respect ShipStation's API rate limit.",
                wait_seconds,
            )
            time.sleep(wait_seconds)

    def handle_response_codes(self, response: requests.Response) -> bool:
        if response.status_code == 200:
            return True
        if response.status_code == 429:
            time.sleep(60)
            return False
        else:
            response.raise_for_status()
            raise Exception(f"{response.status_code}: {response.reason}")

    def fetch_endpoint(self, endpoint: str, params: dict) -> dict:
        while True:
            url = urllib.parse.urljoin(BASE_URL, endpoint)
            response = self.make_request(url, params)
            response_is_valid = self.handle_response_codes(response)
            if response_is_valid:
                self.rate_limit(response)
                return response.json()

    def paginate(self, endpoint: str, params: dict) -> typing.Iterator[dict]:
        url = urllib.parse.urljoin(BASE_URL, endpoint)

        if "page" not in params:
            params["page"] = 1
        if "pageSize" not in params:
            params["pageSize"] = PAGE_SIZE

        while True:
            response = self.make_request(url, params)
            response_is_valid = self.handle_response_codes(response)
            if not response_is_valid:
                continue

            self.rate_limit(
                response
            )  # This needs to be done before any break statement
            response_json = response.json()
            if response_json["total"] == 0:
                LOGGER.info("No Data for endpoint")
                break

            yield response_json[endpoint]

            LOGGER.info(
                "Finished requesting page %s out of %s total pages.",
                response_json["page"],
                response_json["pages"],
            )

            if response_json["page"] >= response_json["pages"]:
                break

            params["page"] += 1

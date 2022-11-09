import requests


def assert_http_ok(response: requests.Response):
    if response.status_code != 200:
        if not response.content:
            raise Exception(f"Http request failed with status {response.status_code} and no content")
        else:
            raise Exception(
                f"Http request failed with status {response.status_code} and content:\n{response.json()}")

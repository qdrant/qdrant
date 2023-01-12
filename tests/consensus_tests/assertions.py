import requests


def assert_http_ok(response: requests.Response):
    if response.status_code != 200:
        base_msg = f"HTTP request to {response.url} failed with status code {response.status_code} after {response.elapsed.total_seconds()}s"
        if not response.content:
            raise Exception(f"{base_msg} and without response body")
        else:
            raise Exception(
                f"{base_msg} with response body:\n{response.json()}")

import itertools

import requests


def assert_http_ok(response: requests.Response):
    if response.status_code != 200:
        base_msg = f"HTTP request to {response.url} failed with status code {response.status_code} after {response.elapsed.total_seconds()}s"
        if not response.content:
            raise Exception(f"{base_msg} and without response body")
        else:
            raise Exception(
                f"{base_msg} with response body:\n{response.json()}")


def assert_hw_measurements_equal(left: dict[str, int], right: dict[str, int]):
    keys = set([key for key in itertools.chain(left.keys(), right.keys())])
    for key in keys:
        if key in left and left[key] > 0:
            assert right.get(key) == left[key]

        if key in right and right[key] > 0:
            assert left.get(key) == right[key]


def assert_hw_measurements_equal_many(left_list: list[dict[str, int]], right_list: list[dict[str, int]]):
    for left,right in zip(left_list, right_list):
        assert_hw_measurements_equal(left, right)

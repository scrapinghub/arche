from typing import Dict, List, Optional

from arche.readers.items import CollectionItems, JobItems
from arche.rules.result import Result
import numpy as np
import pandas as pd
import pytest


cloud_items = [
    {"_key": "112358/13/21/0", "price": 0, "name": "Elizabeth"},
    {"_key": "112358/13/21/1", "name": "Margaret"},
    {"_key": "112358/13/21/2", "price": 10, "name": "Yulia"},
    {"_key": "112358/13/21/3", "price": 11, "name": "Vivien"},
]
default_schema = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "required": ["_key", "name"],
    "properties": {
        "_key": {"type": "string"},
        "price": {"type": "integer"},
        "name": {"type": "string"},
    },
    "additionalProperties": False,
}


@pytest.fixture(scope="session")
def get_cloud_items(request):
    return cloud_items


@pytest.fixture(scope="session")
def get_raw_items(request):
    return np.array(cloud_items)


@pytest.fixture(scope="session")
def get_schema():
    return default_schema


@pytest.fixture(scope="function")
def get_df():
    df = pd.DataFrame({"first": [0.25, 0.75], "second": [0.0, 1.0]})
    df.name = "a df"
    return df


class Job:
    def __init__(self, items=None, metadata=None, stats=None, key="112358/13/21"):
        self.items = Source(items, stats)
        self.key = key
        if metadata:
            self.metadata = metadata
        else:
            self.metadata = {}


class Collection:
    def __init__(self, count: Optional[int] = None):
        self._count = count

    def count(self) -> Optional[int]:
        return self._count


class Source:
    def __init__(self, items=None, stats=None):
        self.items = items
        if stats:
            self._stats = stats
        else:
            if self.items:
                input_values = len(self.items)
            else:
                input_values = 0
            self._stats = {"totals": {"input_values": input_values}}

    def stats(self):
        return self._stats

    def iter(self, **kwargs):
        start = kwargs.get("start", 0)
        if start:
            start = int(start.split("/")[-1])
        count = kwargs.get("count", len(self.items) - start)

        # Scrapinghub API returns all posible items even if `count` greater than possible
        if start + count > len(self.items):
            limit = len(self.items)
        else:
            limit = start + count

        if kwargs.get("filter"):
            field_name = kwargs.get("filter")[0][0]
            value = kwargs.get("filter")[0][1][0]
            filtered_items = []

            counter = 0
            for index in range(start, limit):
                if counter == limit:
                    return
                if self.items[index].get(field_name) == value:
                    filtered_items.append(self.items[index])
                    counter += 1

            for filtered_item in filtered_items:
                yield filtered_item
        else:
            for index in range(start, limit):
                yield self.items[index]


@pytest.fixture(scope="function")
def get_source():
    return Source(items=cloud_items)


@pytest.fixture(scope="function", params=[(cloud_items, None, None)])
def get_job(request):
    return Job(*request.param)


@pytest.fixture(scope="function", params=[(cloud_items)])
def get_collection(request):
    return Collection(*request.param)


@pytest.fixture(scope="function")
def get_jobs():
    return Job(), Job()


class ScrapinghubClient:
    def __init__(self, job: Optional[Job] = get_job):
        self._job = job

    def get_job(self):
        return self._job


@pytest.fixture(scope="function")
def get_client():
    return ScrapinghubClient()


@pytest.fixture(scope="function", params=[cloud_items])
def get_job_items(request, mocker):
    mocker.patch(
        "arche.readers.items.JobItems.job", return_value=get_job, autospec=True
    )
    mocker.patch(
        "arche.readers.items.JobItems.fetch_data",
        return_value=np.array(request.param),
        autospec=True,
    )

    job_items = JobItems(key="112358/13/21", count=len(request.param))
    return job_items


@pytest.fixture(scope="function", params=[cloud_items])
def get_collection_items(request, mocker):
    mocker.patch(
        "arche.tools.api.get_collection", return_value=get_collection, autospec=True
    )
    mocker.patch(
        "arche.readers.items.CollectionItems.fetch_data",
        return_value=np.array(request.param),
        autospec=True,
    )

    collection_items = CollectionItems(
        key="112358/collections/s/pages", count=len(request.param)
    )
    return collection_items


def create_result(
    rule_name,
    messages,
    stats=None,
    err_items_count=None,
    checked_fields=None,
    items_count=None,
):
    result = Result(rule_name)
    for level, messages in messages.items():
        for message in messages:
            result.add_message(level, *message)

    if stats:
        result.stats = stats
    if err_items_count:
        result.err_items_count = err_items_count
    if checked_fields:
        result.checked_fields = checked_fields
    if items_count:
        result.items_count = items_count
    return result


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, Result) and isinstance(right, Result) and op == "==":
        assert_msgs = ["Results are equal"]
        for (left_n, left_v), (_, right_v) in zip(
            left.__dict__.items(), right.__dict__.items()
        ):
            if left_n == "_stats":
                for left_stat, right_stat in zip(left_v, right_v):
                    if not Result.tensors_equal(left_stat, right_stat):
                        assert_msgs.extend([f"{left_stat}", "!=", f"{right_stat}"])
            elif left_v != right_v:
                assert_msgs.extend([f"{left_v}", "!=", f"{right_v}"])
        return assert_msgs


def create_named_df(data: Dict, index: List[str], name: str) -> pd.DataFrame:
    df = pd.DataFrame(data, index=index)
    df.name = name
    return df

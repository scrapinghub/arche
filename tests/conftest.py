from copy import deepcopy
from itertools import zip_longest
from typing import Dict, List, Optional

from arche.readers.items import CollectionItems, JobItems
from arche.rules.result import Result
import numpy as np
import pandas as pd
import pytest


CLOUD_ITEMS = [
    {"_key": "112358/13/21/0", "_type": "Type", "price": 0, "name": "Elizabeth"},
    {"_key": "112358/13/21/1", "_type": "Type", "name": "Margaret"},
    {"_key": "112358/13/21/2", "_type": "Type", "price": 10, "name": "Yulia"},
    {"_key": "112358/13/21/3", "_type": "Type", "price": 11, "name": "Vivien"},
]

DEFAULT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "required": ["name"],
    "type": "object",
    "properties": {"price": {"type": "integer"}, "name": {"type": "string"}},
    "additionalProperties": False,
}


@pytest.fixture(scope="session")
def get_cloud_items(request):
    return CLOUD_ITEMS


@pytest.fixture(scope="session")
def get_raw_items(request):
    return np.array(CLOUD_ITEMS)


@pytest.fixture(scope="session")
def get_schema():
    return DEFAULT_SCHEMA


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


def _is_filtered(x, by):
    if by:
        return x.get(by[0][0]) == by[0][1][0]
    return True


class Source:
    def __init__(
        self, items: Optional[List[Dict]] = None, stats: Optional[Dict] = None
    ):
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
        count = kwargs.get("count", None)
        counter = 0
        if start:
            start = int(start.split("/")[-1])

        for item in self.items[start:]:
            if counter == count:
                return
            if _is_filtered(item, kwargs.get("filter")):
                counter += 1
                yield item


class StoreSource:
    def __init__(self, items: List[Dict] = None):
        self.items = items

    def count(self):
        return len(self.items)

    def iter(self, **kwargs):
        start = kwargs.get("start", self.items[0].get("_key"))

        def start_idx():
            for i, item in enumerate(self.items):
                if item.get("_key") == start:
                    return i

        count = kwargs.get("count", None)
        counter = 0
        for item in self.items[start_idx() :]:
            if counter == count:
                return
            if _is_filtered(item, kwargs.get("filter")):
                counter += 1
                yield item


@pytest.fixture(scope="function")
def get_source():
    return Source(items=CLOUD_ITEMS)


@pytest.fixture(scope="function", params=[(CLOUD_ITEMS, None, None)])
def get_job(request):
    return Job(*request.param)


@pytest.fixture(scope="function")
def get_collection():
    return Collection()


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


@pytest.fixture(scope="function")
def get_job_items(mocker):
    mocker.patch(
        "arche.readers.items.JobItems.job", return_value=get_job, autospec=True
    )
    raw_data = deepcopy(CLOUD_ITEMS)
    mocker.patch(
        "arche.readers.items.JobItems.fetch_data",
        return_value=np.array(raw_data),
        autospec=True,
    )
    job_items = JobItems(key="112358/13/21", count=len(raw_data))
    return job_items


@pytest.fixture(scope="function")
def get_collection_items(mocker):
    mocker.patch(
        "arche.tools.api.get_collection", return_value=get_collection, autospec=True
    )
    mocker.patch(
        "arche.readers.items.CollectionItems.fetch_data",
        return_value=np.array(CLOUD_ITEMS),
        autospec=True,
    )

    collection_items = CollectionItems(
        key="112358/collections/s/pages", count=len(CLOUD_ITEMS)
    )
    return collection_items


def create_result(
    rule_name, messages, stats=None, err_items_count=None, items_count=None
):
    result = Result(rule_name)
    for level, messages in messages.items():
        for message in messages:
            result.add_message(level, *message)

    if stats:
        result.stats = stats
    if err_items_count:
        result.err_items_count = err_items_count
    if items_count:
        result.items_count = items_count
    return result


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, Result) and isinstance(right, Result) and op == "==":
        assert_msgs = ["Results are equal"]
        for (left_n, left_v), (_, right_v) in zip_longest(
            left.__dict__.items(), right.__dict__.items()
        ):
            if left_n == "_stats":
                for left_stat, right_stat in zip_longest(left_v, right_v):
                    try:
                        if isinstance(left_stat, pd.DataFrame):
                            pd.testing.assert_frame_equal(left_stat, right_stat)
                        else:
                            pd.testing.assert_series_equal(left_stat, right_stat)
                    except AssertionError as e:
                        assert_msgs.extend([f"{left_stat}", "!=", f"{right_stat}"])
                        assert_msgs.extend(str(e).split("\n"))
            elif left_v != right_v:
                assert_msgs.extend([f"{left_v}", "!=", f"{right_v}"])
        return assert_msgs


def create_named_df(data: Dict, index: List[str], name: str) -> pd.DataFrame:
    df = pd.DataFrame(data, index=index)
    df.name = name
    return df

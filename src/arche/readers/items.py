from abc import abstractmethod
import numbers
from typing import Any, Dict, Iterable, Optional

from arche import SH_URL
from arche.tools import pandas, api
import numpy as np
import pandas as pd
from scrapinghub import ScrapinghubClient
from scrapinghub.client.jobs import Job

RawItems = Iterable[Dict[str, Any]]


class Items:
    def __init__(self, raw: RawItems, df: pd.DataFrame, expand: bool = False):
        self.raw = raw
        self.df = self.process_df(df)
        self._flat_df = None
        self.expand = expand

    def __len__(self):
        return len(self.df)

    @property
    def flat_df(self):
        if self._flat_df is None:
            if self.expand:
                self._flat_df, self._columns_map = pandas.flatten_df(self.df)
            else:
                self._flat_df = self.df
                self._columns_map = {}
        return self._flat_df

    @staticmethod
    def process_df(df: pd.DataFrame) -> pd.DataFrame:
        # clean empty objects - mainly lists and dicts, but keep everything else
        df = df.applymap(lambda x: x if x or isinstance(x, numbers.Real) else np.nan)
        if "_type" in df.columns:
            df["_type"] = df["_type"].astype("category")
        return df

    def get_origin_column_name(self, column_name: str) -> str:
        return self._columns_map.get(column_name, column_name)

    @classmethod
    def from_df(cls, df: pd.DataFrame, expand: bool = True):
        if "_key" not in df.columns:
            df["_key"] = df.index
            df["_key"] = df["_key"].apply(str)
        return cls(raw=np.array(df.to_dict("records")), df=df, expand=expand)

    @classmethod
    def from_array(cls, iterable: RawItems, expand: bool = True):
        return cls(raw=iterable, df=pd.DataFrame(list(iterable)), expand=expand)


class CloudItems(Items):
    def __init__(
        self,
        key: str,
        count: Optional[int] = None,
        filters: Optional[api.Filters] = None,
        expand: bool = True,
    ):
        self.key = key
        self._count = count
        self._limit = None
        self.filters = filters
        self.expand = expand
        raw = self.fetch_data()
        df = pd.DataFrame(list(raw))
        df["_key"] = self.format_keys(df["_key"])
        super().__init__(raw=raw, df=df, expand=expand)

    @property
    @abstractmethod
    def limit(self):
        "The maximum number of items in source"
        raise NotImplementedError

    @property
    @abstractmethod
    def count(self):
        "The number of items users wants to retrieve"
        raise NotImplementedError

    @abstractmethod
    def fetch_data(self):
        raise NotImplementedError

    def format_keys(self, keys: pd.Series) -> pd.Series:
        raise NotImplementedError


class JobItems(CloudItems):
    def __init__(
        self,
        key: str,
        start: int = 0,
        count: Optional[int] = None,
        filters: Optional[api.Filters] = None,
        expand: bool = True,
    ):
        self.start_index: int = start
        self._job: Job = None
        super().__init__(key, count, filters, expand)

    @property
    def limit(self) -> int:
        if not self._limit:
            self._limit = api.get_items_count(self.job)
        return self._limit

    @property
    def count(self) -> int:
        if not self._count:
            self._count = self.limit - self.start_index
        return self._count

    @property
    def job(self) -> Job:
        if not self._job:
            job = ScrapinghubClient().get_job(self.key)
            if job.metadata.get("state") == "deleted":
                raise ValueError(f"{self.key} has 'deleted' state")
            self._job = job
        return self._job

    def fetch_data(self) -> np.ndarray:
        if self.filters or self.count < 200_000:
            return api.get_items(self.key, self.count, self.start_index, self.filters)
        else:
            return api.get_items_with_pool(self.key, self.count, self.start_index)

    def format_keys(self, keys: pd.Series) -> pd.Series:
        """Get Scrapy Cloud url to an item
        E.g. 112358/13/21/0 to https://app.scrapinghub.com/p/112358/13/21/item/0"""
        return (
            SH_URL + "/" + self.key + "/item/" + keys.str.rsplit("/", 1, expand=True)[1]
        )


class CollectionItems(CloudItems):
    @property
    def limit(self) -> int:
        if not self._limit:
            self._limit = api.get_collection(self.key).count()
        return self._limit

    @property
    def count(self) -> int:
        if not self._count:
            self._count = self.limit
        return self._count

    def fetch_data(self) -> np.ndarray:
        return api.get_items(self.key, self.count, 0, self.filters)

    def format_keys(self, keys: pd.Series) -> pd.Series:
        """Get full Scrapy Cloud url from `_key`
        E.g. be-006 to https://app.scrapinghub.com/p/collections/s/pages/be-006"""

        return f"{SH_URL}/{self.key}/" + keys

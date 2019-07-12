from abc import abstractmethod
import numbers
from typing import Any, Dict, Iterable, Optional

from arche import SH_URL
from arche.tools import api
import numpy as np
import pandas as pd
from scrapinghub import ScrapinghubClient
from scrapinghub.client.jobs import Job
from tqdm import tqdm_notebook

RawItems = Iterable[Dict[str, Any]]


class Items:
    def __init__(self, raw: RawItems, df: pd.DataFrame):
        self.raw = raw
        self.df = self.process_df(df)

    def __len__(self) -> int:
        return len(self.df)

    @staticmethod
    def process_df(df: pd.DataFrame) -> pd.DataFrame:
        # clean empty objects - mainly lists and dicts, but keep everything else
        df = df.applymap(lambda x: x if x or isinstance(x, numbers.Real) else np.nan)
        Items.categorize(df)
        return df

    @staticmethod
    def categorize(df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns with repeating values to `category` type to save memory"""
        if len(df) < 100:
            return
        for c in tqdm_notebook(df.columns, desc="Categorizing"):
            try:
                if df[c].nunique(dropna=False) <= 10:
                    df[c] = df[c].astype("category")
            # ignore lists and dicts columns
            except TypeError:
                continue

    def origin_column_name(self, new: str) -> str:
        if new in self.df.columns:
            return new
        for column in self.df.columns:
            if column in new:
                return column

    @classmethod
    def from_df(cls, df: pd.DataFrame):
        return cls(raw=np.array(df.to_dict("records")), df=df)

    @classmethod
    def from_array(cls, iterable: RawItems):
        return cls(raw=iterable, df=pd.DataFrame(list(iterable)))


class CloudItems(Items):
    def __init__(
        self,
        key: str,
        count: Optional[int] = None,
        filters: Optional[api.Filters] = None,
    ):
        self.key = key
        self._count = count
        self._limit = None
        self.filters = filters
        raw = self.fetch_data()
        df = pd.DataFrame(list(raw))
        df.index = self.format_keys(df["_key"])
        df.index.name = None
        df = df.drop(columns=["_key", "_type"], errors="ignore")
        super().__init__(raw=raw, df=df)

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
        count: Optional[int] = None,
        start_index: int = 0,
        filters: Optional[api.Filters] = None,
    ):
        self.start_index = start_index
        self.start: int = f"{key}/{start_index}"
        self._job: Job = None
        super().__init__(key, count, filters)

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
                raise ValueError(f"'{self.key}' has 'deleted' state")
            self._job = job
        return self._job

    def fetch_data(self) -> np.ndarray:
        if self.filters or self.count < 200_000:
            return api.get_items(
                self.key, self.count, self.start_index, self.start, self.filters
            )
        else:
            return api.get_items_with_pool(self.key, self.count, self.start_index)

    def format_keys(self, keys: pd.Series) -> pd.Series:
        """Get Scrapy Cloud url to an item
        E.g. 112358/13/21/0 to https://app.scrapinghub.com/p/112358/13/21/item/0"""
        return (
            SH_URL + "/" + self.key + "/item/" + keys.str.rsplit("/", 1, expand=True)[1]
        )


class CollectionItems(CloudItems):
    def __init__(
        self,
        key: str,
        count: Optional[int] = None,
        start: Optional[str] = None,
        filters: Optional[api.Filters] = None,
    ):
        self.start = start
        super().__init__(key, count, filters)

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
        desc = f"Fetching from '{self.key.rsplit('/')[-1]}'"
        return api.get_items(
            self.key, self.count, 0, self.start, self.filters, desc=desc
        )

    def format_keys(self, keys: pd.Series) -> pd.Series:
        """Get full Scrapy Cloud url from `_key`
        E.g. be-006 to https://app.scrapinghub.com/p/collections/s/pages/be-006"""

        return f"{SH_URL}/{self.key}/" + keys

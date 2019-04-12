from abc import ABC, abstractmethod
import numbers
from typing import Optional

from arche.tools import pandas, api
import pandas as pd
from scrapinghub import ScrapinghubClient
from scrapinghub.client.jobs import Job

# Job = scrapinghub.client.jobs.Job


class Items(ABC):
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
        self._dicts = None
        self._df = None
        self._flat_df = None
        self.expand = expand

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

    @property
    def dicts(self):
        "Raw dicts of items downloaded from Scrapy Cloud"
        if not self._dicts:
            self._dicts = self.fetch_data()
        return self._dicts

    @property
    def df(self):
        if self._df is None:
            self._df = self.create_df_from_items()
        return self._df

    @property
    def size(self):
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

    def create_df_from_items(self):
        df = pd.DataFrame(self.dicts)
        # clean empty objects, but keep everything else
        df = df.applymap(lambda x: x if x or isinstance(x, numbers.Real) else None)
        return df

    def get_origin_column_name(self, column_name):
        return self._columns_map.get(column_name, column_name)


class JobItems(Items):
    def __init__(self, start: int = 0, *args, **kwargs):
        self.start_index = start
        self._job: Job = None
        super().__init__(*args, **kwargs)

    @property
    def limit(self):
        if not self._limit:
            self._limit = api.get_items_count(self.job)
        return self._limit

    @property
    def count(self):
        if not self._count:
            self._count = self.limit - self.start_index
        return self._count

    @property
    def job(self):
        if not self._job:
            self._job = ScrapinghubClient().get_job(self.key)
        if self._job.metadata.get("state") == "deleted":
            raise ValueError(f"{self.key} has 'deleted' state")

        return self._job

    def fetch_data(self):
        if self.filters or self.count < 200_000:
            return api.get_items(self.key, self.count, self.start_index, self.filters)
        else:
            return api.get_items_with_pool(self.key, self.count, self.start_index)


class CollectionItems(Items):
    @property
    def limit(self):
        if not self._limit:
            self._limit = api.get_collection(self.key).count()
        return self._limit

    @property
    def count(self):
        if not self._count:
            self._count = self.limit
        return self._count

    def fetch_data(self):
        return api.get_items(self.key, self.count, 0, self.filters)

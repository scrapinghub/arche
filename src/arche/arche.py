from functools import lru_cache
import logging
from typing import Iterable, Optional, Union

from arche.data_quality_report import DataQualityReport
from arche.readers.items import Items, CollectionItems, JobItems, RawItems
from arche.readers.schema import Schema, SchemaSource
from arche.report import Report
import arche.rules.category as category_rules
import arche.rules.coverage as coverage_rules
import arche.rules.duplicates as duplicate_rules
import arche.rules.json_schema as schema_rules
import arche.rules.metadata as metadata_rules
from arche.rules.others import compare_boolean_fields, garbage_symbols
import arche.rules.price as price_rules
from arche.tools import api, helpers, maintenance
import IPython
import pandas as pd


logger = logging.getLogger("arche")


class Arche:
    def __init__(
        self,
        source: Union[str, pd.DataFrame, RawItems],
        schema: Optional[SchemaSource] = None,
        target: Optional[Union[str, pd.DataFrame]] = None,
        count: Optional[int] = None,
        start: Union[str, int] = None,
        filters: Optional[api.Filters] = None,
        expand: bool = None,
    ):
        """
        Args:
            source: a data source to validate, accepts job keys, pandas df, lists
            schema: a JSON schema source used to run validation
            target: a data source to compare with
            count: the amount of items to read from start
            start: an item key to start reading from
            filters: Scrapinghub filtering, see
            https://python-scrapinghub.readthedocs.io/en/latest/client/apidocs.html#scrapinghub.client.items.Items # noqa
        """
        if expand:
            maintenance.deprecate(
                "'expand' parameter is deprecated and will be removed in the next 0.3.7"
                " release. See CHANGES.md for more details.",
                gone_in="0.3.7",
            )
        if isinstance(source, str) and target == source:
            raise ValueError(
                "'target' is equal to 'source'. Data to compare should have different sources."
            )
        if isinstance(source, pd.DataFrame):
            logger.warning(
                "Pandas stores `NA` (missing) data differently, "
                "which might affect schema validation. "
                "Should you care, consider passing raw data in array-like types.\n"
                "For more details, see https://pandas.pydata.org/pandas-docs/"
                "stable/user_guide/gotchas.html#nan-integer-na-values-and-na-type-promotions"
            )
        self.source = source
        self._schema = None
        self.schema_source = None
        if schema:
            self.schema = schema
        self.target = target
        self.start = start
        self.count = count
        self.filters = filters
        self._source_items = None
        self._target_items = None
        self.report = Report()

    @property
    def source_items(self):
        if not self._source_items:
            self._source_items = self.get_items(
                self.source, self.count, self.start, self.filters
            )
        return self._source_items

    @property
    def target_items(self):
        if self.target is None:
            return None
        if not self._target_items:
            self._target_items = self.get_items(
                self.target, self.count, self.start, self.filters
            )
        return self._target_items

    @property
    def schema(self):
        if not self._schema and self.schema_source:
            self._schema = Schema(self.schema_source)
        return self._schema

    @schema.setter
    def schema(self, schema_source):
        self.schema_source = schema_source
        self._schema = Schema(schema_source)

    @staticmethod
    def get_items(
        source: Union[str, pd.DataFrame, RawItems],
        count: Optional[int],
        start: Union[str, int],
        filters: Optional[api.Filters],
    ) -> Items:
        if isinstance(source, pd.DataFrame):
            return Items.from_df(source)
        elif isinstance(source, Iterable) and not isinstance(source, str):
            return Items.from_array(source)
        elif helpers.is_job_key(source):
            return JobItems(source, count, start or 0, filters)
        elif helpers.is_collection_key(source):
            return CollectionItems(source, count, start, filters)
        else:
            raise ValueError(f"'{source}' is not a valid job or collection key")

    def save_result(self, rule_result):
        self.report.save(rule_result)

    def report_all(self, short: bool = False) -> None:
        self.run_all_rules()
        IPython.display.clear_output()
        self.report.write_summaries()
        self.report.write("\n" * 2)
        self.report.write_details(short)

    def run_all_rules(self):
        if isinstance(self.source_items, JobItems):
            self.check_metadata(self.source_items.job)
            if self.target_items:
                self.compare_metadata(self.source_items.job, self.target_items.job)
        self.run_general_rules()
        self.run_comparison_rules()
        self.run_schema_rules()

    def data_quality_report(self, bucket: Optional[str] = None):
        if helpers.is_collection_key(self.source):
            raise ValueError("Collections are not supported")
        if not self.schema:
            raise ValueError("Schema is empty")
        IPython.display.clear_output()
        DataQualityReport(self.source_items, self.schema, self.report, bucket)

    @lru_cache(maxsize=32)
    def run_general_rules(self):
        self.save_result(garbage_symbols(self.source_items.df))
        df = self.source_items.df
        self.save_result(
            coverage_rules.check_fields_coverage(
                df.drop(columns=df.columns[df.columns.str.startswith("_")])
            )
        )
        self.save_result(category_rules.get_categories(df))

    def validate_with_json_schema(self) -> None:
        """Run JSON schema check and output results. It will try to find all errors, but
        there are no guarantees. Slower than `check_with_json_schema()`
        """
        res = schema_rules.validate(
            self.schema.raw, self.source_items.raw, self.source_items.df.index
        )
        self.save_result(res)
        res.show()

    def glance(self) -> None:
        """Run JSON schema check and output results. In most cases it will return
        only the first error per item. Usable for big jobs as it's about 100x faster than
        `validate_with_json_schema()`.
        """
        res = schema_rules.validate(
            self.schema.raw,
            self.source_items.raw,
            self.source_items.df.index,
            fast=True,
        )
        self.save_result(res)
        res.show()

    def run_schema_rules(self) -> None:
        if not self.schema:
            return
        self.save_result(
            schema_rules.validate(
                self.schema.raw, self.source_items.raw, self.source_items.df.index
            )
        )

        target_columns = (
            self.target_items.df.columns.values if self.target_items else None
        )

        check_tags_result = schema_rules.check_tags(
            self.source_items.df.columns.values, target_columns, self.schema.tags
        )
        self.save_result(check_tags_result)
        if check_tags_result.errors:
            return

        self.run_customized_rules(self.source_items, self.schema.tags)
        self.compare_with_customized_rules(
            self.source_items, self.target_items, self.schema.tags
        )

    def run_customized_rules(self, items, tagged_fields):
        self.save_result(price_rules.compare_was_now(items.df, tagged_fields))
        self.save_result(duplicate_rules.find_by_unique(items.df, tagged_fields))
        self.save_result(duplicate_rules.find_by_name_url(items.df, tagged_fields))
        self.save_result(
            category_rules.get_coverage_per_category(
                items.df, tagged_fields.get("category", []) + self.schema.enums
            )
        )

    @lru_cache(maxsize=32)
    def check_metadata(self, job):
        self.save_result(metadata_rules.check_outcome(job))
        self.save_result(metadata_rules.check_errors(job))

    @lru_cache(maxsize=32)
    def compare_metadata(self, source_job, target_job):
        self.save_result(metadata_rules.compare_spider_names(source_job, target_job))
        self.save_result(metadata_rules.check_errors(source_job, target_job))
        self.save_result(
            metadata_rules.compare_number_of_scraped_items(source_job, target_job)
        )
        self.save_result(coverage_rules.get_difference(source_job, target_job))
        self.save_result(metadata_rules.compare_response_ratio(source_job, target_job))
        self.save_result(metadata_rules.compare_runtime(source_job, target_job))
        self.save_result(metadata_rules.compare_finish_time(source_job, target_job))

    @lru_cache(maxsize=32)
    def run_comparison_rules(self):
        if not self.target_items:
            return
        for r in [coverage_rules.compare_scraped_fields, compare_boolean_fields]:
            self.save_result(r(self.source_items.df, self.target_items.df))

    def compare_with_customized_rules(self, source_items, target_items, tagged_fields):
        if not target_items:
            return
        self.save_result(
            category_rules.get_difference(
                source_items.df,
                target_items.df,
                tagged_fields.get("category", []) + self.schema.enums,
            )
        )
        for r in [
            price_rules.compare_prices_for_same_urls,
            price_rules.compare_names_for_same_urls,
            price_rules.compare_prices_for_same_names,
        ]:
            self.save_result(r(source_items.df, target_items.df, tagged_fields))

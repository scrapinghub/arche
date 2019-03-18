from functools import lru_cache
import json
import logging
from typing import List, Optional, Union

from arche.data_quality_report import DataQualityReport
from arche.readers.items import CollectionItems, JobItems
from arche.readers.schema import get_schema, SchemaSource
from arche.report import Report
import arche.rules.category_coverage as category_coverage
import arche.rules.coverage as coverage_rules
import arche.rules.duplicates as duplicate_rules
from arche.rules.garbage_symbols import garbage_symbols
import arche.rules.json_schema as schema_rules
import arche.rules.metadata as metadata_rules
from arche.rules.other_rules import compare_boolean_fields
import arche.rules.price as price_rules
from arche.tools import api, helpers
import arche.tools.schema as schema_tools
import numpy as np

logger = logging.getLogger(__name__)


class Arche:
    def __init__(
        self,
        source: str,
        schema: Optional[SchemaSource] = None,
        target: Optional[str] = None,
        start: int = 0,
        count: Optional[int] = None,
        filters: Optional[api.Filters] = None,
        expand: bool = True,
    ):
        """
        Args:
            source: a data source to validate. Supports job or collection keys
            schema: a JSON schema source used to run validation
            target: a data source to compare with
            start: an item number to start reading from
            count: the amount of items to read from start
            filters: Scrapinghub filtering
            expand: if enabled, use flattened data in garbage rules, affects performance, see flatten_df # noqa
        """
        self.source = source
        if target == self.source:
            logger.warning("'target' is the same as 'source', and will be ignored")
            self.target = None
        else:
            self.target = target
        self.start = start
        self.count = count
        self.filters = filters
        self.expand = expand
        self.schema_source = schema
        if schema:
            self._schema = get_schema(schema)
        else:
            self._schema = None
        self._source_items = None
        self._target_items = None

        self.report = Report()

    @property
    def source_items(self):
        if not self._source_items:
            self._source_items = self.get_items(
                self.source, self.start, self.count, self.filters, self.expand
            )
        return self._source_items

    @property
    def target_items(self):
        if not self.target:
            return None
        if not self._target_items:
            self._target_items = self.get_items(
                self.target, self.start, self.count, self.filters, self.expand
            )
        return self._target_items

    @property
    def schema(self):
        if not self._schema and self.schema_source:
            self._schema = get_schema(self.schema_source)
        return self._schema

    @schema.setter
    def schema(self, schema_source):
        self.schema_source = schema_source
        self._schema = get_schema(schema_source)

    @staticmethod
    def get_items(
        source: str,
        start: int,
        count: Optional[int],
        filters: Optional[api.Filters],
        expand: bool,
    ) -> Union[JobItems, CollectionItems]:
        if helpers.is_job_key(source):
            return JobItems(
                key=source, start=start, count=count, filters=filters, expand=expand
            )
        elif helpers.is_collection_key(source):
            if start:
                raise ValueError("Collections API does not support 'start' parameter")
            return CollectionItems(
                key=source, count=count, filters=filters, expand=expand
            )
        else:
            raise ValueError(f"'{source}' is not a valid job or collection key")

    def save_result(self, rule_result):
        self.report.save(rule_result)

    def basic_json_schema(self, items_numbers: List[int] = None):
        basic_json_schema(self.source)

    def report_all(self):
        self.run_all_rules()
        self.report.write_summary()
        self.report.write("\n" * 2)
        self.report.write_details(short=True)

    def run_all_rules(self):
        if helpers.is_job_key(self.source_items.key):
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
        if not self.report.results:
            self.save_result(
                schema_rules.validate(
                    self.schema, items_dicts=self.source_items.dicts, fast=False
                )
            )

        DataQualityReport(self.source_items, self.schema, self.report, bucket)

    @lru_cache(maxsize=32)
    def run_general_rules(self):
        self.save_result(garbage_symbols(self.source_items))
        self.save_result(coverage_rules.check_fields_coverage(self.source_items.df))

    def validate_with_json_schema(self):
        """Run JSON schema check and output results. It will try to find all errors, but
        there are no guarantees. Slower than `check_with_json_schema()`
        """
        res = schema_rules.validate(
            self.schema, items_dicts=self.source_items.dicts, fast=False
        )
        self.save_result(res)
        self.report.write_result(res, short=False)

    def glance(self):
        """Run JSON schema check and output results. In most cases it will stop after
        the first error per item. Usable for big jobs as it's about 100x faster than
        `validate_with_json_schema()`.
        """
        res = schema_rules.validate(
            self.schema, items_dicts=self.source_items.dicts, fast=True
        )
        self.save_result(res)
        self.report.write_result(res, short=False)

    def run_schema_rules(self):
        if not self.schema:
            return

        self.save_result(schema_rules.validate(self.schema, self.source_items.dicts))

        json_fields = schema_tools.JsonFields(self.schema)
        target_columns = (
            self.target_items.df.columns.values if self.target_items else np.array([])
        )

        check_tags_result = schema_rules.check_tags(
            self.source_items.df.columns.values, target_columns, json_fields.tagged
        )
        self.save_result(check_tags_result)
        if check_tags_result.errors:
            return

        self.run_customized_rules(self.source_items, json_fields)
        self.compare_with_customized_rules(
            self.source_items, self.target_items, json_fields.tagged
        )

    @lru_cache(maxsize=32)
    def run_customized_rules(self, items, fields):
        self.save_result(price_rules.compare_was_now(items.df, fields.tagged))
        self.save_result(duplicate_rules.check_uniqueness(items.df, fields.tagged))
        self.save_result(duplicate_rules.check_items(items.df, fields.tagged))
        self.save_result(
            category_coverage.get_coverage_per_category(items.df, fields.tagged)
        )

    @lru_cache(maxsize=32)
    def check_metadata(self, job):
        self.save_result(metadata_rules.check_outcome(job))
        self.save_result(metadata_rules.check_errors(job))
        self.save_result(metadata_rules.check_response_ratio(job))

    @lru_cache(maxsize=32)
    def compare_metadata(self, source_job, target_job):
        self.save_result(metadata_rules.compare_spider_names(source_job, target_job))
        self.save_result(metadata_rules.compare_errors(source_job, target_job))
        self.save_result(
            metadata_rules.compare_number_of_scraped_items(source_job, target_job)
        )
        self.save_result(coverage_rules.compare_fields_counts(source_job, target_job))
        self.save_result(metadata_rules.compare_response_ratio(source_job, target_job))
        self.save_result(metadata_rules.compare_runtime(source_job, target_job))
        self.save_result(metadata_rules.compare_finish_time(source_job, target_job))

    @lru_cache(maxsize=32)
    def run_comparison_rules(self):
        if not self.target_items:
            return
        self.save_result(
            coverage_rules.compare_scraped_fields(
                self.source_items.df, self.target_items.df
            )
        )
        self.save_result(
            compare_boolean_fields(self.source_items.df, self.target_items.df)
        )

    def compare_with_customized_rules(self, source_items, target_items, tagged_fields):
        if not target_items:
            return
        self.save_result(
            category_coverage.compare_coverage_per_category(
                source_items.key,
                target_items.key,
                source_items.df,
                target_items.df,
                tagged_fields,
            )
        )
        self.save_result(
            price_rules.compare_prices_for_same_urls(
                source_items.df, target_items.df, tagged_fields
            )
        )
        self.save_result(
            price_rules.compare_names_for_same_urls(
                source_items.df, target_items.df, tagged_fields
            )
        )
        self.save_result(
            price_rules.compare_prices_for_same_names(
                source_items.df, target_items.df, tagged_fields
            )
        )


def basic_json_schema(data_source: str, items_numbers: List[int] = None):
    """Prints a json schema based on the provided job_key and item numbers

    Args:
        data_source: a collection or job key
        items_numbers: array of item numbers to create schema from
    """
    schema = schema_tools.create_json_schema(data_source, items_numbers)
    print(json.dumps(schema, indent=4))

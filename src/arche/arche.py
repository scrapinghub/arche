from functools import lru_cache
import logging
from typing import List, Optional, Union

from arche.data_quality_report import DataQualityReport
from arche.readers.items import CollectionItems, JobItems
import arche.readers.schema as sr
from arche.report import Report
import arche.rules.category as category_rules
import arche.rules.coverage as coverage_rules
import arche.rules.duplicates as duplicate_rules
import arche.rules.json_schema as schema_rules
import arche.rules.metadata as metadata_rules
from arche.rules.others import compare_boolean_fields, garbage_symbols
import arche.rules.price as price_rules
from arche.tools import api, helpers, maintenance, schema
import IPython

logger = logging.getLogger(__name__)


class Arche:
    def __init__(
        self,
        source: str,
        schema: Optional[sr.SchemaSource] = None,
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
        self.schema_source = None
        self._schema = None
        if schema:
            self.schema = sr.get_schema(schema)
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
            self._schema = sr.get_schema(self.schema_source)
        return self._schema

    @schema.setter
    def schema(self, schema_source):
        self.schema_source = schema_source
        self._schema = sr.get_schema(schema_source)

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
        """Prints a json schema based on data from `self.source`

        Args:
            items_numbers: array of item numbers to create a schema from
        """
        maintenance.deprecate(
            "'Arche.basic_json_schema()' was deprecated in 2019.03.25 and "
            "will be removed in 2019.04.22.",
            replacement="Use 'basic_json_schema()' instead",
            gone_in="0.4.0",
        )
        schema.basic_json_schema(self.source, items_numbers)

    def report_all(self):
        self.run_all_rules()
        IPython.display.clear_output()
        self.report.write_summaries()
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
        IPython.display.clear_output()
        DataQualityReport(self.source_items, self.schema, self.report, bucket)

    @lru_cache(maxsize=32)
    def run_general_rules(self):
        self.save_result(garbage_symbols(self.source_items))
        self.save_result(
            coverage_rules.check_fields_coverage(
                self.source_items.df.drop(columns=["_type", "_key"])
            )
        )

    def validate_with_json_schema(self):
        """Run JSON schema check and output results. It will try to find all errors, but
        there are no guarantees. Slower than `check_with_json_schema()`
        """
        res = schema_rules.validate(
            self.schema, items_dicts=self.source_items.dicts, fast=False
        )
        self.save_result(res)
        res.show()

    def glance(self):
        """Run JSON schema check and output results. In most cases it will stop after
        the first error per item. Usable for big jobs as it's about 100x faster than
        `validate_with_json_schema()`.
        """
        res = schema_rules.validate(
            self.schema, items_dicts=self.source_items.dicts, fast=True
        )
        self.save_result(res)
        res.show()

    def run_schema_rules(self):
        if not self.schema:
            return

        self.save_result(schema_rules.validate(self.schema, self.source_items.dicts))

        tagged_fields = sr.Tags().get(self.schema)
        target_columns = (
            self.target_items.df.columns.values if self.target_items else None
        )

        check_tags_result = schema_rules.check_tags(
            self.source_items.df.columns.values, target_columns, tagged_fields
        )
        self.save_result(check_tags_result)
        if check_tags_result.errors:
            return

        self.run_customized_rules(self.source_items, tagged_fields)
        self.compare_with_customized_rules(
            self.source_items, self.target_items, tagged_fields
        )

    def run_customized_rules(self, items, tagged_fields):
        self.save_result(price_rules.compare_was_now(items.df, tagged_fields))
        self.save_result(duplicate_rules.check_uniqueness(items.df, tagged_fields))
        self.save_result(duplicate_rules.check_items(items.df, tagged_fields))
        self.save_result(
            category_rules.get_coverage_per_category(
                items.df, tagged_fields.get("category", [])
            )
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
        self.save_result(coverage_rules.get_difference(source_job, target_job))
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
            category_rules.get_difference(
                source_items.key,
                target_items.key,
                source_items.df,
                target_items.df,
                tagged_fields.get("category", []),
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

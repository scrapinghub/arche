from collections import defaultdict
from enum import Enum
import logging
import random
from typing import List

from arche.schema_definitions import extension
from arche.tools import api, helpers, maintenance
from genson import SchemaBuilder
from scrapinghub import ScrapinghubClient

logger = logging.getLogger(__name__)


EXTENDED_KEYWORDS = {"tag", "unique", "coverage_percentage"}


class Tag(Enum):
    unique = (0,)
    category = (1,)
    category_field = (1,)
    name_field = (2,)
    product_url_field = (3,)
    product_price_field = (4,)
    product_price_was_field = (5,)


class JsonFields:
    tags = set([name for name, member in Tag.__members__.items()])

    def __init__(self, schema):
        self.schema = schema
        self.tagged = defaultdict(list)
        self.get_tags()

    def get_tags(self):
        if "properties" not in self.schema:
            logger.error("The schema does not have 'properties'")
            return

        for key, value in self.schema["properties"].items():
            property_tags = value.get("tag", [])
            if property_tags:
                self.get_field_tags(property_tags, key)

    def get_field_tags(self, tags, field):
        tags = self.parse_tag(tags)
        if not tags:
            logger.warning(f"'{tags}' tag value is invalid, should be str or list[str]")
            return

        invalid_tags = tags - self.tags
        if invalid_tags:
            logger.warning(
                f"{invalid_tags} tag(s) are unsupported, valid tags are:\n"
                f"{', '.join(sorted(list(self.tags)))}"
            )
            tags = tags - invalid_tags

        for tag in tags:
            if tag == "category_field":
                tag = "category"
                maintenance.deprecate(
                    "'category_field' tag was deprecated in 2019.03.11 and "
                    "will be removed in 2019.04.01.",
                    replacement="Use 'category' instead",
                    gone_in="2019.04.01",
                )
            self.tagged[tag].append(field)

    @staticmethod
    def parse_tag(value):
        if isinstance(value, str):
            return set([value])
        if isinstance(value, list):
            return set(value)
        return None


def create_json_schema(source_key: str, item_numbers: List[int] = None) -> dict:
    client = ScrapinghubClient()
    if helpers.is_collection_key(source_key):
        store = api.get_collection(source_key)
        items_count = store.count()
    elif helpers.is_job_key(source_key):
        job = client.get_job(source_key)
        items_count = api.get_items_count(job)
        store = job.items
    else:
        logger.error(f"{source_key} is not a job or collection key")
        return

    if items_count == 0:
        logger.error(f"{source_key} does not have any items")
        return

    item_n_err = "{} is a bad item number, choose numbers between 0 and {}"
    if item_numbers:
        item_numbers.sort()
        if item_numbers[-1] >= items_count or item_numbers[0] < 0:
            logger.error(item_n_err.format(item_numbers[-1], items_count - 1))
            return
    else:
        item_numbers = set_item_no(items_count)

    samples = []
    for n in item_numbers:
        items = api.get_items(source_key, start_index=n, count=1)
        samples.append(items[0])

    return infer_schema(samples)


def infer_schema(samples):
    builder = SchemaBuilder("http://json-schema.org/draft-07/schema#")
    for s in samples:
        builder.add_object(s)
    builder.add_schema(extension)

    return builder.to_schema()


def set_item_no(items_count):
    """Generate random numbers within items_count range

    Returns:
        Random 4 numbers if items_count > 4 otherwise items numbers
    """
    if items_count <= 4:
        return [i for i in range(items_count)]
    return random.sample(range(0, items_count), 4)

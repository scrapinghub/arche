import json
import logging
import random
from typing import List

from arche.schema_definitions import extension
from arche.tools import api, helpers
from genson import SchemaBuilder
from scrapinghub import ScrapinghubClient

logger = logging.getLogger(__name__)


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
        items = api.get_items(source_key, start_index=n, count=1, p_bar=None)
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


def basic_json_schema(data_source: str, items_numbers: List[int] = None):
    """Prints a json schema based on the provided job_key and item numbers

    Args:
        data_source: a collection or job key
        items_numbers: array of item numbers to create schema from
    """
    schema = create_json_schema(data_source, items_numbers)
    print(json.dumps(schema, indent=4))

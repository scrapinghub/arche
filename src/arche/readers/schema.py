from collections import defaultdict
from enum import Enum
import json
import os
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse
import urllib.request

from arche.tools import s3
import perfect_jsonschema

EXTENDED_KEYWORDS = {"tag", "unique", "coverage_percentage"}

SchemaObject = Dict[str, Union[str, bool, int, float, None, List]]
Schema = Dict[str, SchemaObject]
SchemaSource = Union[str, Schema]
TaggedFields = Dict[str, List[str]]


def get_schema(schema_source: Optional[SchemaSource]):
    if isinstance(schema_source, str):
        schema_source = get_schema_from_url(schema_source)

    if isinstance(schema_source, dict):
        perfect_jsonschema.check(schema_source, EXTENDED_KEYWORDS)
        return schema_source
    else:
        raise ValueError(
            f"{json.dumps(str(schema_source), indent=4)} is an unidentified schema source.\n"
            f"A dict, a full s3 path or URL is expected"
        )


def get_schema_from_url(path: str) -> Schema:
    o = urlparse(path)
    netloc = o.netloc
    relative_path = o.path.lstrip("/")
    if not netloc or not relative_path:
        raise ValueError(f"'{path}' is not an s3 path or URL to a schema")
    if o.scheme == "s3":
        return json.loads(s3.get_contents_as_string(netloc, relative_path))
    else:
        return json.loads(get_contents(path))


def set_auth() -> None:
    if "BITBUCKET_USER" in os.environ and "BITBUCKET_PASSWORD" in os.environ:
        auth_handler = urllib.request.HTTPBasicAuthHandler()
        auth_handler.add_password(
            realm="Bitbucket.org HTTP",
            uri="https://bitbucket.org",
            user=os.getenv("BITBUCKET_USER"),
            passwd=os.getenv("BITBUCKET_PASSWORD"),
        )
        opener = urllib.request.build_opener(auth_handler)
        urllib.request.install_opener(opener)


def get_contents(url: str) -> str:
    with urllib.request.urlopen(url) as f:
        return f.read().decode("utf-8")


class Tag(Enum):
    unique = (0,)
    category = (1,)
    name_field = (2,)
    product_url_field = (3,)
    product_price_field = (4,)
    product_price_was_field = (5,)


class Tags:
    values = set([name for name, _ in Tag.__members__.items()])

    def __init__(self):
        self.tagged_fields = defaultdict(list)

    def get(self, schema: Schema) -> TaggedFields:
        if "properties" not in schema:
            raise ValueError("The schema does not have 'properties'")

        for key, value in schema["properties"].items():
            property_tags = value.get("tag", [])
            if property_tags:
                self.get_field_tags(property_tags, key)
        return self.tagged_fields

    def get_field_tags(self, tags, field):
        tags = self.parse_tag(tags)
        if not tags:
            raise ValueError(
                f"'{tags}' tag value is invalid, should be str or list[str]"
            )

        invalid_tags = tags - self.values
        if invalid_tags:
            raise ValueError(
                f"{invalid_tags} tag(s) are unsupported, valid tags are:\n"
                f"{', '.join(sorted(list(self.values)))}"
            )

        for tag in tags:
            self.tagged_fields[tag].append(field)

    @staticmethod
    def parse_tag(value):
        if isinstance(value, str):
            return set([value])
        if isinstance(value, list):
            return set(value)
        return None


set_auth()

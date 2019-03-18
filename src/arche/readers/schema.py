import json
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse
from urllib.request import urlopen

from arche.tools import s3
from arche.tools.schema import EXTENDED_KEYWORDS
import perfect_jsonschema

SchemaObject = Dict[str, Union[str, bool, int, float, None, List]]
Schema = Dict[str, SchemaObject]
SchemaSource = Union[str, Schema]


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


def get_contents(url: str):
    with urlopen(url) as f:
        return f.read().decode("utf-8")

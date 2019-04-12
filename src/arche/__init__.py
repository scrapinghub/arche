import logging

__version__ = "0.3.0"
SH_URL = "https://app.scrapinghub.com/p"  # noqa

from arche.arche import Arche
from arche.readers.items import CollectionItems, JobItems
from arche.rules.duplicates import find_by as find_duplicates_by
from arche.tools.schema import basic_json_schema
from plotly.offline import init_notebook_mode

__all__ = [
    "basic_json_schema",
    "Arche",
    "find_duplicates_by",
    "CollectionItems",
    "JobItems",
]

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("HubstorageClient").setLevel(logging.CRITICAL)
logging.getLogger().setLevel(logging.DEBUG)

init_notebook_mode(connected=True)

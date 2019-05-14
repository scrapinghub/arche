import logging

__version__ = "0.3.5"
SH_URL = "https://app.scrapinghub.com/p"  # noqa

from _plotly_future_ import v4  # noqa
from arche.arche import Arche
from arche.readers.items import CollectionItems, JobItems
from arche.rules.duplicates import find_by as find_duplicates_by
from arche.tools.schema import basic_json_schema
import numpy as np
import pandas as pd
import plotly.io as pio

pio.renderers.default = "notebook_connected+plotly_mimetype"

__all__ = [
    "basic_json_schema",
    "Arche",
    "find_duplicates_by",
    "CollectionItems",
    "JobItems",
    "np",
    "pd",
]

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("HubstorageClient").setLevel(logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s\n%(message)s")

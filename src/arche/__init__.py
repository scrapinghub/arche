import logging

__version__ = "2019.03.18"
SH_URL = "https://app.scrapinghub.com/p"  # noqa

from arche.arche import Arche
from arche.tools.schema import basic_json_schema

__all__ = ["basic_json_schema", "Arche"]

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("HubstorageClient").setLevel(logging.CRITICAL)
logging.getLogger().setLevel(logging.DEBUG)

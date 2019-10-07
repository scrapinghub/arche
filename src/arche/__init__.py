import logging
from typing import *  # noqa

__version__ = "0.3.6"
SH_URL = "https://app.scrapinghub.com/p"  # noqa

from arche.arche import Arche
from arche.tools.schema import basic_json_schema
import numpy as np
import pandas as pd
import plotly.io as pio

pio.renderers.default = "notebook_connected+jupyterlab"

__all__ = ["basic_json_schema", "Arche", "np", "pd"]

logging.basicConfig(level=logging.WARNING, format="%(levelname)-8s\n%(message)s")

import logging
from typing import *  # noqa

__version__ = "0.3.6"
SH_URL = "https://app.scrapinghub.com/p"  # noqa

from arche.arche import Arche
from arche.tools import dataframe
from arche.tools.schema import basic_json_schema
import numpy as np
import pandas as pd
import plotly.io as pio

pd.DataFrame._repr_html_ = dataframe._repr_html_
pd.set_option("display.max_colwidth", -1)
pd.set_option("display.render_links", True)

pio.renderers.default = "notebook_connected+jupyterlab"

__all__ = ["basic_json_schema", "Arche", "np", "pd"]

logging.basicConfig(level=logging.WARNING, format="%(levelname)-8s\n%(message)s")

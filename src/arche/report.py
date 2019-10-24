from typing import Dict


from arche import SH_URL
from arche.rules.result import Result
from bleach import linkify, callbacks
from IPython.display import display_html
from jinja2 import Environment, PackageLoader, select_autoescape
import numpy as np
import pandas as pd


class Report:
    def __init__(self):
        self.results: Dict[str, Result] = {}

        self.env = Environment(
            loader=PackageLoader("arche", "templates"),
            autoescape=select_autoescape(["html"]),
            extensions=["jinja2.ext.loopcontrols"],
        )
        self.env.filters["linkify"] = linkify

    def save(self, result: Result) -> None:
        self.results[result.name] = result

    def __call__(self, rule: Result = None, keys_limit: int = None) -> None:
        if rule:
            template = self.env.get_template("single-rule.html")
            resultHTML = template.render(
                rule=rule,
                pd=pd,
                linkfy_callbacks=[callbacks.target_blank],
                keys_limit=keys_limit,
            )
        else:
            template = self.env.get_template("full-report.html")
            resultHTML = template.render(
                rules=sorted(self.results.values(), key=lambda x: x.outcome.value),
                pd=pd,
                linkfy_callbacks=[callbacks.target_blank],
                keys_limit=keys_limit,
            )
        # this renders the report as an iframe
        # the option was added for generating the docs
        template = self.env.get_template("iframe.html")
        resultHTML = template.render(html_str=resultHTML)
        display_html(resultHTML, raw=True)

    @staticmethod
    def sample_keys(keys: pd.Series, limit: int) -> str:
        if len(keys) > limit:
            sample = keys.sample(limit)
        else:
            sample = keys

        def url(x: str) -> str:
            if SH_URL in x:
                return f"[{x.split('/')[-1]}]({x})"
            key, number = x.rsplit("/", 1)
            return f"[{number}]({SH_URL}/{key}/item/{number})"

        # make links only for Cloud data
        if keys.dtype == np.dtype("object") and "/" in keys.iloc[0]:
            sample = sample.apply(url)

        return ", ".join(sample.apply(str))

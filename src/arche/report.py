from functools import partial
from typing import Dict

from arche import SH_URL
from arche.rules.result import Level, Outcome, Result
from IPython.display import display_markdown
import numpy as np
import pandas as pd

display_markdown = partial(display_markdown, raw=True)


class Report:
    def __init__(self):
        self.results: Dict[str, Result] = {}

    def save(self, result: Result) -> None:
        self.results[result.name] = result

    @staticmethod
    def write_color_text(text: str, color: str = "#8A0808") -> None:
        display_markdown(f"<font style='color:{color};'>{text}</font>")

    @staticmethod
    def write_rule_name(rule_name: str) -> None:
        display_markdown(f"<h4>{rule_name}</h4>")

    @classmethod
    def write(cls, text: str) -> None:
        display_markdown(text)

    def write_summaries(self) -> None:
        display_markdown(f"<h2>Executed {len(self.results)} rules</h2>")
        for result in self.results.values():
            self.write_summary(result)

    @classmethod
    def write_summary(cls, result: Result) -> None:
        cls.write_rule_name(result.name)
        if not result.messages:
            cls.write_rule_outcome(Outcome.PASSED, Level.INFO)
        for level, rule_msgs in result.messages.items():
            for rule_msg in rule_msgs:
                cls.write_rule_outcome(rule_msg.summary, level)

    @classmethod
    def write_rule_outcome(cls, outcome: str, level: Level = Level.INFO) -> None:
        if isinstance(outcome, Outcome):
            outcome = outcome.name
        msg = outcome
        if level == Level.ERROR:
            cls.write_color_text(msg)
        elif level == Level.WARNING:
            cls.write_color_text(msg, color="#CCCC00")
        elif outcome == Outcome.PASSED.name:
            cls.write_color_text(msg, color="#0B6121")
        else:
            cls.write(msg)

    def write_details(self, short: bool = False, keys_limit: int = 10) -> None:
        display_markdown("<h2>Details</h2>")
        for result in self.results.values():
            if result.detailed_messages_count:
                display_markdown(
                    f"{result.name} ({result.detailed_messages_count} message(s)):"
                )
                self.write_rule_details(result, short, keys_limit)
                display_markdown("<br>")
        display_markdown("<h2>Plots</h2>")
        for result in self.results.values():
            for f in result.figures:
                f.show()

    @classmethod
    def write_rule_details(
        cls, result: Result, short: bool = False, keys_limit: int = 10
    ) -> None:
        for rule_msgs in result.messages.values():
            for rule_msg in rule_msgs:
                if rule_msg.errors:
                    cls.write_detailed_errors(rule_msg.errors, short, keys_limit)
                if rule_msg.detailed:
                    cls.write(rule_msg.detailed)

    @classmethod
    def write_detailed_errors(cls, errors: Dict, short: bool, keys_limit: int) -> None:
        error_messages = sorted(errors.items(), key=lambda i: len(i[1]), reverse=True)

        if short:
            keys_limit = 5
            error_messages = error_messages[:5]

        for attribute, keys in error_messages:
            if isinstance(keys, list):
                keys = pd.Series(keys)
            if isinstance(keys, set):
                keys = pd.Series(list(keys))

            sample = Report.sample_keys(keys, keys_limit)
            display_markdown(f"{len(keys)} items affected - {attribute}: {sample}")

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

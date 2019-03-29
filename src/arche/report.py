from typing import Dict

from arche.rules.result import Level, Result
from colorama import Fore, Style
from IPython.display import display, HTML
import pandas as pd
import plotly.graph_objs as go
from plotly.offline import iplot


class Report:
    def __init__(self):
        self.results: Dict[str, Result] = {}

    def wipe(self):
        self.results = {}

    def save(self, result):
        self.results[result.name] = result

    @staticmethod
    def write_color_text(text, color=Fore.RED, style=""):
        print(color + style + text + Style.RESET_ALL)

    @staticmethod
    def write_rule_name(rule_name):
        print(f"\n{rule_name}:")

    @classmethod
    def write(cls, text):
        print(text)

    def write_summaries(self):
        for result in self.results.values():
            self.write_summary(result)

    @classmethod
    def write_summary(cls, result: Result):
        if not result.messages:
            return
        cls.write_rule_name(result.name)
        for level, rule_msgs in result.messages.items():
            for rule_msg in rule_msgs:
                cls.write_rule_outcome(rule_msg.summary, level)

    @classmethod
    def write_rule_outcome(cls, result, level=Level.INFO):
        msg = f"\t{result}"
        if level == Level.ERROR:
            cls.write_color_text(msg)
        elif level == Level.WARNING:
            cls.write_color_text(msg, color=Fore.YELLOW)
        else:
            cls.write(msg)

    def write_details(self, short: bool = False, keys_limit: int = 10):
        for result in self.results.values():
            if result.detailed_messages_count:
                self.write_rule_name(
                    f"{result.name} ({result.detailed_messages_count} message(s))"
                )
                self.write_rule_details(result, short, keys_limit)

    @classmethod
    def write_rule_details(
        cls, result: Result, short: bool = False, keys_limit: int = 10
    ):
        for rule_msgs in result.messages.values():
            for rule_msg in rule_msgs:
                if rule_msg.errors:
                    cls.write_detailed_errors(rule_msg.errors, short, keys_limit)
                elif rule_msg.detailed:
                    cls.write(rule_msg.detailed)
                cls.plot(rule_msg.stats)

    @staticmethod
    def plot(stats):
        if stats is not None:
            data = [go.Bar(x=stats.values, y=stats.index.values, orientation="h")]
            layout = go.Layout(
                title=stats.name,
                bargap=0.1,
                yaxis=go.layout.YAxis(automargin=True, side="right"),
                template="ggplot2",
            )
            iplot(go.Figure(data=data, layout=layout))

    @classmethod
    def write_detailed_errors(cls, errors: dict, short: bool, keys_limit: int):
        if short:
            keys_limit = 5
            error_messages = list(errors.items())[:5]
        else:
            error_messages = list(errors.items())
        for attribute, keys in error_messages:
            if isinstance(keys, list):
                keys = pd.Series(keys)
            if isinstance(keys, set):
                keys = pd.Series(list(keys))

            sample = cls.sample_keys(keys, keys_limit)
            msg = f"{len(keys)} items affected - {attribute}: {sample}"
            display(HTML(msg))

        display(HTML(f"<br>"))

    @classmethod
    def sample_keys(cls, keys: pd.Series, limit: int) -> str:
        if len(keys) > limit:
            sample = keys.sample(limit)
        else:
            sample = keys

        sample = [f"<a href='{k}'>{k.split('/')[-1]}</a>" for k in sample]
        sample = " ".join(sample)
        return sample

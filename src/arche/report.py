from typing import Dict

from arche.rules.result import Level, Result, Stat
from colorama import Fore, Style
from IPython.display import display, HTML
import ipywidgets
import pandas as pd
import plotly.graph_objs as go


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
        for stat in result.stats:
            cls.plot(stat)

    @staticmethod
    def plot(stat: Stat):
        if isinstance(stat, pd.Series):
            data = [go.Bar(x=stat.values, y=stat.index.values, orientation="h")]
        else:
            data = [
                go.Bar(x=stat[c].values, y=stat.index.values, orientation="h", name=c)
                for c in stat.columns
            ]

        layout = go.Layout(
            title=stat.name,
            bargap=0.1,
            template="ggplot2",
            height=max(min(len(stat) * 20, 900), 450),
            hovermode="y",
            margin=dict(l=200, t=35),
            xaxis=go.layout.XAxis(range=[0, max(stat.values.max(), 1) * 1.05]),
        )
        if stat.name.startswith("Coverage"):
            layout.xaxis.tickformat = ".2p"
        if stat.name == "Coverage for boolean fields":
            layout.barmode = "stack"

        f = go.FigureWidget(data, layout)
        if stat.name == "Fields coverage":
            Report.add_annotations_checkbox(stat, f)
        display(f)

    @staticmethod
    def add_annotations_checkbox(stat: Stat, figure: go.FigureWidget):
        annotations = []
        for value, group in stat.groupby(stat):
            annotations.append(
                dict(
                    xref="paper",
                    yref="y",
                    x=0,
                    y=group.index.values[-1],
                    text=f"{value/max(stat.values) * 100:.2f}%",
                    showarrow=False,
                )
            )
        ants_enabled = ipywidgets.Checkbox(description="%", value=False)

        def response(change):
            if ants_enabled.value:
                figure.layout.annotations = annotations
            else:
                figure.layout.annotations = []

        ants_enabled.observe(response, names="value")
        display(ants_enabled)

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

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union

import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio

Stat = Union[pd.Series, pd.DataFrame]


class Level(Enum):
    ERROR = 2
    WARNING = 1
    INFO = 0


class Outcome(Enum):
    SKIPPED = 0
    PASSED = 1


@dataclass
class Message:
    """
    Args:
        summary: a concise outcome
        detailed: detailed message
        errors: error messages grouped by attributes
    """

    summary: str
    detailed: Optional[str] = None
    errors: Optional[dict] = None


@dataclass
class Result:
    """
    Args:
        name: a rule name
        messages: messages separated by severity
        _stats: pandas data to plot
        items_count: the count of verified items
        checked_fields: the names of verified fields
        err_items_count: the number of error items
        _figures: a list of graphs created from stats
    """

    name: str
    messages: Dict[Level, List[Message]] = field(default_factory=dict)
    _stats: Optional[List[Stat]] = field(default_factory=list)
    items_count: Optional[int] = 0
    checked_fields: Optional[List[str]] = field(default_factory=list)
    err_items_count: Optional[int] = 0
    _figures: Optional[List[go.FigureWidget]] = field(default_factory=list)

    def __eq__(self, other):
        for left, right in zip(self.stats, other.stats):
            if not self.tensors_equal(left, right):
                return False

        return (
            self.name == other.name
            and self.messages == other.messages
            and self.items_count == other.items_count
            and self.checked_fields == other.checked_fields
            and self.err_items_count == other.err_items_count
            and len(self.stats) == len(other.stats)
        )

    @staticmethod
    def tensors_equal(left: Stat, right: Stat):
        try:
            if isinstance(left, pd.DataFrame):
                pd.testing.assert_frame_equal(left, right)
            else:
                pd.testing.assert_series_equal(left, right)
            return True
        except AssertionError:
            return False

    @property
    def info(self):
        return self.messages.get(Level.INFO)

    @property
    def warnings(self):
        return self.messages.get(Level.WARNING)

    @property
    def errors(self):
        return self.messages.get(Level.ERROR)

    @property
    def stats(self):
        return self._stats

    @stats.setter
    def stats(self, value):
        self._stats = value

    @property
    def figures(self):
        if not self._figures:
            self._figures = Result.create_figures(self.stats)
        return self._figures

    def add_info(self, summary, detailed=None, errors=None):
        self.add_message(Level.INFO, summary, detailed, errors)

    def add_warning(self, summary, detailed=None, errors=None):
        self.add_message(Level.WARNING, summary, detailed, errors)

    def add_error(self, summary, detailed=None, errors=None):
        self.add_message(Level.ERROR, summary, detailed, errors)

    def add_message(
        self,
        level: Level,
        summary: str,
        detailed: Optional[str] = None,
        errors: Optional[dict] = None,
    ):
        if not self.messages.get(level):
            self.messages[level] = []
        self.messages[level].append(
            Message(summary=summary, detailed=detailed, errors=errors)
        )

    @property
    def detailed_messages_count(self):
        return (
            self.get_errors_count()
            or self.get_detailed_messages_count()
            or len(self.stats)
        )

    def get_errors_count(self):
        return sum(
            [
                len(message.errors.keys())
                for messages in self.messages.values()
                for message in messages
                if message.errors
            ]
        )

    def get_detailed_messages_count(self):
        return len(
            [
                message.detailed
                for messages in self.messages.values()
                for message in messages
                if message.detailed
            ]
        )

    def show(self, short: bool = False, keys_limit: int = 10):
        from arche.report import Report

        Report.write_summary(self)
        Report.write_rule_details(self, short=short, keys_limit=keys_limit)
        for f in self.figures:
            pio.show(f)

    @staticmethod
    def create_figures(stats: List[Stat]) -> List[go.FigureWidget]:
        figures = []
        for stat in stats:
            if isinstance(stat, pd.Series):
                data = [go.Bar(x=stat.values, y=stat.index.values, orientation="h")]
            else:
                data = [
                    go.Bar(
                        x=stat[c].values, y=stat.index.values, orientation="h", name=c
                    )
                    for c in stat.columns
                ]

            layout = go.Layout(
                title=stat.name,
                bargap=0.1,
                template="seaborn",
                height=max(min(len(stat) * 20, 900), 450),
                hovermode="y",
                margin=dict(l=200, t=35),
                xaxis=go.layout.XAxis(range=[0, max(stat.values.max(), 1) * 1.05]),
            )
            if stat.name.startswith("Coverage"):
                layout.xaxis.tickformat = ".2p"
            if stat.name == "Coverage for boolean fields":
                layout.barmode = "stack"
            if stat.name.startswith("Fields coverage"):
                layout.annotations = Result.make_annotations(stat)

            figures.append(go.FigureWidget(data, layout))
        return figures

    @staticmethod
    def make_annotations(stat: pd.Series) -> List[Dict]:
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
        return annotations

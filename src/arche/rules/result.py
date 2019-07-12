from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union

import IPython
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio

Stat = Union[pd.Series, pd.DataFrame]
COLORS = pio.templates["seaborn"]["layout"]["colorway"]


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
        err_items_count: the number of error items
        _figures: a list of graphs created from stats
    """

    name: str
    messages: Dict[Level, List[Message]] = field(default_factory=dict)
    _stats: Optional[List[Stat]] = field(default_factory=list)
    items_count: Optional[int] = 0
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
        if not self._figures and self.stats:
            self._figures = Result.create_figures(self.stats, self.name)
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

        IPython.display.clear_output()
        Report.write_summary(self)
        Report.write_rule_details(self, short=short, keys_limit=keys_limit)
        for f in self.figures:
            pio.show(f)

    @staticmethod
    def create_figures(stats: List[Stat], name: str) -> List[go.FigureWidget]:
        if name == "Categories":
            data = Result.build_stack_bar_data(stats)
            layout = Result.get_layout("Category fields", len(stats))
            layout.barmode = "stack"
            return [go.FigureWidget(data, layout)]
        figures = []
        for stat in stats:
            y = stat.index.values.astype(str)
            if isinstance(stat, pd.Series):
                colors = [COLORS[0] if v > 0 else COLORS[1] for v in stat.values]
                data = [
                    go.Bar(
                        x=stat.values,
                        y=y,
                        orientation="h",
                        opacity=0.7,
                        marker=dict(color=colors),
                    )
                ]
            else:
                data = [
                    go.Bar(x=stat[c].values, y=y, orientation="h", opacity=0.7, name=c)
                    for c in stat.columns
                ]

            layout = Result.get_layout(stat.name, len(stat))
            layout.xaxis = go.layout.XAxis(
                range=np.array([min(stat.values.min(), 0), max(stat.values.max(), 1)])
                * 1.05
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
    def build_stack_bar_data(values_counts: List[pd.Series]) -> List[go.Bar]:
        """Create data for plotly stack bar chart with consistent colors between
        bars. Each bar values have indexes unique to the bar, without any correlation
        to other obars.

        Args:
            values_counts: an array of value_counts series

        Returns:
            A list of Bar objects.
        """
        data = []
        for vc in values_counts:
            data = data + [
                go.Bar(
                    x=[counts],
                    y=[vc.name],
                    name=str(value)[:30],
                    orientation="h",
                    opacity=0.6,
                    legendgroup=vc.name,
                    marker_color=COLORS[i % 10],
                )
                for i, (value, counts) in enumerate(vc.items())
            ]
        return data

    @staticmethod
    def get_layout(name: str, rows_count: int) -> go.Layout:
        return go.Layout(
            title=name,
            bargap=0.1,
            template="seaborn",
            height=min(max(rows_count * 20, 200), 900),
            hovermode="y",
            margin=dict(l=200, t=35),
        )

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

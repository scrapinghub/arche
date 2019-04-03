from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import pandas as pd


class Level(Enum):
    ERROR = 2
    WARNING = 1
    INFO = 0


@dataclass
class Message:
    """
    Args:
        summary: a concise outcome
        detailed: detailed message
        errors: grouped by attributes error messages
        stats: series
    """

    summary: str
    detailed: Optional[str] = None
    errors: Optional[dict] = None
    stats: Optional[pd.Series] = None

    def __eq__(self, other):
        if self.stats is None:
            stats_equals = other.stats is None
        elif other.stats is None:
            stats_equals = self.stats is None
        else:
            try:
                if isinstance(self.stats, pd.DataFrame):
                    pd.testing.assert_frame_equal(self.stats, other.stats)
                else:
                    pd.testing.assert_series_equal(self.stats, other.stats)
                stats_equals = True
            except AssertionError:
                stats_equals = False

        return (
            self.summary == other.summary
            and self.detailed == other.detailed
            and self.errors == other.errors
            and stats_equals
        )


@dataclass
class Result:
    name: str
    messages: dict = field(default_factory=dict)
    items_count: int = 0
    checked_fields: list = field(default_factory=list)
    err_items_count: int = 0

    @property
    def info(self):
        return self.messages.get(Level.INFO)

    @property
    def warnings(self):
        return self.messages.get(Level.WARNING)

    @property
    def errors(self):
        return self.messages.get(Level.ERROR)

    def add_info(self, summary, detailed=None, errors=None, stats=None):
        self.add_message(Level.INFO, summary, detailed, errors, stats)

    def add_warning(self, summary, detailed=None, errors=None, stats=None):
        self.add_message(Level.WARNING, summary, detailed, errors, stats)

    def add_error(self, summary, detailed=None, errors=None, stats=None):
        self.add_message(Level.ERROR, summary, detailed, errors, stats)

    def add_message(
        self,
        level: Level,
        summary: str,
        detailed: Optional[str] = None,
        errors: Optional[dict] = None,
        stats: Optional[pd.Series] = None,
    ):
        if not self.messages.get(level):
            self.messages[level] = []
        self.messages[level].append(
            Message(summary=summary, detailed=detailed, errors=errors, stats=stats)
        )

    @property
    def detailed_messages_count(self):
        return (
            self.get_errors_count()
            or self.get_detailed_messages_count()
            or self.get_stats_count()
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

    def get_stats_count(self):
        return len(
            [
                message.stats
                for messages in self.messages.values()
                for message in messages
                if message.stats is not None
            ]
        )

    def show(self, short: bool = False, keys_limit: int = 10):
        from arche.report import Report

        Report.write_summary(self)
        Report.write_rule_details(self, short=short, keys_limit=keys_limit)

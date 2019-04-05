from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union

import pandas as pd

Stat = Union[pd.Series, pd.DataFrame]


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
        stats: pandas data to plot
        items_count: the count of verified items
        checked_fields: the names of verified fields
        err_items_count: the number of error items
    """

    name: str
    messages: Dict[Level, List[Message]] = field(default_factory=dict)
    stats: Optional[List[Stat]] = field(default_factory=list)
    items_count: Optional[int] = 0
    checked_fields: Optional[List[str]] = field(default_factory=list)
    err_items_count: Optional[int] = 0

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

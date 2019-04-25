from typing import Dict

from arche.rules.result import Level, Result
from colorama import Fore, Style
from IPython.display import display, HTML
import pandas as pd
import plotly.io as pio


class Report:
    def __init__(self):
        self.results: Dict[str, Result] = {}

    def save(self, result: Result) -> None:
        self.results[result.name] = result

    @staticmethod
    def write_color_text(
        text: str, color: Fore = Fore.RED, style: Style = Style.RESET_ALL
    ) -> None:
        print(color + style + text + Style.RESET_ALL)

    @staticmethod
    def write_rule_name(rule_name: str) -> None:
        print(f"\n{rule_name}:")

    @classmethod
    def write(cls, text: str) -> None:
        print(text)

    def write_summaries(self) -> None:
        for result in self.results.values():
            self.write_summary(result)

    @classmethod
    def write_summary(cls, result: Result) -> None:
        if not result.messages:
            return
        cls.write_rule_name(result.name)
        for level, rule_msgs in result.messages.items():
            for rule_msg in rule_msgs:
                cls.write_rule_outcome(rule_msg.summary, level)

    @classmethod
    def write_rule_outcome(cls, result: Result, level: Level = Level.INFO) -> None:
        msg = f"\t{result}"
        if level == Level.ERROR:
            cls.write_color_text(msg)
        elif level == Level.WARNING:
            cls.write_color_text(msg, color=Fore.YELLOW)
        else:
            cls.write(msg)

    def write_details(self, short: bool = False, keys_limit: int = 10) -> None:
        for result in self.results.values():
            if result.detailed_messages_count:
                self.write_rule_name(
                    f"{result.name} ({result.detailed_messages_count} message(s))"
                )
                self.write_rule_details(result, short, keys_limit)
            for f in result.figures:
                pio.show(f)

    @classmethod
    def write_rule_details(
        cls, result: Result, short: bool = False, keys_limit: int = 10
    ) -> None:
        for rule_msgs in result.messages.values():
            for rule_msg in rule_msgs:
                if rule_msg.errors:
                    cls.write_detailed_errors(rule_msg.errors, short, keys_limit)
                elif rule_msg.detailed:
                    cls.write(rule_msg.detailed)

    @classmethod
    def write_detailed_errors(cls, errors: Dict, short: bool, keys_limit: int) -> None:
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

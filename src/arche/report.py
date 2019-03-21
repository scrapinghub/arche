from typing import Dict

from arche.rules.result import Level, Result
from colorama import Fore, Style
import cufflinks as cf
from IPython.display import display, HTML
import pandas as pd

cf.set_config_file(offline=True, theme="ggplot")


class Report:
    def __init__(self):
        self.results: Dict[str, Result] = {}

    def wipe(self):
        self.results = {}

    def save(self, result):
        self.results[result.name] = result

    def write_color_text(self, text, color=Fore.RED, style=""):
        print(color + style + text + Style.RESET_ALL)

    def write_rule_name(self, rule_name):
        print(f"\n{rule_name}:")

    @classmethod
    def write(cls, text):
        print(text)

    def write_summary(self):
        for result in self.results.values():
            if not result.messages:
                continue
            self.write_rule_name(result.name)
            for level, rule_msgs in result.messages.items():
                for rule_msg in rule_msgs:
                    self.write_rule_outcome(rule_msg.summary, level)

    def write_rule_outcome(self, result, level=Level.INFO):
        msg = f"\t{result}"
        if level == Level.ERROR:
            self.write_color_text(msg)
        elif level == Level.WARNING:
            self.write_color_text(msg, color=Fore.YELLOW)
        else:
            self.write(msg)

    def write_details(self, short: bool = False, keys_limit: int = 10):
        for result in self.results.values():
            self.write_result(result)

    @classmethod
    def write_result(cls, result: Result, short: bool = False, keys_limit: int = 10):
        if result.detailed_messages_count:
            cls.write(
                f"\nRULE: {result.name}\n({result.detailed_messages_count} message(s))\n"
            )
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
            stats.iplot(
                kind="barh",
                bargap=0.1,
                title=stats.name,
                layout=dict(yaxis=dict(automargin=True, side="right")),
            )

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

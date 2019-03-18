import datetime as d
import os


class CollectionKey:
    def __init__(self, project_key, store_key):
        self.project_key = project_key
        self.store_key = store_key

    def __str__(self):
        return f"{self.project_key}/collections/s/{self.store_key}"


def is_collection_key(value: str) -> bool:
    """<project id>/collections/s/<name>"""
    if isinstance(value, str):
        keys = value.split("/")
        if len(keys) == 4 and str.isdigit(keys[0]) and keys[1] == "collections":
            return True
    return False


def parse_collection_key(value):
    parts = value.split("/")
    return CollectionKey(parts[0], parts[3])


def is_job_key(value):
    if isinstance(value, str):
        keys = value.split("/")
        if len(keys) == 3 and all([str.isdigit(k) for k in keys]):
            return True
    return False


def ms_to_time(ms):
    if ms is None:
        return None
    return str(d.timedelta(milliseconds=ms))


def ratio_diff(source, target):
    """Return a difference in ratio between two values"""
    source = to_float_or_zero(source)
    target = to_float_or_zero(target)

    if not source and not target:
        return 0
    if not source:
        source = 0.000_000_000_000_01
    source, target = max(source, target), min(source, target)
    return round((source - target) / source, 2)


def to_float_or_zero(value):
    if value:
        return float(value)
    return 0.0


def is_number(s):
    if s is None:
        return False
    try:
        float(s)
    except ValueError:
        return False
    except TypeError:
        return False
    return True


def cpus_count() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count()

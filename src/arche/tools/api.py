from datetime import datetime
from functools import partial
import math
from multiprocessing import Pool
import time
from typing import List, Tuple, Optional, Union

from arche.tools import helpers
from dateutil.relativedelta import relativedelta
import numpy as np
from scrapinghub import ScrapinghubClient
from scrapinghub.client.jobs import Job
from tqdm import tqdm, tqdm_notebook


Filters = List[Tuple[str, str, str]]


def get_job(key: str) -> Job:
    return ScrapinghubClient().get_job(key)


def get_collection(key):
    client = ScrapinghubClient()
    project = client.get_project(key.split("/")[0])
    collections = project.collections
    return collections.get_store(key.split("/")[3])


def get_errors_count(job):
    if "log_count/ERROR" in job.metadata.get("scrapystats"):
        return job.metadata.get("scrapystats")["log_count/ERROR"]
    else:
        return 0


def get_job_state(job):
    return job.metadata.get("state")


def get_job_close_reason(job):
    return job.metadata.get("close_reason")


def get_items_count(job):
    if job.metadata.get("state") == "deleted":
        return 0

    return job.items.stats().get("totals", {}).get("input_values", 0)


def get_finish_time_difference_in_days(job1, job2):
    finished_time1 = job1.metadata.get("finished_time")
    finished_time2 = job2.metadata.get("finished_time")

    if get_job_state(job2) == "running" or get_job_state(job1) == "running":
        return None

    finish_time1 = datetime.fromtimestamp(finished_time1 / 1000)
    finish_time2 = datetime.fromtimestamp(finished_time2 / 1000)
    time_diff = relativedelta(finish_time2, finish_time1)
    return abs(time_diff.days)


def get_runtime(job):
    """Returns the runtime in milliseconds or None if job is still running"""
    if job.metadata.get("finished_time") is not None:
        finished_time = job.metadata.get("finished_time")
    else:
        return None
    start_time = job.metadata.get("scrapystats")["start_time"]
    return float(finished_time - start_time)


def get_runtime_s(job):
    """Returns job runtime in milliseconds."""

    scrapystats = job.metadata.get("scrapystats")
    finished_time = job.metadata.get("finished_time")
    start_time = scrapystats.get("start_time")
    if finished_time:
        return finished_time - start_time

    return int(round(time.time() * 1000)) - start_time


def get_max_memusage(job):
    if "memusage/max" in job.metadata.get("scrapystats"):
        return job.metadata.get("scrapystats")["memusage/max"]
    else:
        return 0


def get_response_status_count(job):
    scrapystats = job.metadata.get("scrapystats")
    count_200 = scrapystats.get("downloader/response_status_count/200", 0)
    count_301 = scrapystats.get("downloader/response_status_count/301", 0)
    count_404 = scrapystats.get("downloader/response_status_count/404", 0)
    count_503 = scrapystats.get("downloader/response_status_count/503", 0)

    return count_200, count_301, count_404, count_503


def get_requests_count(job):
    scrapystats = job.metadata.get("scrapystats")
    return scrapystats.get("downloader/response_count", 0)


def get_crawlera_user(job):
    crawlera_user = None

    for line in job.logs.list(level="INFO"):
        if "[root] Using crawlera at" in line["message"]:
            crawlera_user = line["message"].split("user: ")[1].replace(")", "")
            break

    return crawlera_user


def get_source(source_key):
    if helpers.is_collection_key(source_key):
        return get_collection(source_key)
    if helpers.is_job_key(source_key):
        return ScrapinghubClient().get_job(source_key).items


def get_items_with_pool(
    source_key: str, count: int, start_index: int = 0, workers: int = 4
) -> np.ndarray:
    """Concurrently reads items from API using Pool

    Args:
        source_key: a job or collection key, e.g. '112358/13/21'
        count: a number of items to retrieve
        start_index: an index to read from
        workers: the number of separate processors to get data in

    Returns:
        A numpy array of items
    """
    active_connections_limit = 10
    processes_count = min(max(helpers.cpus_count(), workers), active_connections_limit)
    batch_size = math.ceil(count / processes_count)

    with Pool(processes_count) as p:
        results = p.starmap(
            partial(get_items, source_key, batch_size, p_bar=tqdm),
            zip([i for i in range(start_index, start_index + count, batch_size)]),
        )
        return np.concatenate(results)


def get_items(
    key: str,
    count: int,
    start_index: int,
    filters: Optional[Filters] = None,
    p_bar: Union[tqdm, tqdm_notebook] = tqdm_notebook,
) -> np.ndarray:
    source = get_source(key)
    items_iter = source.iter(
        start=f"{key}/{start_index}", count=count, filter=filters, meta="_key"
    )
    if p_bar:
        items_iter = p_bar(
            items_iter,
            desc=f"Fetching {start_index}:{start_index+count} from {key}",
            total=count,
            unit_scale=1,
        )
    return np.asarray(list(items_iter))

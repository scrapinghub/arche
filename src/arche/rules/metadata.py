from typing import Optional

from arche import SH_URL
from arche.rules.result import Result
from arche.tools import api, helpers
from scrapinghub.client.jobs import Job


def check_errors(source_job: Job, target_job: Optional[Job] = None) -> Result:
    source_errs = api.get_errors_count(source_job)
    result = Result("Job Errors")
    if not source_errs:
        return result

    errors_url = "{}/{}/log?filterType=error&filterAndHigher"
    result.add_error(
        f"{source_errs} error(s) - {errors_url.format(SH_URL, source_job.key)}"
    )
    if target_job:
        target_errs = api.get_errors_count(target_job)
        result.add_error(
            f"{target_errs} error(s) - {errors_url.format(SH_URL, target_job.key)}"
        )
    return result


def check_outcome(job: Job) -> Result:
    state = api.get_job_state(job)
    reason = api.get_job_close_reason(job)
    result = Result("Job Outcome")
    if state != "finished" or reason != "finished":
        result.add_error(f"Job has '{state}' state, '{reason}' close reason")
    return result


def compare_response_ratio(source_job: Job, target_job: Job) -> Result:
    """Compare request with response per item ratio"""
    s_ratio = round(
        api.get_requests_count(source_job) / api.get_items_count(source_job), 2
    )
    t_ratio = round(
        api.get_requests_count(target_job) / api.get_items_count(target_job), 2
    )

    response_ratio_diff = helpers.ratio_diff(s_ratio, t_ratio)
    msg = f"Difference is {response_ratio_diff:.2%} - {s_ratio} and {t_ratio}"

    result = Result("Compare Responses Per Item Ratio")
    if response_ratio_diff > 0.2:
        result.add_error(msg)
    elif response_ratio_diff > 0.1:
        result.add_warning(msg)
    return result


def compare_number_of_scraped_items(source_job: Job, target_job: Job) -> Result:
    s_count = api.get_items_count(source_job)
    t_count = api.get_items_count(target_job)
    diff = helpers.ratio_diff(s_count, t_count)
    result = Result("Total Scraped Items")
    if 0 <= diff < 0.05:
        if diff == 0:
            msg = "Same number of items"
        else:
            msg = f"Almost the same number of items - {s_count} and {t_count}"
        result.add_info(msg)
    else:
        msg = f"{s_count} differs from {t_count} on {diff:.2%}"
        if 0.05 <= diff < 0.10:
            result.add_warning(msg)
        elif diff >= 0.10:
            result.add_error(msg)
    return result


def compare_spider_names(source_job: Job, target_job: Job) -> Result:
    s_name = source_job.metadata.get("spider")
    t_name = target_job.metadata.get("spider")

    result = Result("Spider Names")
    if s_name != t_name:
        result.add_warning(
            f"{source_job.key} spider is {s_name}, {target_job.key} spider is {t_name}"
        )
    return result


def compare_runtime(source_job: Job, target_job: Job) -> Result:
    source_runtime = api.get_runtime(source_job)
    target_runtime = api.get_runtime(target_job)

    result = Result("Compare Runtime")
    if not source_runtime or not target_runtime:
        result.add_warning("Jobs are not finished")
    elif source_runtime > target_runtime:
        runtime_ratio_diff = helpers.ratio_diff(source_runtime, target_runtime)
        msg = (
            f"Sources differ on {runtime_ratio_diff}% - "
            f"{helpers.ms_to_time(source_runtime)} and "
            f"{helpers.ms_to_time(target_runtime)}"
        )
        if runtime_ratio_diff > 0.2:
            result.add_error(msg)
        elif runtime_ratio_diff > 0.1:
            result.add_warning(msg)
        else:
            result.add_info(msg)
    else:
        result.add_info(
            f"Similar or better runtime - {helpers.ms_to_time(source_runtime)} and "
            f"{helpers.ms_to_time(target_runtime)}"
        )
    return result


def compare_finish_time(source_job: Job, target_job: Job) -> Result:
    diff_in_days = api.get_finish_time_difference_in_days(source_job, target_job)

    result = Result("Finish Time")
    if diff_in_days == 0:
        result.add_info("Less than 1 day difference")
    else:
        if diff_in_days is None:
            result.add_warning("Jobs are not finished")
        else:
            result.add_warning(f"{diff_in_days} day(s) difference between 2 jobs")

    return result

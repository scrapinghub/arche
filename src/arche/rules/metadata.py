from arche import SH_URL
from arche.rules.result import Result
from arche.tools import api, helpers
from scrapinghub.client.jobs import Job


def check_errors(job: Job) -> Result:
    errors_count = api.get_errors_count(job)
    result = Result("Job Errors")
    if errors_count:
        url = f"{SH_URL}/{job.key}/log?filterType=error&filterAndHigher"
        result.add_error(
            f"{errors_count} error(s)", detailed=f"Errors for {job.key} - {url}"
        )
    else:
        result.add_info(f"No errors")
    return result


def check_outcome(job: Job) -> Result:
    state = api.get_job_state(job)
    reason = api.get_job_close_reason(job)
    result = Result("Job Outcome")
    if state != "finished" or reason != "finished":
        result.add_error(f"Job has '{state}' state, '{reason}' close reason")
    else:
        result.add_info("Finished")
    return result


def check_response_ratio(job: Job) -> Result:
    requests_number = api.get_requests_count(job)
    items_count = api.get_items_count(job)
    result = Result("Responses Per Item Ratio")
    result.add_info(
        f"Number of responses / Number of scraped items - "
        f"{round(requests_number / items_count, 2)}"
    )
    return result


def compare_response_ratio(source_job: Job, target_job: Job) -> Result:
    """Compare request with response per item ratio"""
    items_count1 = api.get_items_count(source_job)
    items_count2 = api.get_items_count(target_job)

    source_ratio = round(api.get_requests_count(source_job) / items_count1, 2)
    target_ratio = round(api.get_requests_count(target_job) / items_count2, 2)

    response_ratio_diff = helpers.ratio_diff(source_ratio, target_ratio)
    msg = "Difference is {}% - {} and {}".format(
        response_ratio_diff * 100, source_ratio, target_ratio
    )

    result = Result("Compare Responses Per Item Ratio")
    if response_ratio_diff > 0.2:
        result.add_error(msg)
    elif response_ratio_diff > 0.1:
        result.add_warning(msg)
    return result


def compare_errors(source_job: Job, target_job: Job) -> Result:
    errors_count1 = api.get_errors_count(source_job)
    errors_count2 = api.get_errors_count(target_job)

    result = Result("Compare Job Errors")
    if errors_count1:
        errors_url = "{}/{}/log?filterType=error&filterAndHigher"
        detailed_msg = (
            f"{errors_count1} error(s) for {source_job.key} - "
            f"{errors_url.format(SH_URL, source_job.key)}\n"
            f"{errors_count2} error(s) for {target_job.key} - "
            f"{errors_url.format(SH_URL, target_job.key)}"
        )
        result.add_error(f"{errors_count1} and {errors_count2} errors", detailed_msg)
    return result


def compare_number_of_scraped_items(source_job: Job, target_job: Job) -> Result:
    items_count1 = api.get_items_count(source_job)
    items_count2 = api.get_items_count(target_job)
    diff = helpers.ratio_diff(items_count1, items_count2)
    result = Result("Total Scraped Items")
    if 0 <= diff < 0.05:
        if diff == 0:
            msg = "Same number of items"
        else:
            msg = f"Almost the same number of items - {items_count1} and {items_count2}"
        result.add_info(msg)
    else:
        msg = f"{items_count1} differs from {items_count2} on {diff * 100}%"
        if 0.05 <= diff < 0.10:
            result.add_warning(msg)
        elif diff >= 0.10:
            result.add_error(msg)
    return result


def compare_spider_names(source_job: Job, target_job: Job) -> Result:
    name1 = source_job.metadata.get("spider")
    name2 = target_job.metadata.get("spider")

    result = Result("Spider Names")
    if name1 != name2:
        result.add_warning(
            f"{source_job.key} spider is {name1}, {target_job.key} spider is {name2}"
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

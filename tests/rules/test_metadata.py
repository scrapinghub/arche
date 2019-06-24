from arche import SH_URL
from arche.rules.metadata import (
    check_errors,
    check_outcome,
    compare_finish_time,
    compare_response_ratio,
)
from arche.rules.result import Level
from conftest import create_result, Job
import pytest


error_input = [
    (
        {"log_count/ERROR": 5},
        {
            Level.ERROR: [
                (
                    f"5 error(s) - {SH_URL}/112358/13/21/log?filterType=error&filterAndHigher",
                )
            ]
        },
    ),
    ({}, {}),
    ({"log_count/ERROR": 0}, {}),
]


@pytest.mark.parametrize("error_count, expected_messages", error_input)
def test_check_errors(get_job, error_count, expected_messages):
    job = get_job
    job.metadata = {"scrapystats": error_count}
    job.key = "112358/13/21"

    assert check_errors(job) == create_result("Job Errors", expected_messages)


outcome_input = [
    (
        {"state": "finished", "close_reason": "cancelled"},
        {Level.ERROR: [("Job has 'finished' state, 'cancelled' close reason",)]},
    ),
    (
        {"state": "cancelled", "close_reason": "finished"},
        {Level.ERROR: [("Job has 'cancelled' state, 'finished' close reason",)]},
    ),
    (
        {"close_reason": "finished"},
        {Level.ERROR: [("Job has 'None' state, 'finished' close reason",)]},
    ),
    (
        {"close_reason": "no_reason"},
        {Level.ERROR: [("Job has 'None' state, 'no_reason' close reason",)]},
    ),
    (
        {"state": "cancelled"},
        {Level.ERROR: [("Job has 'cancelled' state, 'None' close reason",)]},
    ),
    (
        {"state": "finished"},
        {Level.ERROR: [("Job has 'finished' state, 'None' close reason",)]},
    ),
    ({}, {Level.ERROR: [("Job has 'None' state, 'None' close reason",)]}),
    ({"state": "finished", "close_reason": "finished"}, {}),
]


@pytest.mark.parametrize("metadata, expected_messages", outcome_input)
def test_check_outcome(get_job, metadata, expected_messages):
    job = get_job
    job.metadata = metadata

    result = check_outcome(job)
    assert result == create_result("Job Outcome", expected_messages)


time_inputs = [
    (
        {
            "state": "finished",
            "close_reason": "finished",
            "finished_time": 1_534_828_902_196,
        },
        {
            "state": "finished",
            "close_reason": "finished",
            "finished_time": 1_534_828_902_196,
        },
        {Level.INFO: [("Less than 1 day difference",)]},
    ),
    (
        {"state": "running", "finished_time": 1_534_838_902_196},
        {
            "state": "finished",
            "close_reason": "finished",
            "finished_time": 1_534_838_902_196,
        },
        {Level.WARNING: [("Jobs are not finished",)]},
    ),
    (
        {
            "state": "finished",
            "close_reason": "finished",
            "finished_time": 1_534_828_902_196,
        },
        {
            "state": "finished",
            "close_reason": "finished",
            "finished_time": 1_554_858_902_196,
        },
        {Level.WARNING: [("19 day(s) difference between 2 jobs",)]},
    ),
]


@pytest.mark.parametrize(
    "source_metadata, target_metadata, expected_messages", time_inputs
)
def test_compare_finish_time(
    get_jobs, source_metadata, target_metadata, expected_messages
):
    source_job, target_job = get_jobs

    source_job.metadata = source_metadata
    target_job.metadata = target_metadata

    result = compare_finish_time(source_job, target_job)
    assert result == create_result("Finish Time", expected_messages)


compare_response_ratio_inputs = [
    (
        {"totals": {"input_values": 1000}},
        {"scrapystats": {"downloader/response_count": 2000}},
        {"totals": {"input_values": 1000}},
        {"scrapystats": {"downloader/response_count": 2000}},
        {},
    ),
    (
        {"totals": {"input_values": 1000}},
        {"scrapystats": {"downloader/response_count": 2000}},
        {"totals": {"input_values": 1000}},
        {"scrapystats": {"downloader/response_count": 4000}},
        {Level.ERROR: [("Difference is 50.00% - 2.0 and 4.0",)]},
    ),
    (
        {"totals": {"input_values": 1000}},
        {"scrapystats": {"downloader/response_count": 2000}},
        {"totals": {"input_values": 1000}},
        {"scrapystats": {"downloader/response_count": 2300}},
        {Level.WARNING: [("Difference is 13.00% - 2.0 and 2.3",)]},
    ),
]


@pytest.mark.parametrize(
    "source_stats, source_metadata, target_stats, target_metadata, expected_messages",
    compare_response_ratio_inputs,
)
def test_compare_response_ratio(
    source_stats, source_metadata, target_stats, target_metadata, expected_messages
):
    source_job = Job(stats=source_stats, metadata=source_metadata)
    target_job = Job(stats=target_stats, metadata=target_metadata)

    result = compare_response_ratio(source_job, target_job)
    assert result == create_result(
        "Compare Responses Per Item Ratio", expected_messages
    )

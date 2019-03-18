from arche.tools import api


def generate_quality_estimation(
    job,
    crawlera_user,
    no_of_validation_warnings,
    no_of_duplicated_items,
    checked_dup_items_count,
    no_of_duplicated_skus,
    no_of_checked_skus_items,
    no_of_price_warns,
    no_of_checked_price_items,
    tested,
    **kwargs
):
    no_of_scraped_items = api.get_items_count(job)
    no_of_errors = api.get_errors_count(job)

    job_state = api.get_job_state(job)
    job_close_reason = api.get_job_close_reason(job)
    response_status_count = api.get_response_status_count(job)

    adherence_to_schema_percent = float(
        get_adherence_to_schema_percent(no_of_validation_warnings, no_of_scraped_items)
    )
    duplicated_items_percent = float(
        get_duplicated_items_percent(no_of_duplicated_items, no_of_scraped_items)
    )
    duplicated_skus_percent = float(
        get_duplicated_skus_percent(no_of_duplicated_skus, no_of_scraped_items)
    )

    crawlera_incapsula_percent = float(get_crawlera_incapsula_percent(crawlera_user))

    no_of_errors_percent = float(get_errors_count_percent(no_of_errors))
    price_was_price_now_comparison_percent = float(
        get_price_was_price_now_comparison_percent(
            no_of_price_warns, no_of_scraped_items
        )
    )
    outcome_percent = float(get_outcome_percent(job_state, job_close_reason))
    response_status_count_percent = float(
        get_response_status_count_percent(response_status_count)
    )
    tested_percent = float(get_tested_percent(tested))

    if all(
        [
            checked_dup_items_count == 0,
            no_of_checked_skus_items == 0,
            no_of_checked_price_items == 0,
        ]
    ):
        quality_estimation = (
            adherence_to_schema_percent * 60 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    elif checked_dup_items_count == 0 and no_of_checked_skus_items == 0:
        quality_estimation = (
            adherence_to_schema_percent * 55 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + price_was_price_now_comparison_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    elif checked_dup_items_count == 0 and no_of_checked_price_items == 0:
        quality_estimation = (
            adherence_to_schema_percent * 55 / 100
            + duplicated_skus_percent * 5 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    elif no_of_checked_skus_items == 0 and no_of_checked_price_items == 0:
        quality_estimation = (
            adherence_to_schema_percent * 50 / 100
            + duplicated_items_percent * 10 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    elif checked_dup_items_count == 0:
        quality_estimation = (
            adherence_to_schema_percent * 50 / 100
            + duplicated_skus_percent * 5 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + price_was_price_now_comparison_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    elif no_of_checked_skus_items == 0:
        quality_estimation = (
            adherence_to_schema_percent * 45 / 100
            + duplicated_items_percent * 10 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + price_was_price_now_comparison_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    elif no_of_checked_price_items == 0:
        quality_estimation = (
            adherence_to_schema_percent * 45 / 100
            + duplicated_items_percent * 10 / 100
            + duplicated_skus_percent * 5 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )
    else:
        quality_estimation = (
            adherence_to_schema_percent * 40 / 100
            + duplicated_items_percent * 10 / 100
            + duplicated_skus_percent * 5 / 100
            + crawlera_incapsula_percent * 8 / 100
            + no_of_errors_percent * 5 / 100
            + price_was_price_now_comparison_percent * 5 / 100
            + outcome_percent * 5 / 100
            + response_status_count_percent * 7 / 100
            + tested_percent * 15 / 100
        )

    field_accuracy = adherence_to_schema_percent * 100 / 100

    for rule_result in kwargs.values():
        if rule_result.err_items_count / rule_result.items_count < 0.1:
            quality_estimation = quality_estimation * 0.95
        else:
            quality_estimation = quality_estimation * 0.90

    return int(quality_estimation), int(field_accuracy)


def check_percentage(rule, scraped_items):
    return float(rule) / scraped_items * 100


def get_adherence_to_schema_percent(rule_result, no_of_scraped_items):
    if rule_result == 0:
        percent = 100
    elif 0 < check_percentage(float(rule_result), no_of_scraped_items) <= 2:
        percent = 75
    elif 2 < check_percentage(float(rule_result), no_of_scraped_items) <= 4:
        percent = 50
    elif 4 < check_percentage(float(rule_result), no_of_scraped_items) <= 8:
        percent = 25
    else:
        percent = 0
    return percent


def get_duplicated_items_percent(rule_result, no_of_scraped_items):
    if rule_result == 0:
        percent = 100
    elif 0 < check_percentage(float(rule_result), no_of_scraped_items) <= 5:
        percent = 75
    elif 5 < check_percentage(float(rule_result), no_of_scraped_items) <= 10:
        percent = 50
    elif 10 < check_percentage(float(rule_result), no_of_scraped_items) <= 20:
        percent = 25
    else:
        percent = 0
    return percent


def get_duplicated_skus_percent(rule_result, no_of_scraped_items):
    if rule_result == 0:
        percent = 100
    elif 0 < check_percentage(float(rule_result), no_of_scraped_items) <= 5:
        percent = 75
    elif 5 < check_percentage(float(rule_result), no_of_scraped_items) <= 10:
        percent = 50
    elif 10 < check_percentage(float(rule_result), no_of_scraped_items) <= 20:
        percent = 25
    else:
        percent = 0
    return percent


def get_crawlera_incapsula_percent(crawlera_user):
    """Having crawlera/incapsula enabled makes spider more unstable"""
    if crawlera_user:
        return 0
    else:
        return 100


def get_errors_count_percent(rule_result):
    if int(rule_result) == 0:
        percent = 100
    elif 0 < int(rule_result) <= 5:
        percent = 50
    elif 5 < int(rule_result) <= 10:
        percent = 20
    else:
        percent = 0
    return percent


def get_price_was_price_now_comparison_percent(rule_result, no_of_scraped_items):
    if rule_result == 0:
        percent = 100
    elif 0 < check_percentage(float(rule_result), no_of_scraped_items) <= 5:
        percent = 50
    else:
        percent = 0
    return percent


def get_outcome_percent(job_state, job_close_reason):
    if all(
        [job_state.lower() == "finished", str(job_close_reason).lower() == "finished"]
    ):
        percent = 100
    else:
        percent = 0
    return percent


def get_response_status_count_percent(rule_result):
    if rule_result[1] == rule_result[2] == rule_result[3] == 0:
        percent = 100
    elif (
        0
        < check_percentage(
            rule_result[1] + rule_result[2] + rule_result[3], rule_result[0]
        )
        <= 1
    ):
        percent = 100
    elif (
        1
        < check_percentage(
            rule_result[1] + rule_result[2] + rule_result[3], rule_result[0]
        )
        <= 5
    ):
        percent = 50
    elif (
        5
        < check_percentage(
            rule_result[1] + rule_result[2] + rule_result[3], rule_result[0]
        )
        <= 10
    ):
        percent = 20
    else:
        percent = 0
    return percent


def get_tested_percent(rule_result):
    if rule_result:
        percent = 100
    else:
        percent = 0
    return percent

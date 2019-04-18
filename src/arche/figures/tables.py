from arche import SH_URL
from arche.tools import api, helpers
import pandas as pd
import plotly.graph_objs as go


def score_table(quality_estimation, field_accuracy) -> go.FigureWidget:
    cells = [
        ["<b>Field Accuracy Score</b>", "<b>Overall Quality Score</b>"],
        ["<b>" + str(field_accuracy) + "<b>", "<b>" + str(quality_estimation) + "</b>"],
    ]

    font = dict(color="black", size=20)
    trace = go.Table(
        header=dict(
            values=[cells[0][0], cells[1][0]], fill=dict(color="gray"), font=font
        ),
        cells=dict(
            values=[cells[0][1:], cells[1][1:]],
            fill=dict(color=[[get_color(quality_estimation)]]),
            font=font,
        ),
    )

    layout = go.Layout(autosize=True, margin=dict(l=0, t=25, b=25, r=0), height=150)
    return go.FigureWidget(data=[trace], layout=layout)


def get_color(value):
    if value > 79:
        return "rgb(112,194,99)"
    if 65 < value < 80:
        return "rgb(233,190,50)"
    return "rgb(233,81,51)"


def job_summary_table(job) -> go.FigureWidget:
    job_url = f"{SH_URL}/{job.key}"
    job_state = api.get_job_state(job)
    job_close_reason = api.get_job_close_reason(job)
    no_of_scraped_items = api.get_items_count(job)
    no_of_errors = api.get_errors_count(job)

    job_runtime = api.get_runtime_s(job) / 1000
    run_time = helpers.ms_to_time(job_runtime)
    crawling_speed = round(job_runtime / 60 / no_of_scraped_items, 3)

    request_success_ratio = round(
        api.get_requests_count(job) / float(no_of_scraped_items), 2
    )

    max_memusage = api.get_max_memusage(job)
    response_status_count = api.get_response_status_count(job)

    crawlera_stat_value = api.get_crawlera_user(job)
    if not crawlera_stat_value:
        crawlera_stat_value = "Not Used"

    job_stats_values = [
        "Job URL",
        "Spider State",
        "Spider Close Reason",
        "Number of Scraped Items",
        "Number of Errors",
        "Runtime",
        "Request Success Ratio [requests/scraped items]",
        "Crawling Speed [items/min]",
        "Crawlera user",
        "Max Memory Usage [Bytes]",
        "Response Status Count",
    ]
    stats_values = [
        '<a href="' + job_url + '">' + job_url + "</a>",
        job_state,
        job_close_reason,
        no_of_scraped_items,
        no_of_errors,
        run_time,
        request_success_ratio,
        crawling_speed,
        crawlera_stat_value,
        max_memusage,
        "200: "
        + str(response_status_count[0])
        + "<br>"
        + "301: "
        + str(response_status_count[1])
        + "<br>"
        + "404: "
        + str(response_status_count[2])
        + "<br>"
        + "503: "
        + str(response_status_count[3])
        + "<br>",
    ]

    trace = go.Table(
        columnorder=[1, 2],
        columnwidth=[300, 200],
        header=dict(
            values=["<b>Job Stat</b>", "<b>Stat Value</b>"],
            fill=dict(color="gray"),
            align=["left"] * 5,
            font=dict(color="black", size=14),
            height=30,
        ),
        cells=dict(
            values=[job_stats_values, stats_values],
            fill=dict(color="lightgrey"),
            font=dict(color="black", size=12),
            height=25,
            align=["left"] * 5,
        ),
    )
    spider = job.metadata.get("spider")
    layout = go.Layout(
        title=f"Summary for spider {spider}",
        autosize=True,
        margin=dict(t=40, b=25, l=0, r=0),
        height=445,
    )

    return go.FigureWidget(data=[trace], layout=layout)


def rules_summary_table(
    df,
    no_of_validation_warnings,
    name_field,
    url_field,
    no_of_checked_duplicated_items,
    no_of_duplicated_items,
    unique,
    no_of_checked_skus,
    no_of_duplicated_skus,
    price_field,
    price_was_field,
    no_of_checked_price_items,
    no_of_price_warns,
    **kwargs,
) -> go.FigureWidget:
    test_name_values = ["Adherence to schema"]
    tested_fields_values = ["All scraped fields" for i in range(1)]
    test_results_values = [f"{no_of_validation_warnings} warnings"]
    status_values = [get_rule_status(no_of_validation_warnings)]

    if no_of_checked_duplicated_items:
        test_name_values.append("Duplicated Items")
        tested_fields_values.append(f"{name_field}, {url_field}")
        test_results_values.append(f"{no_of_duplicated_items} warnings")
        status_values.append(get_rule_status(no_of_duplicated_items))

    if no_of_checked_skus:
        test_name_values.append("Duplicated Field Values")
        tested_fields_values.append(unique)
        test_results_values.append(f"{no_of_duplicated_skus} warnings")
        status_values.append(get_rule_status(no_of_duplicated_skus))

    if no_of_checked_price_items:
        test_name_values.append("Prices comparison")
        tested_fields_values.append(f"{price_field}, {price_was_field}")
        test_results_values.append(f"{no_of_price_warns} warnings")
        status_values.append(get_rule_status(no_of_price_warns))

    for result in kwargs.values():
        test_name_values.append(result.name)
        tested_fields_values.append("All scraped fields")
        results = f"{result.err_items_count} warnings ({len(df)} items checked)"
        test_results_values.append(results)
        status_values.append("Fail" if result.err_items_count else "Pass")

    values = {
        "Test Name": test_name_values,
        "Tested Fields": tested_fields_values,
        "Result": test_results_values,
        "Status": status_values,
    }
    df = pd.DataFrame.from_dict(values)
    df["Color"] = df.apply(
        lambda row: "rgb(233,81,51)" if row.Status == "Fail" else "rgb(112,194,99)",
        axis="columns",
    )

    trace = go.Table(
        columnwidth=[100, 230, 140, 40],
        header=dict(
            values=list(values.keys()),
            fill=dict(color="gray"),
            align="left",
            font=dict(color="black", size=14),
        ),
        cells=dict(
            values=[df["Test Name"], df["Tested Fields"], df["Result"], df["Status"]],
            fill=dict(color=["lightgrey", "lightgrey", "lightgrey", df["Color"]]),
            font=dict(color="black", size=12),
            align="left",
            height=25,
        ),
    )

    layout = go.Layout(
        title="Test Summary",
        autosize=True,
        margin=dict(t=25, b=25, l=0, r=0),
        height=100 + len(df.index) * 25,
    )
    return go.FigureWidget(data=[trace], layout=layout)


def get_rule_status(err_values_number):
    if err_values_number:
        return "Fail"
    return "Pass"


def coverage_by_categories(category_field, df, product_url_fields) -> go.FigureWidget:
    if category_field not in df.columns:
        return None
    if df[category_field].notnull().sum() == 0:
        return None

    cat_grouping = (
        df.groupby(category_field)[category_field]
        .count()
        .sort_values(ascending=False)
        .head(20)
    )
    category_values = cat_grouping.values
    category_names = cat_grouping.index

    if product_url_fields is not None and product_url_fields[0] in df.columns:
        product_url_field = product_url_fields[0]
        category_urls = [
            df[df[category_field] == cat][product_url_field].head(1).values[0]
            for cat in category_names
        ]
        href_tag = '<a href="{}">{}</a>'
        category_names = [
            href_tag.format(link, cat)
            for cat, link in zip(category_names, category_urls)
        ]

    trace = go.Table(
        columnorder=[1, 2],
        columnwidth=[400, 80],
        header=dict(
            values=[f"CATEGORY", "SCRAPED ITEMS"],
            fill=dict(color="gray"),
            align=["left"] * 5,
            font=dict(color="white", size=12),
            height=30,
        ),
        cells=dict(
            values=[category_names, category_values],
            fill=dict(color="lightgrey"),
            font=dict(color="black", size=12),
            height=30,
            align="left",
        ),
    )
    layout = go.Layout(
        title=f"Top 20 Categories for '{category_field}'",
        autosize=True,
        margin=dict(t=30, b=25, l=0, r=0),
        height=(len(category_names) + 2) * 45,
    )
    return go.FigureWidget(data=[trace], layout=layout)

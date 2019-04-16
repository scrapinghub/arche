from arche.readers.schema import TaggedFields
from arche.rules.result import Result
from arche.tools.helpers import is_number, ratio_diff
import pandas as pd


def compare_was_now(df: pd.DataFrame, tagged_fields: TaggedFields):
    """Compare price_was and price_now tagged fields"""

    price_was_fields = tagged_fields.get("product_price_was_field")
    price_fields = tagged_fields.get("product_price_field")
    items_number = len(df.index)

    result = Result("Compare Price Was And Now")

    if (
        price_was_fields
        and price_was_fields[0] in df.columns
        and price_fields
        and price_fields[0] in df.columns
    ):
        price_field = price_fields[0]
        price_was_field = price_was_fields[0]
        prices = df.copy()
        prices[price_was_field] = prices[price_was_field].astype(float)
        prices[price_field] = prices[price_field].astype(float)

        df_prices_less = pd.DataFrame(
            prices[prices[price_was_field] < prices[price_field]],
            columns=["_key", price_was_field, price_field],
        )

        price_less_percent = "{:.2%}".format(len(df_prices_less) / items_number)

        if not df_prices_less.empty:
            error = f"Past price is less than current for {len(df_prices_less)} items"
            result.add_error(
                f"{price_less_percent} ({len(df_prices_less)}) of "
                f"items with {price_was_field} < {price_field}",
                detailed=f"{error}:\n{list(df_prices_less['_key'])}",
            )

        df_prices_equals = pd.DataFrame(
            prices[prices[price_was_field] == prices[price_field]],
            columns=["_key", price_was_field, price_field],
        )
        price_equal_percent = "{:.2%}".format(len(df_prices_equals) / items_number)

        if not df_prices_equals.empty:
            result.add_warning(
                (
                    f"{price_equal_percent} ({len(df_prices_equals)}) "
                    f"of items with {price_was_field} = {price_field}"
                ),
                detailed=(
                    f"Prices equal for {len(df_prices_equals)} items:\n"
                    f"{list(df_prices_equals['_key'])}"
                ),
            )

        result.err_items_count = len(df_prices_equals) + len(df_prices_less)
        result.items_count = len(df.index)

    else:
        result.add_info(
            "product_price_field or product_price_was_field tags were not "
            "found in schema"
        )
    return result


def compare_prices_for_same_urls(
    source_df: pd.DataFrame, target_df: pd.DataFrame, tagged_fields: TaggedFields
):
    """For each pair of items that have the same `product_url_field` tagged field,
    compare `product_price_field` field

    Returns:
        A result containing pairs of items with same `product_url_field`
        from `source_df` and `target_df` which `product_price_field` differ,
        missing and new `product_url_field` tagged fields.
    """
    result = Result("Compare Prices For Same Urls")
    url_field = tagged_fields.get("product_url_field")
    if not url_field:
        result.add_info("product_url_field tag is not set")
        return result

    url_field = url_field[0]
    price_field = tagged_fields.get("product_price_field")

    source_df = source_df.dropna(subset=[url_field])
    target_df = target_df.dropna(subset=[url_field])

    same_urls = source_df[(source_df[url_field].isin(target_df[url_field].values))][
        url_field
    ]
    new_urls = source_df[~(source_df[url_field].isin(target_df[url_field].values))][
        url_field
    ]
    missing_urls = target_df[(~target_df[url_field].isin(source_df[url_field].values))][
        url_field
    ]

    missing_detailed_messages = []
    for url in missing_urls:
        key = target_df.loc[target_df[url_field] == url]["_key"].iloc[0]
        missing_detailed_messages.append(f"Missing {url} from {key}")

    result.add_info(
        f"{len(missing_urls)} urls missing from the tested job",
        detailed="\n".join(missing_detailed_messages),
    )
    result.add_info(f"{len(new_urls)} new urls in the tested job")
    result.add_info(f"{len(same_urls)} same urls in both jobs")

    diff_prices_count = 0
    if not price_field:
        result.add_info("product_price_field tag is not set")
    else:
        price_field = price_field[0]
        detailed_messages = []
        for url in same_urls:
            if url.strip() != "nan":
                source_price = source_df[source_df[url_field] == url][price_field].iloc[
                    0
                ]
                target_price = target_df[target_df[url_field] == url][price_field].iloc[
                    0
                ]

                if (
                    is_number(source_price)
                    and is_number(target_price)
                    and ratio_diff(source_price, target_price) > 0.1
                ):
                    diff_prices_count += 1
                    source_key = source_df[source_df[url_field] == url]["_key"].iloc[0]
                    target_key = target_df[target_df[url_field] == url]["_key"].iloc[0]
                    msg = (
                        f"different prices for url: {url}\nsource price is {source_price} "
                        f"for {source_key}\ntarget price is {target_price} for {target_key}"
                    )
                    detailed_messages.append(msg)

        res = f"{len(same_urls)} checked, {diff_prices_count} errors"
        if detailed_messages:
            result.add_error(res, detailed="\n".join(detailed_messages))
        else:
            result.add_info(res)

    return result


def compare_names_for_same_urls(
    source_df: pd.DataFrame, target_df: pd.DataFrame, tagged_fields: TaggedFields
):
    """For each pair of items that have the same `product_url_field` tagged field,
    compare `name_field` field"""

    result = Result("Compare Names Per Url")
    url_field = tagged_fields.get("product_url_field")
    if not url_field:
        result.add_info("product_url_field tag is not set")
        return result

    url_field = url_field[0]
    name_field = tagged_fields.get("name_field")

    diff_names_count = 0
    if not name_field:
        result.add_info("name_field tag is not set")
        return result

    name_field = name_field[0]
    if any(
        [
            name_field not in source_df.columns.values,
            name_field not in target_df.columns.values,
        ]
    ):
        return

    same_urls = source_df[(source_df[url_field].isin(target_df[url_field].values))][
        url_field
    ]

    detailed_messages = []
    for url in same_urls:
        if url.strip() != "nan":
            source_name = source_df[source_df[url_field] == url][name_field].iloc[0]
            target_name = target_df[target_df[url_field] == url][name_field].iloc[0]

            if (
                source_name != target_name
                and source_name.strip() != "nan"
                and target_name.strip() != "nan"
            ):
                diff_names_count += 1
                source_key = source_df[source_df[url_field] == url]["_key"].iloc[0]
                target_key = target_df[target_df[url_field] == url]["_key"].iloc[0]
                msg = (
                    f"different names for url: {url}\nsource name is {source_name} "
                    f"for {source_key}\ntarget name is {target_name} for {target_key}"
                )
                detailed_messages.append(msg)

    res = f"{len(same_urls)} checked, {diff_names_count} errors"
    if detailed_messages:
        result.add_error(res, detailed="\n".join(detailed_messages))
    else:
        result.add_info(res)

    return result


def compare_prices_for_same_names(
    source_df: pd.DataFrame, target_df: pd.DataFrame, tagged_fields: TaggedFields
):
    result = Result("Compare Prices For Same Names")
    name_field = tagged_fields.get("name_field")
    if not name_field:
        result.add_info("name_field tag is not set")
        return result

    name_field = name_field[0]

    product_url_field = tagged_fields.get("product_url_field")
    if not product_url_field:
        result.add_info("product_url_field tag is not set")
    else:
        product_url_field = product_url_field[0]
    source_df = source_df[source_df[name_field].notnull()]
    target_df = target_df[target_df[name_field].notnull()]

    same_names = source_df[(source_df[name_field].isin(target_df[name_field].values))][
        name_field
    ]
    new_names = source_df[~(source_df[name_field].isin(target_df[name_field].values))][
        name_field
    ]
    missing_names = target_df[
        ~(target_df[name_field].isin(source_df[name_field].values))
    ][name_field]

    detailed_messages = []
    for name in missing_names:
        target_key = target_df.loc[target_df[name_field] == name]["_key"].iloc[0]
        msg = f"Missing {name} from {target_key}"
        if product_url_field:
            url = target_df.loc[target_df[name_field] == name][product_url_field].iloc[
                0
            ]
            detailed_messages.append(f"{msg}\n{url}")

    result.add_info(
        f"{len(missing_names)} names missing from the tested job",
        detailed="\n".join(detailed_messages),
    )
    result.add_info(f"{len(new_names)} new names in the tested job")
    result.add_info(f"{len(same_names)} same names in both jobs")

    price_tag = "product_price_field"
    price_field = tagged_fields.get(price_tag)
    if not price_field:
        result.add_info("product_price_field tag is not set")
        return result

    price_field = price_field[0]
    count = 0

    detailed_messages = []
    for name in same_names:
        if name.strip() != "nan":
            source_price = source_df[source_df[name_field] == name][price_field].iloc[0]
            target_price = target_df[target_df[name_field] == name][price_field].iloc[0]
            if is_number(source_price) and is_number(target_price):
                if ratio_diff(source_price, target_price) > 0.1:
                    count += 1
                    source_key = source_df[source_df[name_field] == name]["_key"].iloc[
                        0
                    ]
                    target_key = target_df[target_df[name_field] == name]["_key"].iloc[
                        0
                    ]
                    msg = (
                        f"different price for {name}\nsource price is {source_price} "
                        f"for {source_key}\ntarget price is {target_price} for {target_key}"
                    )
                    detailed_messages.append(msg)

    result_msg = f"{len(same_names)} checked, {count} errors"
    if detailed_messages:
        result.add_error(result_msg, detailed="\n".join(detailed_messages))
    else:
        result.add_info(result_msg)

    return result

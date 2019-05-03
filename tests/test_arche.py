from arche import arche
from arche.arche import Arche
from arche.rules.result import Result
from conftest import get_job_items_mock
import pandas as pd
import pytest


def test_target_equals_source():
    with pytest.raises(ValueError) as excinfo:
        Arche(source="0/0/1", target="0/0/1")
    assert (
        str(excinfo.value)
        == "'target' is equal to 'source'. Data to compare should have different sources."
    )


def test_target_items(mocker, get_job_items):
    mocker.patch("arche.Arche.get_items", return_value=get_job_items)
    arche = Arche("source", target="target")
    assert arche.target_items is get_job_items
    assert arche._target_items is get_job_items
    assert arche.target_items is get_job_items


def test_target_items_none(mocker):
    arche = Arche("source")
    assert arche.target_items is None


def test_arche_df(get_df):
    a = Arche(source=get_df, target=get_df)
    pd.testing.assert_frame_equal(a.source_items.df, get_df)
    pd.testing.assert_frame_equal(a.target_items.df, get_df)


schema_dummies = [
    {"$schema": "http://json-schema.org/draft-07/schema"},
    {"$schema": "http://json-schema.org/draft-06/schema"},
]


@pytest.mark.parametrize(
    "passed_schema_source, set_schema_source, expected_schema",
    [
        (schema_dummies[0], None, schema_dummies[0]),
        (None, schema_dummies[1], schema_dummies[1]),
        (schema_dummies[1], schema_dummies[0], schema_dummies[0]),
        (None, None, None),
    ],
)
def test_schema(passed_schema_source, set_schema_source, expected_schema):
    arche = Arche("source", schema=passed_schema_source)
    assert arche._schema == passed_schema_source
    assert arche.schema_source == passed_schema_source
    if set_schema_source:
        arche.schema = set_schema_source
        assert arche.schema_source == set_schema_source
    assert arche.schema == expected_schema


@pytest.mark.parametrize(
    "source, start, count, filters, expand", [("112358/13/21", 1, 50, None, False)]
)
def test_get_items(mocker, get_items, source, start, count, filters, expand):
    mocker.patch(
        "arche.readers.items.JobItems.fetch_data", return_value=get_items, autospec=True
    )
    items = Arche.get_items(
        source=source, start=start, count=count, filters=filters, expand=expand
    )
    assert items.key == source
    assert items.count == count
    assert items.filters == filters
    assert items.expand == expand
    assert items.start_index == start


@pytest.mark.parametrize(
    "source, count, filters, expand", [("112358/collections/s/pages", 5, None, True)]
)
def test_get_items_from_collection(mocker, get_items, source, count, filters, expand):
    mocker.patch(
        "arche.readers.items.CollectionItems.fetch_data",
        return_value=get_items,
        autospec=True,
    )
    items = Arche.get_items(
        source=source, count=count, start=0, filters=filters, expand=expand
    )
    assert items.key == source
    assert items.count == 5
    assert items.filters == filters
    assert items.expand == expand


def test_get_items_start():
    with pytest.raises(ValueError) as excinfo:
        Arche.get_items(
            source="112358/collections/s/pages",
            count=1,
            start=1,
            filters=None,
            expand=None,
        )
    assert str(excinfo.value) == "Collections API does not support 'start' parameter"


def test_get_items_from_bad_source():
    with pytest.raises(ValueError) as excinfo:
        Arche.get_items(source="bad_key", count=1, start=1, filters=None, expand=None)
    assert str(excinfo.value) == f"'bad_key' is not a valid job or collection key"


def test_arche_dataframe():
    a = Arche(
        pd.DataFrame({"c": [0, 1]}), schema={"properties": {"c": {"type": "string"}}}
    )
    a.report_all()
    assert a.report.results.get("JSON Schema Validation").detailed_messages_count == 1
    assert not a.report.results.get("Garbage Symbols").detailed_messages_count
    assert a.report.results.get("Fields Coverage").detailed_messages_count == 1
    assert not a.report.results.get("Tags").detailed_messages_count
    assert not a.report.results.get("Compare Price Was And Now").detailed_messages_count
    assert not a.report.results.get("Duplicated Items").detailed_messages_count
    assert not a.report.results.get(
        "Coverage For Scraped Categories"
    ).detailed_messages_count
    Arche(
        pd.DataFrame({"_key": ["0", "1"], "c": [0, 1]}),
        schema={"properties": {"c": {"type": "string"}}},
    ).report_all()


def test_report_all(mocker):
    mocked_run_all = mocker.patch("arche.Arche.run_all_rules", autospec=True)
    mocked_write_summary = mocker.patch(
        "arche.report.Report.write_summaries", autospec=True
    )
    # autospec and classmethod bug https://github.com/python/cpython/pull/11613
    mocked_write = mocker.patch("arche.report.Report.write", autospec=False)
    mocked_write_details = mocker.patch(
        "arche.report.Report.write_details", autospec=True
    )

    arche = Arche("source")
    arche.report_all()

    mocked_run_all.assert_called_once_with(arche)
    mocked_write_summary.assert_called_once_with(arche.report)
    mocked_write.assert_called_once_with("\n" * 2)
    mocked_write_details.assert_called_once_with(arche.report, short=True)


@pytest.mark.parametrize("source_key, target_key", [("112358/13/21", "112358/13/20")])
def test_run_all_rules_job(mocker, source_key, target_key):
    mocked_check_metadata = mocker.patch("arche.Arche.check_metadata", autospec=True)
    mocked_compare_metadata = mocker.patch(
        "arche.Arche.compare_metadata", autospec=True
    )

    mocked_run_general_rules = mocker.patch(
        "arche.Arche.run_general_rules", autospec=True
    )
    mocked_run_comparison_rules = mocker.patch(
        "arche.Arche.run_comparison_rules", autospec=True
    )
    mocked_run_schema_rules = mocker.patch(
        "arche.Arche.run_schema_rules", autospec=True
    )
    arche = Arche(source=source_key, target=target_key)
    arche._source_items = get_job_items_mock(mocker, key=source_key)
    arche._target_items = get_job_items_mock(mocker, key=target_key)
    arche.run_all_rules()

    mocked_check_metadata.assert_called_once_with(arche.source_items.job)
    mocked_compare_metadata.assert_called_once_with(
        arche.source_items.job, arche.target_items.job
    )
    mocked_run_general_rules.assert_called_once_with()
    mocked_run_comparison_rules.assert_called_once_with()
    mocked_run_schema_rules.assert_called_once_with(arche)


def test_run_all_rules_collection(mocker, get_collection_items):
    mocked_check_metadata = mocker.patch("arche.Arche.check_metadata", autospec=True)
    mocked_compare_metadata = mocker.patch(
        "arche.Arche.compare_metadata", autospec=True
    )

    mocked_run_general_rules = mocker.patch(
        "arche.Arche.run_general_rules", autospec=True
    )
    mocked_run_comparison_rules = mocker.patch(
        "arche.Arche.run_comparison_rules", autospec=True
    )
    mocked_run_schema_rules = mocker.patch(
        "arche.Arche.run_schema_rules", autospec=True
    )
    arche = Arche(source="collection_key")
    arche._source_items = get_collection_items
    arche.run_all_rules()

    mocked_check_metadata.assert_not_called()
    mocked_compare_metadata.assert_not_called()
    mocked_run_general_rules.assert_called_once_with()
    mocked_run_comparison_rules.assert_called_once_with()
    mocked_run_schema_rules.assert_called_once_with(arche)


def test_validate_with_json_schema(mocker, get_job_items):
    mocked_save_result = mocker.patch("arche.Arche.save_result", autospec=True)
    res = Result("fine")
    mocked_validate = mocker.patch(
        "arche.rules.json_schema.validate", autospec=True, return_value=res
    )
    mocked_show = mocker.patch("arche.rules.result.Result.show", autospec=True)

    arche = Arche(
        "source", schema={"$schema": "http://json-schema.org/draft-07/schema"}
    )
    arche._source_items = get_job_items
    arche.validate_with_json_schema()

    mocked_validate.assert_called_once_with(arche.schema, arche.source_items.df, False)
    mocked_save_result.assert_called_once_with(arche, res)
    mocked_show.assert_called_once_with(res)


@pytest.mark.parametrize(
    "source, expected_message",
    [
        ("source", "Schema is empty"),
        ("112358/collections/s/pages", "Collections are not supported"),
    ],
)
def test_data_quality_report_fails(source, expected_message):
    with pytest.raises(ValueError) as excinfo:
        Arche(source).data_quality_report()
    assert str(excinfo.value) == expected_message


def test_data_quality_report(mocker, get_job_items):
    mocked_dqr = mocker.patch.object(
        arche, "DataQualityReport", autospec=True, return_value=None
    )

    g = Arche("source", schema={"$schema": "http://json-schema.org/draft-07/schema"})
    g._source_items = get_job_items
    g.report.results = "some_res"
    g.data_quality_report("s3")
    mocked_dqr.assert_called_with(g.source_items, g.schema, g.report, "s3")


def test_compare_with_customized_rules(mocker, get_job_items):
    mocked_save_result = mocker.patch("arche.Arche.save_result", autospec=True)
    mocked_coverage = mocker.patch("arche.rules.category.get_difference", autospec=True)
    mocked_price_url = mocker.patch(
        "arche.rules.price.compare_prices_for_same_urls", autospec=True
    )
    mocked_name_url = mocker.patch(
        "arche.rules.price.compare_names_for_same_urls", autospec=True
    )
    mocked_price_name = mocker.patch(
        "arche.rules.price.compare_prices_for_same_names", autospec=True
    )

    source_items = get_job_items
    target_items = get_job_items
    arche = Arche("source")
    arche.compare_with_customized_rules(source_items, target_items, {})

    mocked_coverage.assert_called_once_with(source_items.df, target_items.df, [])
    mocked_price_url.assert_called_once_with(source_items.df, target_items.df, {})
    mocked_name_url.assert_called_once_with(source_items.df, target_items.df, {})
    mocked_price_name.assert_called_once_with(source_items.df, target_items.df, {})

    assert mocked_save_result.call_count == 4


def test_compare_with_customized_rules_none_target(mocker, get_job_items):
    mocked_coverage = mocker.patch("arche.rules.category.get_difference", autospec=True)
    arche = Arche("key")
    assert not arche.compare_with_customized_rules(
        source_items=get_job_items, target_items=None, tagged_fields={}
    )
    mocked_coverage.assert_not_called()

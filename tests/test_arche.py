from arche import arche, SH_URL
from arche.arche import Arche
from arche.rules.result import Level
from conftest import create_result
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


schema_dummies = [{"properties": {"name": {}}}, {"properties": {"url": {}}}]


def test_schema():
    arche = Arche("source", schema=schema_dummies[0])
    assert arche.schema_source == schema_dummies[0]
    assert arche.schema.raw == schema_dummies[0]
    arche = Arche("source")
    assert not arche.schema_source
    assert not arche.schema


@pytest.mark.parametrize(
    "passed_schema_source, set_schema_source, expected_schema",
    [
        (None, schema_dummies[1], schema_dummies[1]),
        (schema_dummies[1], schema_dummies[0], schema_dummies[0]),
    ],
)
def test_schema_setter(passed_schema_source, set_schema_source, expected_schema):
    arche = Arche("source", schema=passed_schema_source)
    assert arche.schema_source == passed_schema_source
    arche.schema = set_schema_source
    assert arche.schema_source == set_schema_source
    assert arche.schema.raw == expected_schema


@pytest.mark.parametrize(
    "source, count, start, filters, expected_start",
    [
        ("112358/13/21", 50, 1, None, 1),
        ("112358/13/22", None, None, [("_type", ["CityItem"])], 0),
    ],
)
def test_get_items(
    mocker, get_raw_items, source, start, count, filters, expected_start
):
    mocker.patch(
        "arche.readers.items.JobItems.fetch_data",
        return_value=get_raw_items,
        autospec=True,
    )
    mocker.patch(
        "arche.readers.items.api.get_items_count",
        return_value=len(get_raw_items),
        autospec=True,
    )
    mocker.patch("arche.readers.items.JobItems.job", autospec=True)
    items = Arche.get_items(source=source, start=start, count=count, filters=filters)
    assert items.key == source
    assert items.count == count or len(get_raw_items)
    assert items.filters == filters
    assert items.start_index == expected_start
    assert items.start == f"{source}/{expected_start}"


def test_get_items_from_iterable(get_cloud_items):
    items = Arche.get_items(get_cloud_items, start=None, count=None, filters=None)
    assert items.raw == get_cloud_items


@pytest.mark.parametrize(
    "source, count, filters", [("112358/collections/s/pages", 5, None)]
)
def test_get_items_from_collection(mocker, get_raw_items, source, count, filters):
    mocker.patch(
        "arche.readers.items.CollectionItems.fetch_data",
        return_value=get_raw_items,
        autospec=True,
    )
    items = Arche.get_items(source=source, count=count, start=0, filters=filters)
    assert items.key == source
    assert items.count == 5
    assert items.filters == filters


def test_get_items_from_bad_source():
    with pytest.raises(ValueError) as excinfo:
        Arche.get_items(source="bad_key", count=1, start=1, filters=None)
    assert str(excinfo.value) == f"'bad_key' is not a valid job or collection key"


def test_arche_dataframe(mocker):
    a = Arche(
        source=pd.DataFrame({"c": [0, 1]}),
        schema={"properties": {"c": {"type": "integer"}}},
        target=pd.DataFrame({"c": [1, 1]}),
    )
    mocker.patch("arche.report.Report.write_details", autospec=True)
    a.report_all()
    executed = [
        "Garbage Symbols",
        "Fields Coverage",
        "Scraped Fields",
        "Boolean Fields",
        "JSON Schema Validation",
        "Tags",
        "Compare Price Was And Now",
        "Duplicates By **unique** Tag",
        "Duplicates By **name_field, product_url_field** Tags",
        "Coverage For Scraped Categories",
        "Category Coverage Difference",
        "Compare Prices For Same Urls",
        "Compare Names Per Url",
        "Compare Prices For Same Names",
    ]
    for e in executed:
        assert a.report.results.get(e)
    assert a.report.results.get("JSON Schema Validation").errors is None
    assert (
        a.report.results.get("JSON Schema Validation").info[0].summary
        == "2 items were checked, 0 error(s)"
    )
    assert (
        Arche(
            pd.DataFrame({"_key": ["0", "1"], "c": [0, 1]}),
            schema={"properties": {"c": {"type": "string"}}},
        ).report_all()
        is None
    )


def test_arche_dataframe_data_warning(caplog):
    Arche(pd.DataFrame())
    assert "Pandas stores `NA` (missing)" in caplog.text


def test_report_all(mocker, get_cloud_items):
    mocked_write_summaries = mocker.patch(
        "arche.report.Report.write_summaries", autospec=True
    )
    mocked_write = mocker.patch("arche.report.Report.write", autospec=True)

    source = pd.DataFrame(get_cloud_items)
    source["b"] = True
    a = Arche(source=source, target=pd.DataFrame(get_cloud_items[:2]))
    a.report_all()
    executed = {
        "Garbage Symbols",
        "Fields Coverage",
        "Scraped Fields",
        "Boolean Fields",
        "Categories",
    }
    mocked_write_summaries.assert_called_once_with(a.report)
    mocked_write.assert_called_once_with("\n" * 2)
    assert executed == a.report.results.keys()


def test_run_all_rules_job(mocker, get_cloud_items):
    a = Arche(
        source=pd.DataFrame(get_cloud_items), target=pd.DataFrame(get_cloud_items[:2])
    )
    a.run_all_rules()
    executed = {
        "Garbage Symbols",
        "Fields Coverage",
        "Scraped Fields",
        "Boolean Fields",
        "Categories",
    }

    assert executed == a.report.results.keys()


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


def test_validate_with_json_schema(mocker, get_job_items, get_schema):
    res = create_result(
        "JSON Schema Validation", {Level.INFO: [("4 items were checked, 0 error(s)",)]}
    )
    mocked_show = mocker.patch("arche.rules.result.Result.show", autospec=True)

    a = Arche("source", schema=get_schema)
    a._source_items = get_job_items
    a.validate_with_json_schema()

    mocked_show.assert_called_once_with(res)
    assert len(a.report.results) == 1
    assert a.report.results.get("JSON Schema Validation") == res


def test_validate_with_json_schema_fails(mocker, get_job_items, get_schema):
    mocked_md = mocker.patch("arche.report.display_markdown", autospec=True)
    url = f"{SH_URL}/112358/13/21/item/1"
    res = create_result(
        "JSON Schema Validation",
        {
            Level.ERROR: [
                (
                    "4 items were checked, 1 error(s)",
                    None,
                    {"'price' is a required property": {url}},
                )
            ]
        },
    )
    schema = {"type": "object", "required": ["price"], "properties": {"price": {}}}
    a = Arche("source", schema=schema)
    a._source_items = get_job_items
    a.validate_with_json_schema()

    assert len(a.report.results) == 1
    assert a.report.results.get("JSON Schema Validation") == res
    mocked_md.assert_any_call(
        f"1 items affected - 'price' is a required property: [1]({url})", raw=True
    )


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


def test_data_quality_report(mocker, get_job_items, get_schema):
    mocked_dqr = mocker.patch.object(
        arche, "DataQualityReport", autospec=True, return_value=None
    )

    g = Arche("source", schema=get_schema)
    g._source_items = get_job_items
    g.report.results = "some_res"
    g.data_quality_report("s3")
    mocked_dqr.assert_called_with(g.source_items, g.schema, g.report, "s3")


def test_compare_with_customized_rules_none_target(mocker, get_job_items):
    mocked_coverage = mocker.patch("arche.rules.category.get_difference", autospec=True)
    arche = Arche("key")
    assert not arche.compare_with_customized_rules(
        source_items=get_job_items, target_items=None, tagged_fields={}
    )
    mocked_coverage.assert_not_called()

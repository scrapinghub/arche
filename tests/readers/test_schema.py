from collections import defaultdict

from arche.readers.schema import Schema, set_auth
from jsonschema.exceptions import SchemaError
import pytest

good_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {"unicode_number": {"pattern": "^-?[0-9]{1,7}\\.[0-9]{2}$"}},
    "required": ["company"],
    "properties": {"company": {"pattern": "^Dell$"}},
}


@pytest.mark.parametrize(
    "source, downloaded, expected",
    [
        (good_schema, None, good_schema),
        ("s3://some/valid/path.json", good_schema, good_schema),
    ],
)
def test_read_schema(mocker, source, downloaded, expected):
    mocker.patch(
        "arche.readers.schema.Schema.from_url", return_value=downloaded, autospec=True
    )
    assert Schema.read(source) == expected


@pytest.mark.parametrize(
    "source, downloaded_schema, expected_error",
    [
        ("bad_source", {"types": "string"}, SchemaError),
        ({}, None, SchemaError),
        ({"propertie": {}}, None, SchemaError),
    ],
)
def test_read_schema_fails(mocker, source, downloaded_schema, expected_error):
    mocker.patch(
        "arche.readers.schema.Schema.from_url",
        return_value=downloaded_schema,
        autospec=True,
    )
    with pytest.raises(expected_error):
        Schema.read(source)


def test_get_schema_fails_on_type():
    with pytest.raises(ValueError) as excinfo:
        Schema.read(1)
    assert (
        str(excinfo.value)
        == '"1" is an unidentified schema source.\nA dict, a full s3 path or URL is expected'
    )


@pytest.mark.parametrize(
    "schema_path, schema_contents, expected",
    [
        (
            "s3://bucket/schema.json",
            '{"required": ["name", "address"]}',
            {"required": ["name", "address"]},
        ),
        (
            "https://bucket/schema.json",
            '{"required": ["address"]}',
            {"required": ["address"]},
        ),
    ],
)
def test_schema_from_url(schema_path, schema_contents, expected, mocker):
    mocker.patch(
        "arche.readers.schema.s3.get_contents",
        return_value=schema_contents,
        autospec=True,
    )
    assert Schema.from_url(schema_path) == expected


def test_schema_json(capsys):
    s = Schema(
        {
            "definitions": {"float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"}},
            "properties": {"name": {}},
            "additionalProperties": False,
        }
    )
    s.json()
    assert (
        capsys.readouterr().out
        == """{
    "definitions": {
        "float": {
            "pattern": "^-?[0-9]+\\\\.[0-9]{2}$"
        }
    },
    "properties": {
        "name": {}
    },
    "additionalProperties": false
}\n"""
    )


def test_schema_repr():
    assert Schema(
        {
            "definitions": {"float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"}},
            "properties": {"name": {}},
            "additionalProperties": False,
        }
    ).__repr__() == (
        "{'additionalProperties': False,\n"
        " 'definitions': {'float': {'pattern': '^-?[0-9]+\\\\.[0-9]{2}$'}},\n"
        " 'properties': {'name': {}}}"
    )


def test_set_auth_skipped(mocker):
    mocked_install = mocker.patch(
        "arche.readers.schema.urllib.request.install_opener", autospec=True
    )
    set_auth()
    mocked_install.assert_not_called()


def test_set_auth(mocker):
    mocked_install = mocker.patch(
        "arche.readers.schema.urllib.request.install_opener", autospec=True
    )
    mocker.patch.dict(
        "arche.readers.schema.os.environ",
        {"BITBUCKET_USER": "user", "BITBUCKET_PASSWORD": "pass"},
    )
    set_auth()
    mocked_install.assert_called_once()


def test_schema(get_schema):
    s = Schema(get_schema)
    assert s.allowed_tags == {
        "unique",
        "category",
        "name_field",
        "product_url_field",
        "product_price_field",
        "product_price_was_field",
    }
    assert s.raw == get_schema
    assert not s.enums
    assert not s.tags


@pytest.mark.parametrize("schema", [{"required": ["name"]}, {"properties": {}}])
def test_schema_no_properties(schema):
    with pytest.raises(ValueError) as excinfo:
        Schema(source=schema)
    assert str(excinfo.value) == "The schema does not have any 'properties'"


@pytest.mark.parametrize(
    "schema, expected_tags",
    [
        (
            {"properties": {"id": {"tag": "unique"}, "url": {"tag": "unique"}}},
            {"unique": ["id", "url"]},
        ),
        ({"properties": {"id": {"tag": "unique"}, "url": {}}}, {"unique": ["id"]}),
    ],
)
def test_schema_tags(schema, expected_tags):
    assert Schema(schema).tags == expected_tags


@pytest.mark.parametrize(
    "tags, exception",
    [
        (None, "'None' tag value is invalid, should be str or list[str]"),
        (
            ["name_field", "t"],
            (
                "{'t'} tag(s) are unsupported, valid tags are:\ncategory, "
                "name_field, product_price_field, product_price_was_field, "
                "product_url_field, unique"
            ),
        ),
    ],
)
def test_get_field_tags_fails(tags, exception):
    with pytest.raises(ValueError) as excinfo:
        Schema.get_field_tags(tags, None, defaultdict(list))
    assert str(excinfo.value) == exception


@pytest.mark.parametrize(
    "tags, field, expected_tags",
    [
        ("name_field", "name", {"name_field": ["name"]}),
        (
            ["name_field", "unique"],
            "name",
            {"name_field": ["name"], "unique": ["name"]},
        ),
        ("category", "state", {"category": ["state"]}),
    ],
)
def test_get_field_tags(tags, field, expected_tags):
    assert Schema.get_field_tags(tags, field, defaultdict(list)) == expected_tags


@pytest.mark.parametrize(
    "value, expected",
    [
        ("name_field", {"name_field"}),
        (["name_field", "unique"], {"name_field", "unique"}),
        (3, None),
    ],
)
def test_parse_tag(value, expected):
    assert Schema.parse_tag(value) == expected


def test_get_enums():
    s = Schema({"properties": {"a": {"enum": ["x"]}, "b": {"enum": ["y"]}}})
    assert s.get_enums() == ["a", "b"]
    s = Schema({"properties": {"a": {"type": "string", "enum": ["x"]}}})
    assert s.get_enums() == ["a"]
    s = Schema({"properties": {"a": {"type": "string"}}})
    assert not s.get_enums()

import arche.readers.schema as reader
from jsonschema.exceptions import SchemaError
import pytest


good_schema = {
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
def test_get_schema(mocker, source, downloaded, expected):
    mocker.patch(
        "arche.readers.schema.get_schema_from_url",
        return_value=downloaded,
        autospec=True,
    )
    assert reader.get_schema(source) == expected


@pytest.mark.parametrize(
    "source, downloaded_schema, expected_error",
    [
        ("bad_source", {"types": "string"}, SchemaError),
        ({}, None, SchemaError),
        ({"propertie": {}}, None, SchemaError),
    ],
)
def test_get_schema_fails(mocker, source, downloaded_schema, expected_error):
    mocker.patch(
        "arche.readers.schema.get_schema_from_url",
        return_value=downloaded_schema,
        autospec=True,
    )
    with pytest.raises(expected_error):
        reader.get_schema(source)


def test_get_schema_fails_on_type():
    with pytest.raises(ValueError) as excinfo:
        reader.get_schema(1)
    assert str(excinfo.value) == (
        """"1" is an unidentified schema source.\nA dict, a full s3 path or URL is expected"""
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
def test_get_schema_from_url(schema_path, schema_contents, expected, mocker):
    mocker.patch(
        "arche.readers.schema.s3.get_contents_as_string",
        return_value=schema_contents,
        autospec=True,
    )
    mocker.patch(
        "arche.readers.schema.get_contents", return_value=schema_contents, autospec=True
    )
    assert reader.get_schema_from_url(schema_path) == expected


@pytest.mark.parametrize(
    "path, expected_message",
    [
        ("s3://bucket", "'s3://bucket' is not an s3 path or URL to a schema"),
        ("bucket/file.json", "'bucket/file.json' is not an s3 path or URL to a schema"),
    ],
)
def test_get_schema_from_url_fails(path, expected_message):
    with pytest.raises(ValueError) as excinfo:
        reader.get_schema_from_url(path)
    assert str(excinfo.value) == expected_message

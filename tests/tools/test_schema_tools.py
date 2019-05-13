import arche.tools.schema as schema_tools


schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        "float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"},
        "url": {
            "pattern": (
                r"^https?://(www\.)?[a-z0-9.-]*\.[a-z]{2,}"
                r"([^<>%\x20\x00-\x1f\x7F]|%[0-9a-fA-F]{2})*$"
            )
        },
    },
    "type": "object",
    "properties": {"url": {"type": "string"}, "_key": {"type": "integer"}},
    "additionalProperties": False,
    "required": ["_key", "url"],
}


def test_set_item_no():
    assert schema_tools.set_item_no(4) == [0, 1, 2, 3]
    assert schema_tools.set_item_no(1) == [0]
    assert len(schema_tools.set_item_no(5)) == 4
    assert len(schema_tools.set_item_no(124112414)) == 4


def test_infer_schema():
    item1 = {"url": "https://example.com", "_key": 0}
    item2 = {"url": "https://example.com", "_key": 1}

    assert schema_tools.infer_schema([item1, item2]) == schema
    assert schema_tools.infer_schema([item2]) == schema


def test_basic_json_schema(mocker):
    mocked_create_js = mocker.patch(
        "arche.tools.schema.create_json_schema", return_value=schema, autospec=True
    )
    assert schema_tools.basic_json_schema("235801/1/15", [0, 5]).d == schema
    mocked_create_js.assert_called_once_with("235801/1/15", [0, 5])


def test_create_json_schema(mocker, get_job, get_raw_items):
    mocker.patch("arche.tools.api.get_job", return_value=get_job, autospec=True)
    mocker.patch("arche.tools.api.get_items", return_value=get_raw_items, autospec=True)
    schema_tools.create_json_schema(get_job.key, [2])
    assert schema_tools.create_json_schema(get_job.key, [2, 3]) == {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": {
            "float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"},
            "url": {
                "pattern": (
                    r"^https?://(www\.)?[a-z0-9.-]*\.[a-z]{2,}"
                    r"([^<>%\x20\x00-\x1f\x7F]|%[0-9a-fA-F]{2})*$"
                )
            },
        },
        "additionalProperties": False,
        "type": "object",
        "properties": {
            "_key": {"type": "string"},
            "name": {"type": "string"},
            "price": {"type": "integer"},
        },
        "required": ["_key", "name", "price"],
    }


def test_basic_schema_json(capsys):
    bs = schema_tools.BasicSchema(
        {
            "definitions": {"float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"}},
            "additionalProperties": False,
        }
    )
    bs.json()
    assert (
        capsys.readouterr().out
        == """{
    "definitions": {
        "float": {
            "pattern": "^-?[0-9]+\\\\.[0-9]{2}$"
        }
    },
    "additionalProperties": false
}\n"""
    )


def test_basic_schema_repr():
    assert schema_tools.BasicSchema(
        {
            "definitions": {"float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"}},
            "additionalProperties": False,
        }
    ).__repr__() == (
        "{'additionalProperties': False,\n "
        "'definitions': {'float': {'pattern': '^-?[0-9]+\\\\.[0-9]{2}$'}}}"
    )

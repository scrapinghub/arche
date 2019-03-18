from arche.tools.schema import infer_schema, set_item_no


def test_set_item_no():
    assert set_item_no(4) == [0, 1, 2, 3]
    assert set_item_no(1) == [0]
    assert len(set_item_no(5)) == 4
    assert len(set_item_no(124112414)) == 4


def test_infer_schema():
    item1 = {"url": "https://example.com", "_key": 0}
    item2 = {"url": "https://example.com", "_key": 1}
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
    assert infer_schema([item1, item2]) == schema
    assert infer_schema([item2]) == schema

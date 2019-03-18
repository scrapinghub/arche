from jsonschema import FormatChecker, validate, ValidationError
import pytest


@pytest.mark.parametrize(
    "instance, format_value",
    [
        ("2018-12-18", "date-time"),
        ("rubbish", "date-time"),
        ("2", "uri"),
        ("daa520", "color"),
    ],
)
def test_format(instance, format_value):
    with pytest.raises(ValidationError) as excinfo:
        validate(instance, {"format": format_value}, format_checker=FormatChecker())
    assert f"'{instance}' is not a '{format_value}'" in str(excinfo.value)

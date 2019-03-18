import arche.tools.helpers as h
import pytest


def test_ms_to_time():
    assert h.ms_to_time(None) is None
    assert h.ms_to_time(1000) == "0:00:01"


input_ratio_values = [
    (1, 1, 0),
    (2, 1, 0.5),
    (1, 2, 0.5),
    (0, 100, 1.0),
    (0, 0, 0),
    ("0", "0", 0),
    (50, 0, 1.0),
    ("", "1", 1.0),
]


@pytest.mark.parametrize("new_value, old_value, expected", input_ratio_values)
def test_ratio_diff(new_value, old_value, expected):
    assert h.ratio_diff(new_value, old_value) == expected


@pytest.mark.parametrize(
    "value, expected", [(1, 1.0), (0, 0.0), ("0", 0.0), ("", 0.0), (None, 0.0)]
)
def test_to_float_or_zero(value, expected):
    assert h.to_float_or_zero(value) == expected


@pytest.mark.xfail(raises=ValueError)
def test_to_float_or_zero_exception():
    h.to_float_or_zero("b")


@pytest.mark.parametrize(
    "job_key, expected",
    [
        ("0/0/0", True),
        (0, False),
        ("0/0", False),
        ("a/b/c.json", False),
        ("0/0/0/", False),
        ("2/collections/s/q2", False),
    ],
)
def test_is_job_key(job_key, expected):
    assert h.is_job_key(job_key) is expected


@pytest.mark.parametrize(
    "c_key, expected",
    [
        ("112358/collections/s/pages", True),
        ("0/0/0", False),
        ("s/collections/s/s", False),
        ("112358/cs/s/f", False),
        (None, False),
    ],
)
def test_is_collection_key(c_key, expected):
    assert h.is_collection_key(c_key) is expected


number_inputs = [
    (0, True),
    ("0", True),
    (None, False),
    (60.0, True),
    ([50.0], False),
    (" -10", True),
    ("\n+10", True),
    ("s", False),
]


@pytest.mark.parametrize("number, is_number", number_inputs)
def test_is_number(number, is_number):
    assert h.is_number(number) is is_number


collection_data = [
    ("112358", "pages", "112358/collections/s/pages"),
    ("", "", "/collections/s/"),
]


@pytest.mark.parametrize("project_key, source_key, expected_key", collection_data)
def test_collection_key(project_key, source_key, expected_key):
    assert str(h.CollectionKey(project_key, source_key)) == expected_key


def test_parse_collection_key():
    ck = h.parse_collection_key("112358/collections/s/pages")
    assert ck.project_key == "112358"
    assert ck.store_key == "pages"


def test_cpus_count(mocker):
    mocker.patch.object(
        h.os, "sched_getaffinity", create=True, return_value={0, 1, 2, 3}
    )
    assert h.cpus_count() == 4


def test_cpus_count_no_affinity(mocker):
    mocker.patch.object(
        h.os, "sched_getaffinity", create=True, side_effect=AttributeError()
    )
    mocker.patch.object(h.os, "cpu_count", create=True, return_value=2)
    assert h.cpus_count() == 2

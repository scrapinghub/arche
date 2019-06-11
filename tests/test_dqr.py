from arche.data_quality_report import DataQualityReport
from arche.readers.schema import Schema
from arche.report import Report


def test_dqr_empty_report(mocker, get_job_items, get_schema):
    mocker.patch(
        "arche.data_quality_report.DataQualityReport.plot_to_notebook", autospec=True
    )
    mocker.patch(
        "arche.tools.api.get_response_status_count",
        return_value=(10, 0, 0, 0),
        autospec=True,
    )
    mocker.patch("arche.tools.api.get_runtime_s", return_value=60, autospec=True)
    mocker.patch("arche.tools.api.get_items_count", return_value=1000, autospec=True)
    mocker.patch("arche.tools.api.get_requests_count", return_value=1000, autospec=True)
    dqr = DataQualityReport(
        items=get_job_items, schema=Schema(get_schema), report=Report()
    )
    assert len(dqr.figures) == 4

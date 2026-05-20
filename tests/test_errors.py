from hotdata.rest import ApiException

from hotdata_dlt_destination.errors import (
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)


def test_transient_status_codes() -> None:
    mapped = classify_sdk_error(ApiException(status=429, reason="rate limited"))
    assert isinstance(mapped, HotdataTransientError)


def test_terminal_status_codes() -> None:
    mapped = classify_sdk_error(ApiException(status=400, reason="bad request"))
    assert isinstance(mapped, HotdataTerminalError)

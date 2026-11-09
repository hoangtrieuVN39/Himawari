import pytest
import requests
from unittest.mock import Mock
from src.ingestion.http_client import fetch_with_retry, DEFAULT_USER_AGENT

def test_fetch_with_retry_success(mocker):
    mock_get = mocker.patch("requests.get")
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    resp = fetch_with_retry("https://api.example.com/test", params={"q": 1})
    assert resp.status_code == 200
    assert mock_get.call_count == 1

    # Verify User-Agent was injected
    called_headers = mock_get.call_args[1]["headers"]
    assert called_headers["User-Agent"] == DEFAULT_USER_AGENT

def test_fetch_with_retry_backoff_and_recover(mocker):
    mock_get = mocker.patch("requests.get")
    mocker.patch("time.sleep") # Do not actually sleep in tests

    fail_resp = Mock()
    fail_resp.status_code = 503
    fail_resp.raise_for_status.side_effect = requests.HTTPError("Service Unavailable", response=fail_resp)

    ok_resp = Mock()
    ok_resp.status_code = 200
    ok_resp.raise_for_status.return_value = None

    # Fail on first call, succeed on second
    mock_get.side_effect = [fail_resp, ok_resp]

    resp = fetch_with_retry("https://api.example.com/test", max_retries=3, backoff_factor=0.01)
    assert resp.status_code == 200
    assert mock_get.call_count == 2

def test_fetch_with_retry_exhausted(mocker):
    mock_get = mocker.patch("requests.get")
    mocker.patch("time.sleep")

    mock_get.side_effect = requests.ConnectionError("Connection refused")

    with pytest.raises(requests.RequestException):
        fetch_with_retry("https://api.example.com/fail", max_retries=3, backoff_factor=0.01)

    assert mock_get.call_count == 3


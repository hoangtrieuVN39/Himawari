import time
from typing import Optional, Dict, Any
import requests

DEFAULT_USER_AGENT = "VN-Airport-Weather-Dispatcher-Academic-Project/1.0 (MIS5007)"

def fetch_with_retry(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 15.0,
    max_retries: int = 3,
    backoff_factor: float = 2.0
) -> requests.Response:
    """
    Executes an HTTP GET request with default project User-Agent, timeout,
    and exponential backoff retry mechanism (e.g. 2s, 4s, 8s).
    Raises requests.RequestException if all attempts fail.
    """
    req_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        req_headers.update(headers)

    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                headers=req_headers,
                timeout=timeout_seconds
            )
            response.raise_for_status()
            return response
        except (requests.RequestException, Exception) as err:
            last_error = err
            if attempt == max_retries:
                break
            sleep_duration = backoff_factor * (2 ** (attempt - 1))
            time.sleep(sleep_duration)

    if last_error is not None:
        if isinstance(last_error, requests.RequestException):
            raise last_error
        raise requests.RequestException(f"Request failed: {last_error}") from last_error

    raise requests.RequestException("Request failed with unknown error")


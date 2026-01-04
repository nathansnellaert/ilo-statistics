"""ILO SDMX API client with rate limiting."""

from ratelimit import limits, sleep_and_retry
from subsets_utils import get

BASE_URL = "https://sdmx.ilo.org/rest"

# ILO doesn't specify strict rate limits, but be conservative
@sleep_and_retry
@limits(calls=10, period=1)
def rate_limited_get(endpoint, params=None, headers=None):
    """Make a rate-limited GET request to ILO SDMX API."""
    url = f"{BASE_URL}/{endpoint}"
    default_headers = {
        'Accept': 'application/vnd.sdmx.data+csv;version=1.0.0',
    }
    if headers:
        default_headers.update(headers)
    response = get(url, params=params, headers=default_headers, timeout=120.0)
    return response


def get_dataflows():
    """
    Get list of all available dataflows (datasets).

    Returns:
        XML/JSON response with dataflow metadata
    """
    response = rate_limited_get(
        'dataflow/ILO',
        headers={'Accept': 'application/vnd.sdmx.structure+json;version=1.0'}
    )
    response.raise_for_status()
    return response.json()


def get_data_csv(dataflow_id, key='ALL', start_period=None, end_period=None, version='1.0'):
    """
    Get data in CSV format for a specific dataflow.

    Args:
        dataflow_id: The dataflow identifier (e.g., 'DF_EMP_TEMP_SEX_AGE_NB')
        key: Dimension filter (default 'ALL' for all data)
        start_period: Start period filter (e.g., '2020')
        end_period: End period filter (e.g., '2024')
        version: Dataflow version (default '1.0')

    Returns:
        CSV text response
    """
    endpoint = f"data/ILO,{dataflow_id},{version}/{key}"

    params = {}
    if start_period:
        params['startPeriod'] = start_period
    if end_period:
        params['endPeriod'] = end_period

    response = rate_limited_get(
        endpoint,
        params=params,
        headers={'Accept': 'text/csv'}
    )
    response.raise_for_status()
    return response.text


def get_datastructure(dataflow_id):
    """
    Get the data structure definition for a dataflow.

    Args:
        dataflow_id: The dataflow identifier

    Returns:
        JSON response with structure metadata
    """
    response = rate_limited_get(
        f'datastructure/ILO/{dataflow_id}',
        headers={'Accept': 'application/vnd.sdmx.structure+json;version=1.0'}
    )
    response.raise_for_status()
    return response.json()

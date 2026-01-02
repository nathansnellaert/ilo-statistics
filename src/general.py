"""General SDMX utilities for ILO connector."""

import csv
import io
import pyarrow as pa
from typing import Dict, List, Any, Optional

from subsets_utils import load_raw_file, load_raw_json

# Cached metadata
_DATAFLOWS = None


def load_dataflows() -> List[Dict]:
    """Load dataflow catalogue from raw data."""
    global _DATAFLOWS
    if _DATAFLOWS is None:
        try:
            raw = load_raw_json("dataflows")
            _DATAFLOWS = raw.get("data", {}).get("dataflows", [])
        except FileNotFoundError:
            _DATAFLOWS = []
    return _DATAFLOWS


def get_dataflow_info(dataflow_id: str) -> Optional[Dict]:
    """Get metadata for a specific dataflow."""
    dataflows = load_dataflows()
    for df in dataflows:
        if df.get("id") == dataflow_id:
            return df
    return None


def get_dataflow_name(dataflow_id: str, lang: str = "en") -> str:
    """Get human-readable name for a dataflow."""
    info = get_dataflow_info(dataflow_id)
    if info:
        names = info.get("names", [])
        for name in names:
            if name.get("lang") == lang:
                return name.get("value", dataflow_id)
        if names:
            return names[0].get("value", dataflow_id)
    return dataflow_id


def parse_sdmx_csv(csv_text: str) -> List[Dict[str, Any]]:
    """Parse SDMX CSV response into list of records."""
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize SDMX record: lowercase keys, convert obs_value to float."""
    normalized = {}
    for key, value in record.items():
        lower_key = key.lower()
        if lower_key == 'obs_value':
            normalized[lower_key] = float(value) if value else None
        else:
            normalized[lower_key] = value
    return normalized


def load_dataflow_data(dataflow_id: str) -> pa.Table:
    """
    Load raw data for a dataflow from disk.

    Expects data to already exist from ingest phase.
    Raises FileNotFoundError if not ingested yet.

    Args:
        dataflow_id: ILO dataflow identifier (e.g., 'DF_UNE_DEAP_SEX_AGE_RT')

    Returns:
        PyArrow table with normalized data
    """
    cache_key = f"data_{dataflow_id}"
    csv_text = load_raw_file(cache_key, extension="csv")

    records = parse_sdmx_csv(csv_text)
    if not records:
        return pa.table({})

    normalized = [normalize_record(r) for r in records]
    return pa.Table.from_pylist(normalized)


def dataflow_to_dataset_id(dataflow_id: str) -> str:
    """
    Convert dataflow ID to dataset ID.

    E.g., 'DF_UNE_DEAP_SEX_AGE_RT' -> 'ilo_une_deap_sex_age_rt'
    """
    if dataflow_id.startswith('DF_'):
        base = dataflow_id[3:]
    else:
        base = dataflow_id
    return f"ilo_{base.lower()}"

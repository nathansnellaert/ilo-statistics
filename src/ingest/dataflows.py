"""Ingest ILO dataflow catalogue."""

from ilo_client import get_dataflows
from subsets_utils import save_raw_json


def run():
    """Fetch ILO dataflow catalogue and save raw JSON."""
    print("  Fetching dataflow catalogue...")
    response = get_dataflows()
    save_raw_json(response, "dataflows")
    print("  Saved raw dataflows JSON")

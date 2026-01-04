"""Ingest all ILO dataflow data."""

from general import load_dataflows
from ilo_client import get_data_csv
from subsets_utils import save_raw_file


def run():
    """Fetch data for all available dataflows from the catalogue."""
    dataflows = load_dataflows()
    print(f"  Fetching {len(dataflows)} dataflows...")

    for i, df in enumerate(dataflows, 1):
        dataflow_id = df['id']
        name = df.get('names', {}).get('en', dataflow_id)
        print(f"    [{i}/{len(dataflows)}] {dataflow_id}: {name[:50]}")

        try:
            csv_text = get_data_csv(dataflow_id)
            save_raw_file(csv_text, f"data_{dataflow_id}", extension="csv")
            print(f"      {len(csv_text):,} bytes")
        except Exception as e:
            print(f"      Error: {e}")

    print(f"  Done fetching dataflows")

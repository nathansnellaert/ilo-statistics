"""ILO transforms orchestrator.

Generic transform system that processes all raw data downloaded during ingest.
Discovers dataflows from raw/ directory - no config needed.
"""

import os
from pathlib import Path

from general import load_dataflows, load_dataflow_data, dataflow_to_dataset_id
from subsets_utils import upload_data, publish, get_data_dir


def get_ingested_dataflows() -> list:
    """Discover all dataflows that have been ingested (have raw CSV files)."""
    raw_dir = Path(get_data_dir()) / "raw"
    dataflows = []

    for f in raw_dir.glob("data_DF_*.csv"):
        # Extract dataflow ID from filename: data_DF_XXX.csv -> DF_XXX
        dataflow_id = f.stem.replace("data_", "")
        dataflows.append(dataflow_id)

    return sorted(dataflows)


def get_dataflow_metadata(dataflow_id: str) -> dict:
    """Get metadata for a dataflow from the catalogue."""
    dataflows = load_dataflows()
    for df in dataflows:
        if df.get('id') == dataflow_id:
            names = df.get('names', {})
            name = names.get('en', dataflow_id) if isinstance(names, dict) else dataflow_id
            desc = df.get('description', '')
            if isinstance(desc, dict):
                desc = desc.get('en', '')
            return {
                'title': f"ILO: {name}",
                'description': desc or f"Data from ILO dataflow {dataflow_id}",
            }
    return {
        'title': f"ILO: {dataflow_id}",
        'description': f"Data from ILO dataflow {dataflow_id}",
    }


def transform_dataflow(dataflow_id: str) -> dict:
    """
    Transform raw data for a dataflow into a published dataset.

    Args:
        dataflow_id: ILO dataflow identifier

    Returns:
        Status dict with dataset info
    """
    dataset_id = dataflow_to_dataset_id(dataflow_id)
    metadata = get_dataflow_metadata(dataflow_id)

    # Load raw data (from ingest phase)
    table = load_dataflow_data(dataflow_id)

    if table.num_rows == 0:
        raise ValueError(f"No data found for {dataflow_id}")

    # Build full metadata
    full_metadata = {
        "id": dataset_id,
        "title": metadata['title'],
        "description": metadata['description'],
        "source_dataflow": dataflow_id,
    }

    # Upload and publish
    upload_data(table, dataset_id, mode="overwrite")
    publish(dataset_id, full_metadata)

    return {
        "dataflow_id": dataflow_id,
        "dataset_id": dataset_id,
        "rows": table.num_rows,
        "status": "success"
    }


def run_one(dataflow_id: str) -> dict:
    """Run transform for a single dataflow."""
    try:
        return transform_dataflow(dataflow_id)
    except Exception as e:
        return {
            "dataflow_id": dataflow_id,
            "status": "error",
            "error": str(e)
        }


def run_all() -> list:
    """Run transforms for all ingested dataflows."""
    results = []
    dataflow_ids = get_ingested_dataflows()

    print(f"Found {len(dataflow_ids)} ingested dataflows")

    for i, dataflow_id in enumerate(dataflow_ids, 1):
        dataset_id = dataflow_to_dataset_id(dataflow_id)
        print(f"[{i}/{len(dataflow_ids)}] {dataflow_id} -> {dataset_id}")

        result = run_one(dataflow_id)
        results.append(result)

        if result["status"] == "success":
            print(f"    {result['rows']:,} rows")
        else:
            print(f"    Error: {result['error']}")

    # Summary
    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    print(f"\nTransforms complete: {success} succeeded, {failed} failed")

    return results

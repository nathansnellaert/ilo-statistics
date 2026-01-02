#!/usr/bin/env python3
"""Main orchestrator for ILO Statistics integration."""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import dataflows as ingest_dataflows
from ingest import data as ingest_data
import transforms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    parser.add_argument("--dataflow", type=str, help="Run single dataflow transform")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("Fetching dataflow catalogue...")
        ingest_dataflows.run()
        print("Fetching dataflow data...")
        ingest_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        if args.dataflow:
            result = transforms.run_one(args.dataflow)
            if result["status"] != "success":
                raise RuntimeError(f"Transform failed: {result.get('error')}")
        else:
            transforms.run_all()

    print("\n=== ILO connector complete ===")


if __name__ == "__main__":
    main()

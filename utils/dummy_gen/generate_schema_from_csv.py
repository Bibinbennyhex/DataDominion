#!/usr/bin/env python3
"""
Generate schemas.json (table -> [{column_name, type}]) from CSV file(s).
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date
from pathlib import Path
from typing import Dict, List


def is_int(value: str) -> bool:
    try:
        int(value)
        return True
    except ValueError:
        return False


def is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def infer_column_type(column_name: str, values: List[str]) -> str:
    lowered = column_name.lower()
    non_empty = [v.strip() for v in values if v is not None and v.strip() != ""]

    if lowered == "key":
        return "bigint"
    if not non_empty:
        return "string"
    if all(is_int(v) for v in non_empty):
        nums = [int(v) for v in non_empty]
        if all(0 <= n < 1000 for n in nums):
            return "numeric_lt_1000"
        return "bigint"
    if all(is_iso_date(v) for v in non_empty):
        return "date"
    if all(v.isalnum() for v in non_empty):
        return "alphanumeric"
    return "string"


def read_csv_columns(csv_path: Path) -> Dict[str, List[str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"No header columns found in {csv_path}")
        values_by_col = {name: [] for name in reader.fieldnames}
        for row in reader:
            for name in reader.fieldnames:
                values_by_col[name].append(row.get(name, ""))
    return values_by_col


def build_schema_for_csv(csv_path: Path) -> List[Dict[str, str]]:
    values_by_col = read_csv_columns(csv_path)
    schema = []
    for col in values_by_col.keys():
        schema.append(
            {
                "column_name": col,
                "type": infer_column_type(col, values_by_col[col]),
            }
        )
    return schema


def write_source_schema_csv(
    payload: Dict[str, List[Dict[str, str]]], source_schema_csv_dir: Path
) -> None:
    source_schema_csv_dir.mkdir(parents=True, exist_ok=True)
    for table_name, columns in payload.items():
        out_path = source_schema_csv_dir / f"{table_name}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["column_name", "type"])
            writer.writeheader()
            for col in columns:
                writer.writerow({"column_name": col["column_name"], "type": col["type"]})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate schema folder from one CSV or all CSVs in a directory. "
            "Outputs schema/schemas.json and schema/source_schema_csv/*.csv"
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="CSV file path or directory containing CSV files.",
    )
    parser.add_argument(
        "--schema-dir",
        default="schema",
        help="Schema directory path where schemas.json and source_schema_csv/ are written.",
    )
    parser.add_argument(
        "--table-name",
        default=None,
        help="Table name to use when --input is a single CSV. Default: CSV file name.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    schema_dir = Path(args.schema_dir)
    output_path = schema_dir / "schemas.json"
    source_schema_csv_dir = schema_dir / "source_schema_csv"

    if input_path.is_file():
        if input_path.suffix.lower() != ".csv":
            raise ValueError(f"Input file is not CSV: {input_path}")
        table_name = args.table_name or input_path.stem
        payload = {table_name: build_schema_for_csv(input_path)}
    elif input_path.is_dir():
        csv_files = sorted(input_path.glob("*.csv"))
        if not csv_files:
            raise ValueError(f"No CSV files found in directory: {input_path}")
        payload = {csv_file.stem: build_schema_for_csv(csv_file) for csv_file in csv_files}
    else:
        raise ValueError(f"Input path does not exist: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_source_schema_csv(payload, source_schema_csv_dir)
    print(f"Wrote schema JSON to: {output_path.resolve()}")
    print(f"Wrote source schema CSV files to: {source_schema_csv_dir.resolve()}")


if __name__ == "__main__":
    main()

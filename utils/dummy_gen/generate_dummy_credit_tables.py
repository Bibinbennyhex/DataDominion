#!/usr/bin/env python3
"""
Generate dummy credit-scoring table schemas and sample data.

Outputs:
1) schema JSON (table -> [{column_name, type}]) used by generator
2) <table>.csv files with generated dummy rows
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import string
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class ColumnDef:
    column_name: str
    value_type: str

    def to_dict(self) -> Dict[str, str]:
        return {"column_name": self.column_name, "type": self.value_type}


def _date_range(start_date: date, end_date: date) -> List[date]:
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    days = (end_date - start_date).days
    return [start_date + timedelta(days=i) for i in range(days + 1)]


def _random_attr_name(prefix: str) -> str:
    letters = "".join(random.choices(string.ascii_lowercase, k=random.randint(1, 3)))
    digits = "".join(random.choices(string.digits, k=random.randint(2, 5)))
    return f"{prefix}{letters}{digits}"


def build_premier_schema(attr_count: int = 12) -> List[ColumnDef]:
    cols = [ColumnDef("key", "bigint"), ColumnDef("snapshot_dt", "date")]
    used = set()
    while len(used) < attr_count:
        used.add(_random_attr_name("pr_attr_"))
    cols.extend(ColumnDef(name, "numeric_lt_1000") for name in sorted(used))
    return cols


def build_custom_schema(attr_count: int = 12) -> List[ColumnDef]:
    cols = [ColumnDef("key", "bigint"), ColumnDef("snapshot_dt", "date")]
    names = [f"INC_{i:02d}" for i in range(1, (attr_count // 2) + 1)]
    names += [f"AS{i:02d}" for i in range(1, attr_count - len(names) + 1)]
    cols.extend(ColumnDef(name, "numeric_lt_1000") for name in names)
    return cols


def build_custom_created_schema(attr_count: int = 12) -> List[ColumnDef]:
    cols = [ColumnDef("key", "bigint"), ColumnDef("snapshot_dt", "date")]
    names = [f"CINSA_{i:02d}" for i in range(1, attr_count + 1)]
    cols.extend(ColumnDef(name, "numeric_lt_1000") for name in names)
    return cols


def build_finbase_schema() -> List[ColumnDef]:
    return [
        ColumnDef("key", "bigint"),
        ColumnDef("snapshot_dt", "date"),
        ColumnDef("infiv_1", "alphanumeric"),
    ]


def build_fintech_attr_schema() -> List[ColumnDef]:
    return [
        ColumnDef("key", "bigint"),
        ColumnDef("snapshot_dt", "date"),
        ColumnDef("infiv", "numeric_lt_1000"),
    ]


def build_bureau_score_schema() -> List[ColumnDef]:
    cols = [
        ColumnDef("key", "bigint"),
        ColumnDef("snapshot_dt", "date"),
        ColumnDef("score_date", "numeric_lt_1000"),
        ColumnDef("score", "numeric_lt_1000"),
    ]
    for months in range(3, 22, 3):
        cols.append(ColumnDef(f"score_dt_{months}M", "numeric_lt_1000"))
    return cols


def build_all_schemas(
    premier_attr_count: int = 12,
    custom_attr_count: int = 12,
    custom_created_attr_count: int = 12,
) -> Dict[str, List[ColumnDef]]:
    return {
        "premier": build_premier_schema(attr_count=premier_attr_count),
        "custom": build_custom_schema(attr_count=custom_attr_count),
        "custom_created": build_custom_created_schema(
            attr_count=custom_created_attr_count
        ),
        "finbase": build_finbase_schema(),
        "fintech_attr": build_fintech_attr_schema(),
        "bureau_score": build_bureau_score_schema(),
    }


def _to_iso(d: date) -> str:
    return d.isoformat()


def _random_bigint_key(index: int) -> int:
    # Keep keys within signed 64-bit range.
    return 100000000000 + index


def _random_numeric_under_1000() -> int:
    return random.randint(0, 999)


def _random_alphanumeric(length: int = 10) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choices(alphabet, k=length))


def _subtract_months(d: date, months: int) -> date:
    # Approximation for dummy data: 30-day month shift.
    return d - timedelta(days=30 * months)


def _generate_row(
    table_name: str,
    columns: Iterable[ColumnDef],
    key_index: int,
    snapshot_date: date,
) -> Dict[str, str | int]:
    row: Dict[str, str | int] = {}
    for col in columns:
        name = col.column_name
        if name == "key" or col.value_type == "bigint":
            row[name] = _random_bigint_key(key_index)
        elif name == "snapshot_dt":
            row[name] = _to_iso(snapshot_date)
        elif name == "score_date" and col.value_type == "date":
            row[name] = _to_iso(snapshot_date)
        elif (
            name.startswith("score_dt_")
            and name.endswith("M")
            and col.value_type == "date"
        ):
            months = int(name[len("score_dt_") : -1])
            row[name] = _to_iso(_subtract_months(snapshot_date, months))
        elif col.value_type == "numeric_lt_1000":
            row[name] = _random_numeric_under_1000()
        elif col.value_type == "alphanumeric":
            row[name] = _random_alphanumeric()
        elif col.value_type == "date":
            row[name] = _to_iso(snapshot_date)
        else:
            raise ValueError(f"Unsupported type: {col.value_type} ({table_name}.{name})")
    return row


def write_schema_json(schemas: Dict[str, List[ColumnDef]], output_path: Path) -> None:
    payload = {
        table: [col.to_dict() for col in columns] for table, columns in schemas.items()
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_schema_json(schema_path: Path) -> Dict[str, List[ColumnDef]]:
    raw = json.loads(schema_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Schema JSON must be an object: {schema_path}")

    schemas: Dict[str, List[ColumnDef]] = {}
    for table_name, columns in raw.items():
        if not isinstance(table_name, str):
            raise ValueError("Table names in schema JSON must be strings")
        if not isinstance(columns, list):
            raise ValueError(f"{table_name} schema must be a list")

        parsed_columns: List[ColumnDef] = []
        for idx, col in enumerate(columns):
            if not isinstance(col, dict):
                raise ValueError(
                    f"{table_name}[{idx}] must be an object with column_name/type"
                )
            column_name = col.get("column_name")
            value_type = col.get("type")
            if not isinstance(column_name, str) or not isinstance(value_type, str):
                raise ValueError(
                    f"{table_name}[{idx}] must include string column_name and type"
                )
            parsed_columns.append(ColumnDef(column_name=column_name, value_type=value_type))
        schemas[table_name] = parsed_columns

    return schemas


def write_table_csv(
    table_name: str,
    columns: List[ColumnDef],
    output_dir: Path,
    rows_per_table: int,
    start_date: date,
    end_date: date,
) -> None:
    date_values = _date_range(start_date, end_date)
    fieldnames = [c.column_name for c in columns]
    csv_path = output_dir / f"{table_name}.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, rows_per_table + 1):
            snapshot_date = random.choice(date_values)
            row = _generate_row(
                table_name=table_name,
                columns=columns,
                key_index=i,
                snapshot_date=snapshot_date,
            )
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate dummy credit-scoring table schemas and CSV data."
    )
    parser.add_argument(
        "--output-dir",
        default="generated_data",
        help="Directory where table CSV data files will be written.",
    )
    parser.add_argument(
        "--schema-json",
        default="schema/schemas.json",
        help=(
            "Schema JSON path used by the generator. "
            "If it exists, generator reads from it; otherwise it creates it."
        ),
    )
    parser.add_argument(
        "--rows-per-table",
        type=int,
        default=100,
        help="Rows to generate in each table CSV.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2023-01-01",
        help="Snapshot date range start (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2024-12-31",
        help="Snapshot date range end (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--premier-attrs",
        type=int,
        default=12,
        help="Number of random numeric attributes for premier table.",
    )
    parser.add_argument(
        "--custom-attrs",
        type=int,
        default=12,
        help="Number of numeric attributes for custom table.",
    )
    parser.add_argument(
        "--custom-created-attrs",
        type=int,
        default=12,
        help="Number of numeric attributes for custom_created table.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    schema_json_path = Path(args.schema_json)

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)

    if schema_json_path.exists():
        schemas = load_schema_json(schema_json_path)
    else:
        schemas = build_all_schemas(
            premier_attr_count=args.premier_attrs,
            custom_attr_count=args.custom_attrs,
            custom_created_attr_count=args.custom_created_attrs,
        )
        write_schema_json(schemas, schema_json_path)

    for table_name, columns in schemas.items():
        write_table_csv(
            table_name=table_name,
            columns=columns,
            output_dir=output_dir,
            rows_per_table=args.rows_per_table,
            start_date=start,
            end_date=end,
        )

    print(f"Used schema JSON: {schema_json_path.resolve()}")
    print(f"Generated table CSV data in: {output_dir.resolve()}")


if __name__ == "__main__":
    main()

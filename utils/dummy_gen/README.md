# Dummy Credit Table Data Generator

Scripts to:
- Generate table data CSVs for credit-scoring style tables
- Generate schema JSON from existing CSV files
- Keep schema artifacts in a dedicated `schema/` folder

## Prerequisites
- Python 3.9+ installed
- Run commands from project root:
  - `d:\Work\Experian\Dummy_Generation`

## Files
- `generate_dummy_credit_tables.py`
- `generate_schema_from_csv.py`

## Folder Structure
- `schema/schemas.json` -> schema used by data generator
- `schema/source_schema_csv/*.csv` -> per-table schema CSV (`column_name,type`)
- `sample_output/*.csv` -> generated dummy table data

## Quick Start

### 1) Generate schema artifacts from CSV data
If your source CSVs are in `sample_output`:

```powershell
python generate_schema_from_csv.py --input sample_output --schema-dir schema
```

This creates:
- `schema/schemas.json`
- `schema/source_schema_csv/`

### 2) Generate dummy table data using schema JSON

```powershell
python generate_dummy_credit_tables.py --output-dir sample_output --schema-json schema/schemas.json
```

This creates/overwrites table CSVs in `sample_output/` for:
- `premier`
- `custom`
- `custom_created`
- `finbase`
- `fintech_attr`
- `bureau_score`

## Useful Options

### `generate_dummy_credit_tables.py`
```powershell
python generate_dummy_credit_tables.py `
  --output-dir sample_output `
  --schema-json schema/schemas.json `
  --rows-per-table 100 `
  --start-date 2023-01-01 `
  --end-date 2024-12-31 `
  --seed 42
```

If `--schema-json` file does not exist, the script auto-creates it with default table schemas.

### `generate_schema_from_csv.py`
From one CSV:
```powershell
python generate_schema_from_csv.py --input sample_output\bureau_score.csv --table-name bureau_score --schema-dir schema
```

From a folder of CSVs:
```powershell
python generate_schema_from_csv.py --input sample_output --schema-dir schema
```

## Notes
- `key` is generated/inferred as `bigint`.
- Numeric score/attribute fields are generated as values `< 1000` where schema type is `numeric_lt_1000`.
- `finbase.infiv_1` is alphanumeric.

# Incremental Summary Update Module

This folder contains the incremental update logic for the summary table.

## Scripts

| Script | Description |
|--------|-------------|
| `incremental_summary_update.py` | Main pipeline script |

## Usage

```bash
# Run from spark-iceberg container
docker exec spark-iceberg spark-submit /home/iceberg/notebooks/incremental/incremental_summary_update.py
```

## Scenarios Handled

1. **Case I**: New `cons_acct_key` - creates row with NULL history padding
2. **Case II**: Forward month entry - shifts arrays, handles month gaps with NULLs
3. **Case III**: Backfill - updates existing + fixes all future rows' history arrays

## Configuration

Edit `incremental_summary_update.py` to change:
- `ACCOUNTS_TABLE`: Source table (default: `default.default.accounts_all`)
- `SUMMARY_TABLE`: Target table (default: `default.summary_testing`)
- `HISTORY_LENGTH`: Array size (default: 36)

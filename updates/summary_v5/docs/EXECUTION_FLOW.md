# Summary Pipeline v4.0 - Execution Flow

**Step-by-step trace of code execution as it runs**

---

## How to Read This Document

This document follows the **actual execution path** of the code, as if you're watching it run with a debugger. 

Each section shows:
- 📍 **Where we are** in the code (file:line)
- ⚙️ **What's happening** right now
- 🔄 **What function is called** next
- 📊 **Example data** at this point

---

## Table of Contents

1. [Forward Mode Execution](#forward-mode-execution)
2. [Backfill Mode Execution](#backfill-mode-execution)
3. [Example Execution Traces](#example-execution-traces)

---

## Forward Mode Execution

### User Command

```bash
docker exec spark-iceberg spark-submit \
  /home/iceberg/notebooks/notebooks/summary_v4/summary_pipeline.py \
  --config /home/iceberg/notebooks/notebooks/summary_v4/summary_config.json \
  --mode forward \
  --start-month 2024-01 \
  --end-month 2024-03
```

---

### Step 1: Entry Point

📍 **Location**: `summary_pipeline.py:1266`

```python
if __name__ == "__main__":
    main()
```

⚙️ **What happens**: Python interpreter starts execution, calls `main()`

🔄 **Next**: Jump to `main()` function

---

### Step 2: main() Starts

📍 **Location**: `summary_pipeline.py:1227-1230`

```python
def main():
    args = parse_args()
```

⚙️ **What happens**: The main function starts, needs to parse command-line arguments

🔄 **Next**: Call `parse_args()`

---

### Step 3: Parse Arguments

📍 **Location**: `summary_pipeline.py:1206-1224`

```python
def parse_args():
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v4.0 - Config-driven production scale'
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='Path to config JSON file')
    parser.add_argument('--mode', type=str, choices=['initial', 'forward', 'backfill'],
                       default='forward', help='Processing mode')
    parser.add_argument('--start-month', type=str,
                       help='Start month (YYYY-MM) for forward mode')
    parser.add_argument('--end-month', type=str,
                       help='End month (YYYY-MM) for forward mode')
    parser.add_argument('--backfill-filter', type=str,
                       help='SQL filter for backfill source data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without writing')
    
    return parser.parse_args()
```

⚙️ **What happens**: 
- Creates argument parser
- Defines all command-line arguments
- Parses the actual command-line input

📊 **Result**:
```python
args = Namespace(
    config='/home/iceberg/notebooks/notebooks/summary_v4/summary_config.json',
    mode='forward',
    start_month='2024-01',
    end_month='2024-03',
    backfill_filter=None,
    dry_run=False
)
```

🔄 **Return to**: `main()` at line 1227

---

### Step 4: Load Configuration

📍 **Location**: `summary_pipeline.py:1230-1231`

```python
# Load config
config = SummaryConfig(args.config)
```

⚙️ **What happens**: Creates a `SummaryConfig` object by loading JSON file

🔄 **Next**: Jump into `SummaryConfig.__init__()`

---

### Step 5: SummaryConfig Constructor

📍 **Location**: `summary_pipeline.py:70-122`

```python
def __init__(self, config_path: str):
    # Step 5.1: Load JSON file
    with open(config_path, 'r') as f:
        self._config = json.load(f)
```

⚙️ **What happens**: Opens `summary_config.json` and loads it into memory

📊 **Data at this point**:
```python
self._config = {
    "environment": "local",
    "source_table": "default.default.accounts_all",
    "destination_table": "default.summary",
    "latest_history_table": "default.latest_summary",
    "partition_column": "rpt_as_of_mo",
    "primary_column": "cons_acct_key",
    "history_length": 36,
    "rolling_columns": [...],
    "grid_columns": [...],
    # ... etc
}
```

---

📍 **Location**: `summary_pipeline.py:74-85`

```python
    # Step 5.2: Extract core settings
    self.source_table = self._config['source_table']
    self.destination_table = self._config['destination_table']
    self.latest_history_table = self._config['latest_history_table']
    
    self.partition_column = self._config['partition_column']
    self.primary_column = self._config['primary_column']
    self.primary_date_column = self._config['primary_date_column']
    self.max_identifier_column = self._config['max_identifier_column']
    self.history_length = self._config.get('history_length', 36)
```

⚙️ **What happens**: Extracts table names and key columns from JSON

📊 **Variables now set**:
```python
self.source_table = "default.default.accounts_all"
self.destination_table = "default.summary"
self.latest_history_table = "default.latest_summary"
self.partition_column = "rpt_as_of_mo"
self.primary_column = "cons_acct_key"
self.history_length = 36
```

---

📍 **Location**: `summary_pipeline.py:87-96`

```python
    # Step 5.3: Extract column mappings
    self.columns = self._config['columns']
    self.column_transformations = self._config.get('column_transformations', [])
    self.inferred_columns = self._config.get('inferred_columns', [])
    self.coalesce_exclusion_cols = self._config.get('coalesce_exclusion_cols', [])
    self.date_col_list = self._config.get('date_col_list', [])
    self.latest_history_addon_cols = self._config.get('latest_history_addon_cols', [])
    
    # Step 5.4: Extract rolling columns
    self.rolling_columns = self._config.get('rolling_columns', [])
    self.grid_columns = self._config.get('grid_columns', [])
```

⚙️ **What happens**: Loads all column definitions and transformations

📊 **Example**:
```python
self.rolling_columns = [
    {
        "name": "balance_am",
        "mapper_expr": "CASE WHEN balance_am < 0 THEN NULL ELSE balance_am END",
        "mapper_column": "balance_am",
        "num_cols": 36,
        "type": "Integer"
    },
    # ... 6 more rolling columns
]
```

---

📍 **Location**: `summary_pipeline.py:99-122`

```python
    # Step 5.5: Extract performance settings
    perf = self._config.get('performance', {})
    self.target_records_per_partition = perf.get('target_records_per_partition', 10_000_000)
    self.min_partitions = perf.get('min_partitions', 16)
    self.max_partitions = perf.get('max_partitions', 8192)
    # ... etc
    
    # Step 5.6: Derive helper lists
    self.rolling_mapper_list = [col['mapper_column'] for col in self.rolling_columns]
    self.rolling_history_cols = [f"{col['name']}_history" for col in self.rolling_columns]
```

⚙️ **What happens**: 
- Loads performance tuning parameters
- Creates derived lists for faster lookups

📊 **Variables now set**:
```python
self.rolling_mapper_list = ["balance_am", "actual_payment_am", "credit_limit_am", ...]
self.rolling_history_cols = ["balance_am_history", "actual_payment_am_history", ...]
```

🔄 **Return to**: `main()` at line 1231

---

### Step 6: Create Spark Session

📍 **Location**: `summary_pipeline.py:1233-1235`

```python
# Create Spark session
spark = create_spark_session(config)
spark.sparkContext.setLogLevel("INFO")
```

⚙️ **What happens**: Need to create a Spark session with Iceberg support

🔄 **Next**: Call `create_spark_session(config)`

---

### Step 7: Build Spark Session

📍 **Location**: `summary_pipeline.py:140-145`

```python
def create_spark_session(config: SummaryConfig, app_name: str = "SummaryPipeline_v4") -> SparkSession:
    """Create production-optimized Spark session."""
    
    # Step 7.1: Start building session
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
```

⚙️ **What happens**: Start building Spark session with Iceberg extensions

📊 **What this enables**:
- MERGE operations
- Time travel
- Schema evolution

---

📍 **Location**: `summary_pipeline.py:148-153`

```python
    # Step 7.2: Configure Adaptive Query Execution (AQE)
    if config.adaptive_enabled:
        builder = builder \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", str(config.skew_join_enabled).lower())
```

⚙️ **What happens**: Enable Spark 3.x optimizations for better performance

📊 **Performance impact**: 2-5x speedup on production workloads

---

📍 **Location**: `summary_pipeline.py:156-167`

```python
    # Step 7.3: Configure other optimizations
    if config.dynamic_partition_pruning:
        builder = builder \
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    
    # Step 7.4: Network and timeout settings
    builder = builder \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.autoBroadcastJoinThreshold", f"{config.broadcast_threshold_mb}m") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    
    # Step 7.5: Create session
    return builder.getOrCreate()
```

⚙️ **What happens**: 
- Configure partition pruning
- Set timeouts to prevent failures
- Optimize joins
- Actually create the Spark session

📊 **Result**: Active Spark session ready to process data

🔄 **Return to**: `main()` at line 1235

---

### Step 8: Create Pipeline Object

📍 **Location**: `summary_pipeline.py:1237-1238`

```python
try:
    pipeline = SummaryPipeline(spark, config)
```

⚙️ **What happens**: Create the main pipeline object

🔄 **Next**: Jump to `SummaryPipeline.__init__()`

---

### Step 9: SummaryPipeline Constructor

📍 **Location**: `summary_pipeline.py:367-371`

```python
def __init__(self, spark: SparkSession, config: SummaryConfig):
    self.spark = spark
    self.config = config
    self.start_time = time.time()
    self.optimization_counter = 0
```

⚙️ **What happens**: 
- Store references to Spark session and config
- Record start time for performance tracking
- Initialize optimization counter (triggers table compaction every 12 months)

📊 **Object state**:
```python
pipeline.spark = <SparkSession>
pipeline.config = <SummaryConfig>
pipeline.start_time = 1737532800.123456  # Current timestamp
pipeline.optimization_counter = 0
```

🔄 **Return to**: `main()` at line 1238

---

### Step 10: Determine Processing Mode

📍 **Location**: `summary_pipeline.py:1240-1253`

```python
if args.mode == 'forward' or args.mode == 'initial':
    # Step 10.1: Parse start/end months
    if args.start_month:
        start_month = datetime.strptime(args.start_month, '%Y-%m').date().replace(day=1)
    else:
        start_month = date.today().replace(day=1)
    
    if args.end_month:
        end_month = datetime.strptime(args.end_month, '%Y-%m').date().replace(day=1)
    else:
        end_month = start_month
    
    is_initial = args.mode == 'initial'
    pipeline.run_forward(start_month, end_month, is_initial)
```

⚙️ **What happens**: 
- Parse month strings into date objects
- Set `is_initial` flag
- Call the forward processing function

📊 **Variables**:
```python
start_month = date(2024, 1, 1)
end_month = date(2024, 3, 1)
is_initial = False  # Because mode='forward'
```

🔄 **Next**: Call `pipeline.run_forward()`

---

### Step 11: Start Forward Processing

📍 **Location**: `summary_pipeline.py:377-392`

```python
def run_forward(
    self, 
    start_month: date, 
    end_month: date,
    is_initial: bool = False
):
    """Run forward (month-by-month) processing."""
    
    # Step 11.1: Log startup
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v4.0 - FORWARD MODE")
    logger.info(f"Processing: {start_month} to {end_month}")
    logger.info("=" * 80)
```

⚙️ **What happens**: Log the processing range

📊 **Console output**:
```
================================================================================
SUMMARY PIPELINE v4.0 - FORWARD MODE
Processing: 2024-01-01 to 2024-03-01
================================================================================
```

---

### Step 12: Enter Main Processing Loop

📍 **Location**: `summary_pipeline.py:394-396`

```python
# Step 12.1: Initialize loop
month = start_month
while month <= end_month:
    month_start = time.time()
```

⚙️ **What happens**: Start looping through months (2024-01, 2024-02, 2024-03)

📊 **Loop state**:
```python
Iteration 1: month = 2024-01-01
Iteration 2: month = 2024-02-01
Iteration 3: month = 2024-03-01
```

---

### Step 13: Check for Table Optimization

📍 **Location**: `summary_pipeline.py:399-401`

```python
# Step 13.1: Optimize tables periodically
if self.optimization_counter >= self.config.snapshot_interval:
    self._optimize_tables()
    self.optimization_counter = 0
```

⚙️ **What happens**: 
- Check if we've processed 12 months (default interval)
- If yes, compact Iceberg tables to improve performance
- Reset counter

📊 **Current state** (first iteration):
```python
self.optimization_counter = 0
self.config.snapshot_interval = 12
0 >= 12? No, skip optimization
```

---

### Step 14: Check Initial State

📍 **Location**: `summary_pipeline.py:404`

```python
# Step 14.1: Check/sync with existing data
is_initial = self._check_initial_state(is_initial, month)
```

⚙️ **What happens**: Need to verify if this is truly initial load or continuation

🔄 **Next**: Call `_check_initial_state()`

---

### Step 15: Verify Initial vs Incremental

📍 **Location**: `summary_pipeline.py:493-507`

```python
def _check_initial_state(self, is_initial: bool, current_month: date) -> bool:
    """Check if this should be initial load or continuation."""
    month_str = current_month.strftime('%Y-%m')
    
    # Step 15.1: Check if destination table exists
    if self.spark.catalog.tableExists(self.config.destination_table):
        # Step 15.2: Get max month from existing data
        max_month = self.spark.table(self.config.destination_table) \
            .agg(F.max(self.config.partition_column).alias("max_month")) \
            .first()["max_month"]
        
        if max_month is not None:
            is_initial = False
            # Step 15.3: Validate we're not going backwards
            if max_month > month_str:
                raise ValueError(f"Processing month {month_str} < max existing {max_month}")
    
    return is_initial
```

⚙️ **What happens**:
- Check if `default.summary` table exists
- If exists, get the latest month in the table
- If data exists, switch to incremental mode
- Validate we're not trying to process a month that's already done

📊 **Scenario 1 - First Run** (table doesn't exist):
```python
tableExists("default.summary") = False
return is_initial = False (unchanged from parameter)
```

📊 **Scenario 2 - Continuation** (table has data up to 2023-12):
```python
tableExists("default.summary") = True
max_month = "2023-12"
current_month = "2024-01"
"2023-12" > "2024-01"? No, OK to continue
return is_initial = False
```

📊 **Scenario 3 - Error** (trying to go backwards):
```python
max_month = "2024-06"
current_month = "2024-01"
"2024-06" > "2024-01"? Yes
raise ValueError("Processing month 2024-01 < max existing 2024-06")
```

🔄 **Return to**: `run_forward()` at line 404

---

### Step 16: Process Single Month

📍 **Location**: `summary_pipeline.py:407`

```python
# Step 16.1: Process month
success = self._process_single_month(month, is_initial)
```

⚙️ **What happens**: Process one month's data (this is the core logic!)

🔄 **Next**: Call `_process_single_month()` - **This is where most work happens**

---

### Step 17: Read Source Data

📍 **Location**: `summary_pipeline.py:509-517`

```python
def _process_single_month(self, month: date, is_initial: bool) -> bool:
    """Process a single month's data."""
    month_str = month.strftime('%Y-%m')
    logger.info(f"Processing month: {month_str}")
    
    # Step 17.1: Read source data for this month
    source_df = self.spark.read.table(self.config.source_table) \
        .select(*self.config.get_select_expressions()) \
        .filter(F.col(self.config.partition_column) == month_str)
```

⚙️ **What happens**:
- Format month as "YYYY-MM" string
- Read from `default.default.accounts_all` table
- Apply column name mappings (source → destination names)
- Filter to only this month's data

📊 **SQL executed**:
```sql
SELECT 
  cons_acct_key as cons_acct_key,
  bureau_mbr_id as bureau_member_id,
  acct_bal_am as balance_am,
  days_past_due_ct_4in as days_past_due,
  -- ... all column mappings
FROM default.default.accounts_all
WHERE rpt_as_of_mo = '2024-01'
```

📊 **Data at this point**:
```
+---------------+----------------+----------+---------------+-------------+
| cons_acct_key | bureau_member | balance_ | days_past_due | rpt_as_of_mo|
|               |      _id       |    am    |               |             |
+---------------+----------------+----------+---------------+-------------+
| 1             | XYZ123         | 10000    | 0             | 2024-01     |
| 2             | ABC456         | 5000     | 30            | 2024-01     |
| 3             | DEF789         | 15000    | 0             | 2024-01     |
| ...           | ...            | ...      | ...           | ...         |
+---------------+----------------+----------+---------------+-------------+
```

---

### Step 18: Deduplicate Source Data

📍 **Location**: `summary_pipeline.py:520-526`

```python
# Step 18.1: Deduplicate by primary key, keeping latest
window_spec = Window.partitionBy(self.config.primary_column) \
    .orderBy(F.col(self.config.max_identifier_column).desc())

source_df = source_df \
    .withColumn("_rn", F.row_number().over(window_spec)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn")
```

⚙️ **What happens**:
- If multiple rows exist for same account in same month, keep only the latest (by `base_ts`)
- Use window function to assign row numbers within each account
- Keep only row number 1

📊 **Before deduplication**:
```
+---------------+-------------+----------+---------------------+
| cons_acct_key | rpt_as_of_mo| balance_ | base_ts             |
|               |             |    am    |                     |
+---------------+-------------+----------+---------------------+
| 1             | 2024-01     | 10000    | 2024-01-15 10:00:00 |
| 1             | 2024-01     | 10500    | 2024-01-20 14:30:00 | ← Later timestamp
| 2             | 2024-01     | 5000     | 2024-01-15 10:00:00 |
+---------------+-------------+----------+---------------------+
```

📊 **After deduplication**:
```
+---------------+-------------+----------+---------------------+
| cons_acct_key | rpt_as_of_mo| balance_ | base_ts             |
|               |             |    am    |                     |
+---------------+-------------+----------+---------------------+
| 1             | 2024-01     | 10500    | 2024-01-20 14:30:00 | ← Kept latest
| 2             | 2024-01     | 5000     | 2024-01-15 10:00:00 |
+---------------+-------------+----------+---------------------+
```

---

### Step 19: Apply Transformations

📍 **Location**: `summary_pipeline.py:529-540`

```python
# Step 19.1: Apply column transformations
for transform in self.config.column_transformations:
    source_df = source_df.withColumn(
        transform['name'],
        F.expr(transform['mapper_expr'])
    )

# Step 19.2: Apply inferred columns
for inferred in self.config.inferred_columns:
    source_df = source_df.withColumn(
        inferred['name'],
        F.expr(inferred['mapper_expr'])
    )
```

⚙️ **What happens**:
- Apply data cleaning rules (e.g., replace empty strings with '999')
- Apply business logic (e.g., calculate derived fields)

📊 **Example transformation**:
```python
# Column transformation
transform = {
    "name": "acct_type_dtl_cd",
    "mapper_expr": "CASE WHEN acct_type_dtl_cd = '' OR acct_type_dtl_cd IS NULL THEN '999' ELSE acct_type_dtl_cd END"
}

Before: acct_type_dtl_cd = ''
After:  acct_type_dtl_cd = '999'
```

📊 **Example inferred column**:
```python
# Inferred column
inferred = {
    "name": "orig_loan_am",
    "mapper_expr": "CASE WHEN acct_type_dtl_cd IN ('5', '214', '220') THEN hi_credit_am ELSE credit_limit_am END"
}

If acct_type_dtl_cd = '5' and hi_credit_am = 50000:
  orig_loan_am = 50000
  
If acct_type_dtl_cd = '1' and credit_limit_am = 20000:
  orig_loan_am = 20000
```

---

### Step 20: Check Record Count

📍 **Location**: `summary_pipeline.py:543-548`

```python
# Step 20.1: Check record count
record_count = source_df.count()
if record_count == 0:
    logger.info(f"No records for {month_str}")
    return False

logger.info(f"Records to process: {record_count}")
```

⚙️ **What happens**:
- Count how many records we have for this month
- If zero, skip this month and return
- Log the count

📊 **Example**:
```
Console: "Records to process: 100"
```

---

### Step 21: Calculate Optimal Partitions

📍 **Location**: `summary_pipeline.py:551-552`

```python
# Step 21.1: Configure partitioning
num_partitions = calculate_optimal_partitions(record_count, self.config)
configure_spark_partitions(self.spark, num_partitions)
```

⚙️ **What happens**: Need to determine how many Spark partitions to use

🔄 **Next**: Call `calculate_optimal_partitions()`

---

### Step 22: Partition Calculation

📍 **Location**: `summary_pipeline.py:176-184`

```python
def calculate_optimal_partitions(record_count: int, config: SummaryConfig) -> int:
    """Calculate optimal partition count based on data size."""
    # Step 22.1: Calculate by record count
    partitions_by_count = record_count / config.target_records_per_partition
    
    # Step 22.2: Calculate by data size
    data_size_mb = (record_count * config.avg_record_size_bytes) / (1024 * 1024)
    partitions_by_size = data_size_mb / config.max_file_size_mb
    
    # Step 22.3: Take the max
    optimal = max(partitions_by_count, partitions_by_size)
    
    # Step 22.4: Round to power of 2
    power_of_2 = 2 ** round(math.log2(max(optimal, 1)))
    
    # Step 22.5: Clamp to min/max
    return int(max(config.min_partitions, min(config.max_partitions, power_of_2)))
```

⚙️ **What happens**: Use formula to calculate optimal partitions

📊 **Example calculation** (100 records):
```python
record_count = 100
target_records_per_partition = 10_000_000
avg_record_size_bytes = 200
max_file_size_mb = 256
min_partitions = 16
max_partitions = 8192

# Step 22.1
partitions_by_count = 100 / 10_000_000 = 0.00001

# Step 22.2
data_size_mb = (100 * 200) / (1024 * 1024) = 0.019 MB
partitions_by_size = 0.019 / 256 = 0.000074

# Step 22.3
optimal = max(0.00001, 0.000074) = 0.000074

# Step 22.4
power_of_2 = 2^round(log2(0.000074)) = 2^(-14) ≈ 0.000061 → rounds to 1

# Step 22.5
final = max(16, min(8192, 1)) = 16
```

📊 **Result**: `num_partitions = 16`

🔄 **Next**: Call `configure_spark_partitions()`

---

### Step 23: Set Spark Partitions

📍 **Location**: `summary_pipeline.py:187-191`

```python
def configure_spark_partitions(spark: SparkSession, num_partitions: int):
    """Dynamically configure Spark for partition count."""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    logger.info(f"Configured Spark for {num_partitions} partitions")
```

⚙️ **What happens**: Set Spark configuration for this month's processing

📊 **Console output**:
```
INFO: Configured Spark for 16 partitions
```

🔄 **Return to**: `_process_single_month()` at line 552

---

### Step 24: Repartition and Cache

📍 **Location**: `summary_pipeline.py:554-555`

```python
source_df = source_df.repartition(num_partitions, F.col(self.config.primary_column))
source_df.cache()
```

⚙️ **What happens**:
- Redistribute data across 16 partitions by account key
- Cache in memory (we'll use this data multiple times)

📊 **Effect**:
- Ensures accounts are co-located in same partition
- Faster joins later
- Data stays in memory for reuse

---

### Step 25: Build Summary (Branch Point)

📍 **Location**: `summary_pipeline.py:558-561`

```python
# Step 25.1: Build result
if is_initial:
    result = self._build_initial_summary(source_df)
else:
    result = self._build_incremental_summary(source_df, month_str)
```

⚙️ **What happens**: Branch based on whether this is first month or not

📊 **Decision**:
```
is_initial = False (we checked earlier)
→ Take ELSE branch
→ Call _build_incremental_summary()
```

🔄 **Next**: Call `_build_incremental_summary()`

---

### Step 26: Build Incremental Summary - Get Previous State

📍 **Location**: `summary_pipeline.py:606-625`

```python
def _build_incremental_summary(self, source_df: DataFrame, month_str: str) -> DataFrame:
    """Build incremental summary by shifting history arrays."""
    logger.info("Building incremental summary...")
    
    # Step 26.1: Get previous state from latest_summary
    latest_prev = self.spark.read.table(self.config.latest_history_table).select(
        self.config.primary_column,
        self.config.primary_date_column,
        self.config.partition_column,
        self.config.max_identifier_column,
        *self.config.rolling_history_cols
    )
```

⚙️ **What happens**: Read the latest state of each account from `latest_summary` table

📊 **SQL executed**:
```sql
SELECT 
  cons_acct_key,
  acct_dt,
  rpt_as_of_mo,
  base_ts,
  balance_am_history,
  actual_payment_am_history,
  credit_limit_am_history,
  past_due_am_history,
  payment_rating_cd_history,
  days_past_due_history,
  asset_class_cd_history
FROM default.latest_summary
```

📊 **Data loaded**:
```
+---------------+------------+----------+--------+----------------------------+
| cons_acct_key | rpt_as_of_ | acct_dt  | base_ts| balance_am_history         |
|               |     mo     |          |        | (36 elements)              |
+---------------+------------+----------+--------+----------------------------+
| 1             | 2023-12    | 2023-12  | ...    | [9500, 9000, 8500, ...]    |
| 2             | 2023-12    | 2023-12  | ...    | [4800, 5200, 5500, ...]    |
| 3             | 2023-12    | 2023-12  | ...    | [14500, 15000, 14800, ...] |
+---------------+------------+----------+--------+----------------------------+
```

---

📍 **Location**: `summary_pipeline.py:620-625`

```python
    # Step 26.2: Count and repartition
    prev_count = latest_prev.count()
    logger.info(f"Previous records: {prev_count}")
    
    num_partitions = calculate_optimal_partitions(prev_count, self.config)
    configure_spark_partitions(self.spark, num_partitions)
    
    latest_prev = latest_prev.repartition(num_partitions, self.config.primary_column)
```

⚙️ **What happens**:
- Count previous records (e.g., 100 accounts)
- Recalculate partitions for join
- Repartition by account key for efficient join

📊 **Console**:
```
INFO: Previous records: 100
INFO: Configured Spark for 16 partitions
```

---

### Step 27: Apply Mapper Expressions

📍 **Location**: `summary_pipeline.py:628-632`

```python
# Step 27.1: Apply mapper expressions to source
for rolling_col in self.config.rolling_columns:
    source_df = source_df.withColumn(
        rolling_col['mapper_column'],
        F.expr(rolling_col['mapper_expr'])
    )
```

⚙️ **What happens**: Apply transformations to get the values we'll store in arrays

📊 **Example**:
```python
# For balance_am
rolling_col = {
    "mapper_column": "balance_am",
    "mapper_expr": "CASE WHEN balance_am IN (-2147483647, -1) THEN NULL ELSE balance_am END"
}

Before: balance_am = -1
After:  balance_am = NULL

Before: balance_am = 10000
After:  balance_am = 10000
```

---

### Step 28: Join Current with Previous

📍 **Location**: `summary_pipeline.py:635-639`

```python
# Step 28.1: Join current with previous
joined = source_df.alias("c").join(
    latest_prev.alias("p"),
    self.config.primary_column,
    "left"
)
```

⚙️ **What happens**: Join current month's data with previous state

📊 **Join logic**:
```sql
SELECT *
FROM source_df c
LEFT JOIN latest_prev p
  ON c.cons_acct_key = p.cons_acct_key
```

📊 **Result** (simplified view):
```
+-------+----------+----------+---------------+-----------------------+
| c.cons| c.balance| c.rpt_as | p.rpt_as_of_mo| p.balance_am_history  |
| _acct |    _am   |  _of_mo  |               | (from 2023-12)        |
| _key  |          |          |               |                       |
+-------+----------+----------+---------------+-----------------------+
| 1     | 10000    | 2024-01  | 2023-12       | [9500, 9000, ...]     |
| 2     | 5000     | 2024-01  | 2023-12       | [4800, 5200, ...]     |
| 4     | 8000     | 2024-01  | NULL          | NULL                  | ← New account
+-------+----------+----------+---------------+-----------------------+
```

---

### Step 29: Calculate Month Difference

📍 **Location**: `summary_pipeline.py:642-651`

```python
# Step 29.1: Calculate month difference
joined = joined.withColumn(
    "MONTH_DIFF",
    F.when(
        F.col(f"p.{self.config.partition_column}").isNotNull(),
        F.months_between(
            F.to_date(F.lit(month_str), "yyyy-MM"),
            F.to_date(F.col(f"p.{self.config.partition_column}"), "yyyy-MM")
        ).cast(IntegerType())
    ).otherwise(1)
)
```

⚙️ **What happens**: Calculate how many months passed since last data

📊 **Calculation examples**:
```python
# Account 1: Normal (previous month was 2023-12, current is 2024-01)
current_month = "2024-01"
p.rpt_as_of_mo = "2023-12"
MONTH_DIFF = months_between("2024-01", "2023-12") = 1

# Account 2: Gap (previous month was 2023-10, current is 2024-01)
current_month = "2024-01"
p.rpt_as_of_mo = "2023-10"
MONTH_DIFF = months_between("2024-01", "2023-10") = 3

# Account 4: New account (no previous data)
p.rpt_as_of_mo = NULL
MONTH_DIFF = 1  # Default for new accounts
```

📊 **Data now includes**:
```
+-------+----------+---------------+------------+
| c.cons| c.balance| p.rpt_as_of_mo| MONTH_DIFF |
| _acct |    _am   |               |            |
| _key  |          |               |            |
+-------+----------+---------------+------------+
| 1     | 10000    | 2023-12       | 1          | ← Normal
| 2     | 5000     | 2023-10       | 3          | ← Gap!
| 4     | 8000     | NULL          | 1          | ← New
+-------+----------+---------------+------------+
```

---

### Step 30: Build Select Expressions

📍 **Location**: `summary_pipeline.py:653-710`

```python
# Step 30.1: Initialize list
select_exprs = []

# Step 30.2: Non-rolling columns (from current)
for src, dst in self.config.columns.items():
    if dst not in self.config.coalesce_exclusion_cols and dst not in self.config.rolling_mapper_list:
        select_exprs.append(F.col(f"c.{dst}").alias(dst))

# Step 30.3: Inferred columns
for inferred in self.config.inferred_columns:
    select_exprs.append(F.col(f"c.{inferred['name']}").alias(inferred['name']))
```

⚙️ **What happens**: Start building the SELECT statement for final result

📊 **Columns added so far**:
```python
select_exprs = [
    F.col("c.cons_acct_key").alias("cons_acct_key"),
    F.col("c.bureau_member_id").alias("bureau_member_id"),
    F.col("c.open_dt").alias("open_dt"),
    F.col("c.closed_dt").alias("closed_dt"),
    # ... all non-rolling columns
    F.col("c.orig_loan_am").alias("orig_loan_am"),  # Inferred column
]
```

---

📍 **Location**: `summary_pipeline.py:666-694`

```python
# Step 30.4: Rolling columns (mapper values + shifted histories)
for rolling_col in self.config.rolling_columns:
    mapper_col = rolling_col['mapper_column']
    data_type = rolling_col['type']
    
    # Step 30.4a: Mapper column value
    if data_type == "Integer":
        select_exprs.append(
            F.coalesce(
                F.col(f"c.{mapper_col}"),
                F.lit(None).cast(IntegerType())
            ).alias(mapper_col)
        )
    else:
        select_exprs.append(
            F.coalesce(
                F.col(f"c.{mapper_col}"),
                F.lit(None).cast(StringType())
            ).alias(mapper_col)
        )
    
    # Step 30.4b: Shifted history array
    select_exprs.append(
        RollingColumnBuilder.shifted_history_array(
            rolling_col['name'],
            mapper_col,
            data_type,
            rolling_col['num_cols']
        )
    )
```

⚙️ **What happens**: For each rolling column, add:
1. Current value (with NULL handling)
2. Shifted history array

🔄 **Next**: Call `RollingColumnBuilder.shifted_history_array()` - **Key transformation!**

---

### Step 31: Build Shifted Array (THE KEY LOGIC!)

📍 **Location**: `summary_pipeline.py:247-313`

```python
@staticmethod
def shifted_history_array(
    rolling_col: str,
    mapper_column: str,
    data_type: str,
    num_cols: int,
    current_alias: str = "c",
    previous_alias: str = "p"
) -> Column:
```

⚙️ **What happens**: This is the core algorithm that creates rolling arrays!

---

📍 **Location**: `summary_pipeline.py:265-276`

```python
    # Step 31.1: Extract current value
    current_value = F.coalesce(
        F.col(f"{current_alias}.{mapper_column}").cast(spark_type),
        F.lit(None).cast(spark_type)
    )
    
    # Step 31.2: Extract previous history
    prev_history = F.coalesce(
        F.col(f"{previous_alias}.{rolling_col}_history"),
        F.array(*[F.lit(None).cast(spark_type) for _ in range(num_cols)])
    )
```

⚙️ **What happens**: Get current value and previous array (with NULL fallbacks)

📊 **For Account 1** (normal):
```python
current_value = 10000
prev_history = [9500, 9000, 8500, ...]
```

📊 **For Account 4** (new):
```python
current_value = 8000
prev_history = NULL → fallback to [NULL, NULL, NULL, ...]
```

---

📍 **Location**: `summary_pipeline.py:290-313`

```python
    # Step 31.3: Build the array based on MONTH_DIFF
    return (
        F.when(
            F.col("MONTH_DIFF") == 1,
            # CASE 1: Normal (no gap)
            F.slice(
                F.concat(F.array(current_value), prev_history),
                1, num_cols
            )
        )
        .when(
            F.col("MONTH_DIFF") > 1,
            # CASE 2: Gap detected
            F.slice(
                F.concat(
                    F.array(current_value),
                    null_array(F.col("MONTH_DIFF") - F.lit(1)),
                    prev_history
                ),
                1, num_cols
            )
        )
        .otherwise(new_account_array)
        .alias(f"{rolling_col}_history")
    )
```

⚙️ **What happens**: Three cases based on MONTH_DIFF

---

📊 **CASE 1: MONTH_DIFF = 1 (Normal, no gap)**

For Account 1:
```python
current_value = 10000
prev_history = [9500, 9000, 8500, 8200, ...]

Step 1: concat([10000], [9500, 9000, 8500, 8200, ...])
      = [10000, 9500, 9000, 8500, 8200, ...]

Step 2: slice(array, 1, 36)
      = [10000, 9500, 9000, 8500, 8200, ..., prev[34]]  # Keep first 36
      
Result: balance_am_history = [10000, 9500, 9000, ..., prev[34]]
        ↑ new   ↑ previous values shifted right →
```

---

📊 **CASE 2: MONTH_DIFF > 1 (Gap detected)**

For Account 2 (MONTH_DIFF = 3):
```python
current_value = 5000
MONTH_DIFF = 3
prev_history = [4800, 5200, 5500, ...]

Step 1: Create gap array
        null_array(3 - 1) = null_array(2) = [NULL, NULL]

Step 2: concat([5000], [NULL, NULL], [4800, 5200, 5500, ...])
      = [5000, NULL, NULL, 4800, 5200, 5500, ...]

Step 3: slice(array, 1, 36)
      = [5000, NULL, NULL, 4800, 5200, 5500, ..., prev[32]]
      
Result: balance_am_history = [5000, NULL, NULL, 4800, 5200, ...]
        ↑ new  ↑ gap ↑ gap  ↑ previous values
```

📊 **Interpretation**:
```
Position 0: 2024-01 = 5000     ← Current month
Position 1: 2023-12 = NULL     ← Missing month (gap)
Position 2: 2023-11 = NULL     ← Missing month (gap)
Position 3: 2023-10 = 4800     ← Last known value
Position 4: 2023-09 = 5200     ← Earlier value
...
```

---

📊 **CASE 3: MONTH_DIFF = NULL or 0 (New account)**

For Account 4:
```python
current_value = 8000
prev_history = [NULL, NULL, ...]  # No previous data

Result: new_account_array = [8000, NULL, NULL, ..., NULL]
        ↑ new  ↑ no history
```

---

🔄 **Return to**: `_build_incremental_summary()` at line 694

---

### Step 32: Add Date Columns

📍 **Location**: `summary_pipeline.py:697-710`

```python
# Step 32.1: Date columns with coalesce
select_exprs.append(
    F.coalesce(
        F.col(f"c.{self.config.primary_date_column}"),
        F.to_date(F.lit(month_str))
    ).alias(self.config.primary_date_column)
)
select_exprs.append(
    F.coalesce(
        F.col(f"c.{self.config.partition_column}"),
        F.lit(month_str)
    ).alias(self.config.partition_column)
)

# Step 32.2: Execute the select
result = joined.select(*select_exprs)

return result
```

⚙️ **What happens**:
- Add date columns with fallbacks
- Execute all the transformations we built up
- Return the result DataFrame

📊 **Final result structure**:
```
+-------+----------+------------+-----------------------+-----------------------+
| cons_ | balance_ | rpt_as_of_ | balance_am_history    | payment_rating_cd_    |
| acct_ |    am    |     mo     | (36 ints)             | history (36 strings)  |
|  key  |          |            |                       |                       |
+-------+----------+------------+-----------------------+-----------------------+
| 1     | 10000    | 2024-01    | [10000, 9500, 9000,..| ["0", "0", "1", ...] |
| 2     | 5000     | 2024-01    | [5000, NULL, NULL,   | ["1", "?", "?", ...] |
|       |          |            |  4800, ...]          |                       |
| 4     | 8000     | 2024-01    | [8000, NULL, NULL,..| ["0", "?", "?", ...] |
+-------+----------+------------+-----------------------+-----------------------+
```

🔄 **Return to**: `_process_single_month()` at line 561

---

### Step 33: Generate Grid Columns

📍 **Location**: `summary_pipeline.py:564-565`

```python
# Step 33.1: Generate grid columns
for grid_config in self.config.grid_columns:
    result = generate_grid_column(result, grid_config, self.config.rolling_columns)
```

⚙️ **What happens**: Convert array columns to string grids

🔄 **Next**: Call `generate_grid_column()`

---

### Step 34: Transform Array to Grid

📍 **Location**: `summary_pipeline.py:320-350`

```python
def generate_grid_column(
    df: DataFrame,
    grid_config: Dict,
    rolling_columns: List[Dict]
) -> DataFrame:
    """Generate grid column from rolling history array."""
    
    # Step 34.1: Get config
    grid_name = grid_config['name']
    mapper_col = grid_config['mapper_rolling_column']
    placeholder = grid_config.get('placeholder', '?')
    separator = grid_config.get('separator', '')
    
    history_col = f"{mapper_col}_history"
```

📊 **For our config**:
```python
grid_name = "payment_history_grid"
mapper_col = "payment_rating_cd"
placeholder = "?"
separator = ""
history_col = "payment_rating_cd_history"
```

---

📍 **Location**: `summary_pipeline.py:339-348`

```python
    # Step 34.2: Transform array to string
    df = df.withColumn(
        grid_name,
        F.concat_ws(
            separator,
            F.transform(
                F.col(history_col),
                lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
            )
        )
    )
    
    return df
```

⚙️ **What happens**:
- Take each element in the array
- Convert to string (NULL → "?")
- Concatenate all elements

📊 **Example transformation**:
```python
# Account 1
payment_rating_cd_history = ["0", "0", "1", "0", "0", ...]

↓ F.transform(array, x -> coalesce(x, "?"))

["0", "0", "1", "0", "0", ...]  # No NULLs, no change

↓ F.concat_ws("", ...)

payment_history_grid = "00100..."  # 36 characters

# Account 2 (with gaps)
payment_rating_cd_history = ["1", NULL, NULL, "0", "0", ...]

↓ F.transform(array, x -> coalesce(x, "?"))

["1", "?", "?", "0", "0", ...]

↓ F.concat_ws("", ...)

payment_history_grid = "1??00..."  # 36 characters, ? = gap
```

🔄 **Return to**: `_process_single_month()` at line 565

---

### Step 35: Clean Date Columns

📍 **Location**: `summary_pipeline.py:568-569`

```python
# Step 35.1: Clean date columns
result = replace_invalid_years(result, self.config.date_col_list)
result.cache()
```

🔄 **Next**: Call `replace_invalid_years()`

---

### Step 36: Replace Invalid Dates

📍 **Location**: `summary_pipeline.py:194-203`

```python
def replace_invalid_years(df: DataFrame, columns: List[str]) -> DataFrame:
    """Replace invalid year values (< 1000) with NULL."""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                F.when(F.year(F.col(col_name)) < 1000, None)
                .otherwise(F.to_date(F.col(col_name)))
            )
    return df
```

⚙️ **What happens**: Clean bad legacy dates like "0001-01-01"

📊 **Example**:
```python
columns = ["open_dt", "closed_dt", "last_payment_dt", ...]

Before: open_dt = "0001-01-01"
After:  open_dt = NULL

Before: closed_dt = "2023-12-15"
After:  closed_dt = "2023-12-15"  # Valid, unchanged
```

🔄 **Return to**: `_process_single_month()` at line 569

---

### Step 37: Write Results

📍 **Location**: `summary_pipeline.py:572`

```python
# Step 37.1: Write results
self._write_results(result, is_initial)
```

⚙️ **What happens**: Write to Iceberg tables

🔄 **Next**: Call `_write_results()`

---

### Step 38: Write to Tables

📍 **Location**: `summary_pipeline.py:714-767`

```python
def _write_results(self, result: DataFrame, is_initial: bool):
    """Write results to summary and latest_summary tables."""
    
    # Step 38.1: Check if we have data
    if result.count() == 0:
        logger.info("No records to write")
        return
    
    # Step 38.2: Match schema
    target_cols = [f.name for f in self.spark.table(self.config.destination_table).schema.fields]
    select_cols = [c for c in target_cols if c in result.columns]
    result_matched = result.select(*select_cols)
```

⚙️ **What happens**: Ensure our DataFrame matches the target table schema

---

📍 **Location**: `summary_pipeline.py:739-751`

```python
else:
    # Step 38.3: MERGE for updates (incremental mode)
    logger.info("Merging to summary...")
    result_matched.createOrReplaceTempView("result")
    
    self.spark.sql(f"""
        MERGE INTO {self.config.destination_table} d
        USING result r
        ON d.{self.config.primary_column} = r.{self.config.primary_column} 
           AND d.{self.config.partition_column} = r.{self.config.partition_column}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

⚙️ **What happens**: Execute MERGE (upsert) to summary table

📊 **SQL executed**:
```sql
MERGE INTO default.summary d
USING result r
ON d.cons_acct_key = r.cons_acct_key 
   AND d.rpt_as_of_mo = r.rpt_as_of_mo
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

📊 **What this does**:
```
For Account 1, Month 2024-01:
  - Exists in summary? NO
  - Action: INSERT new row

For Account 2, Month 2024-01:
  - Exists in summary? NO
  - Action: INSERT new row

For Account 3, Month 2024-01 (if already existed):
  - Exists in summary? YES
  - Action: UPDATE existing row with new values
```

---

📍 **Location**: `summary_pipeline.py:754-767`

```python
    # Step 38.4: MERGE to latest_summary
    logger.info("Merging to latest_summary...")
    latest_target_cols = [f.name for f in self.spark.table(self.config.latest_history_table).schema.fields]
    latest_select_cols = [c for c in latest_target_cols if c in result.columns]
    
    result.select(*latest_select_cols) \
        .createOrReplaceTempView("result_latest")
    
    self.spark.sql(f"""
        MERGE INTO {self.config.latest_history_table} d
        USING result_latest r
        ON d.{self.config.primary_column} = r.{self.config.primary_column}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

⚙️ **What happens**: Update latest_summary (only latest month per account)

📊 **SQL executed**:
```sql
MERGE INTO default.latest_summary d
USING result_latest r
ON d.cons_acct_key = r.cons_acct_key  ← Only match on account
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

📊 **Effect**:
```
Account 1: 
  - Was: 2023-12 data
  - Now: 2024-01 data (UPDATED)

Account 4:
  - Was: Didn't exist
  - Now: 2024-01 data (INSERTED)
```

🔄 **Return to**: `_process_single_month()` at line 572

---

### Step 39: Cleanup

📍 **Location**: `summary_pipeline.py:575-578`

```python
# Step 39.1: Cleanup cached data
source_df.unpersist()
result.unpersist()

return True
```

⚙️ **What happens**: Remove cached DataFrames from memory

🔄 **Return to**: `run_forward()` at line 407

---

### Step 40: Track Success and Log

📍 **Location**: `summary_pipeline.py:409-416`

```python
if success:
    is_initial = False
    self.optimization_counter += 1

# Log timing
elapsed = (time.time() - month_start) / 60
logger.info(f"Month {month.strftime('%Y-%m')} completed in {elapsed:.2f} minutes")
logger.info("-" * 80)
```

⚙️ **What happens**:
- Mark as successful
- Increment optimization counter
- Log how long this month took

📊 **Console output**:
```
Month 2024-01 completed in 0.52 minutes
--------------------------------------------------------------------------------
```

---

### Step 41: Move to Next Month

📍 **Location**: `summary_pipeline.py:419`

```python
# Next month
month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
```

⚙️ **What happens**: Safely increment to next month

📊 **Month progression**:
```python
Iteration 1: month = 2024-01-01
  ↓ (2024-01-28 + 4 days).replace(day=1)
  ↓ (2024-02-01).replace(day=1)
  ↓ 2024-02-01

Iteration 2: month = 2024-02-01
  ↓ (2024-02-28 + 4 days).replace(day=1)
  ↓ (2024-03-03).replace(day=1)
  ↓ 2024-03-01

Iteration 3: month = 2024-03-01
  ↓ (2024-03-28 + 4 days).replace(day=1)
  ↓ (2024-04-01).replace(day=1)
  ↓ 2024-04-01

Loop check: 2024-04-01 <= 2024-03-01? NO → Exit loop
```

🔄 **Next**: Back to Step 12 (loop condition check)

---

### Step 42: Loop Complete - Final Optimization

📍 **Location**: `summary_pipeline.py:421-425`

```python
# Step 42.1: Final optimization
self._optimize_tables()

total_time = (time.time() - self.start_time) / 60
logger.info(f"Pipeline completed in {total_time:.2f} minutes")
```

⚙️ **What happens**: After all months processed, optimize tables one final time

🔄 **Next**: Call `_optimize_tables()`

---

### Step 43: Table Optimization

📍 **Location**: `summary_pipeline.py:1181-1199`

```python
def _optimize_tables(self):
    """Optimize Iceberg tables."""
    logger.info("Optimizing tables...")
    
    for table in [self.config.latest_history_table]:
        try:
            parts = table.split('.')
            if len(parts) >= 2:
                catalog = parts[0] if len(parts) == 3 else "spark_catalog"
                db = parts[-2]
                tbl = parts[-1]
                
                # Step 43.1: Compact small files
                logger.info(f"Rewriting data files for {table}...")
                self.spark.sql(f"CALL {catalog}.system.rewrite_data_files('{db}.{tbl}')")
                
                # Step 43.2: Remove old snapshots
                logger.info(f"Expiring snapshots for {table}...")
                self.spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{db}.{tbl}', retain_last => 1)")
        except Exception as e:
            logger.warning(f"Could not optimize {table}: {e}")
```

⚙️ **What happens**:
- Compact small files into larger ones (improves read performance)
- Delete old table snapshots (saves space)

📊 **SQL executed**:
```sql
CALL default.system.rewrite_data_files('default.latest_summary');
CALL default.system.expire_snapshots(table => 'default.latest_summary', retain_last => 1);
```

📊 **Console output**:
```
INFO: Optimizing tables...
INFO: Rewriting data files for default.latest_summary...
INFO: Expiring snapshots for default.latest_summary...
```

🔄 **Return to**: `run_forward()` at line 425

---

### Step 44: Pipeline Complete

📍 **Location**: `summary_pipeline.py:424-425`

```python
total_time = (time.time() - self.start_time) / 60
logger.info(f"Pipeline completed in {total_time:.2f} minutes")
```

⚙️ **What happens**: Log total execution time

📊 **Console output**:
```
Pipeline completed in 1.58 minutes
```

🔄 **Return to**: `main()` at line 1253

---

### Step 45: Cleanup and Exit

📍 **Location**: `summary_pipeline.py:1258-1262`

```python
except Exception as e:
    logger.error(f"Pipeline failed: {e}", exc_info=True)
    sys.exit(1)
finally:
    spark.stop()
```

⚙️ **What happens**:
- If any errors occurred, log them and exit with error code
- Always stop the Spark session (cleanup)

📊 **Success case**:
```
Spark session stopped
Process exits with code 0 (success)
```

---

## Summary of Forward Mode Execution

### Complete Flow

```
1. User runs command
2. main() called
3. Parse arguments
4. Load config from JSON
5. Create Spark session with optimizations
6. Create SummaryPipeline object
7. Call run_forward()
8. Loop through months:
   For each month:
     9.  Check if optimization needed
     10. Verify initial vs incremental
     11. Call _process_single_month()
         12. Read source data for month
         13. Deduplicate by account key
         14. Apply transformations
         15. Calculate optimal partitions
         16. Repartition and cache
         17. Branch: Initial or Incremental?
             If Incremental:
               18. Read previous state from latest_summary
               19. Join current with previous
               20. Calculate MONTH_DIFF
               21. For each rolling column:
                   - Apply mapper expression
                   - Call shifted_history_array()
                     • MONTH_DIFF = 1: Normal shift
                     • MONTH_DIFF > 1: Insert gap NULLs
                     • Otherwise: New account array
               22. Generate grid columns (array → string)
               23. Clean invalid dates
         24. Write to summary (MERGE)
         25. Write to latest_summary (MERGE)
         26. Cleanup cache
     27. Increment to next month
28. Final optimization (compact tables)
29. Stop Spark session
30. Exit
```

### Data Transformation Example

**Input** (accounts_all for 2024-01):
```
cons_acct_key=1, balance_am=10000, days_past_due=0
```

**Previous State** (latest_summary):
```
cons_acct_key=1, balance_am_history=[9500, 9000, 8500, ...]
```

**Output** (summary for 2024-01):
```
cons_acct_key=1, 
balance_am=10000,
balance_am_history=[10000, 9500, 9000, 8500, ...],
payment_history_grid="00100...",
rpt_as_of_mo=2024-01
```

### Key Algorithms

1. **Month Difference Calculation**: Determines gap handling
2. **Array Shifting**: Prepends new value, shifts old values right
3. **Gap Handling**: Inserts NULL values for missing months
4. **Grid Generation**: Converts arrays to strings with placeholders

---

## Backfill Mode Execution

### User Command

```bash
docker exec spark-iceberg spark-submit \
  /home/iceberg/notebooks/notebooks/summary_v4/summary_pipeline.py \
  --config /home/iceberg/notebooks/notebooks/summary_v4/summary_config.json \
  --mode backfill \
  --backfill-filter "rpt_as_of_mo = '2024-02' AND cons_acct_key IN (1, 2, 3)"
```

### Steps 1-10: Same as Forward Mode

(Parse args, load config, create Spark session, create pipeline object)

### Step 11b: Start Backfill Processing

📍 **Location**: `summary_pipeline.py:1256`

```python
elif args.mode == 'backfill':
    pipeline.run_backfill(source_filter=args.backfill_filter)
```

🔄 **Next**: Call `run_backfill()`

---

### Step 12b: Get Backfill Data

📍 **Location**: `summary_pipeline.py:427-464`

```python
def run_backfill(
    self,
    batch_df: Optional[DataFrame] = None,
    source_filter: Optional[str] = None
):
    """Run backfill processing for late-arriving data."""
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v4.0 - BACKFILL MODE")
    logger.info("=" * 80)
    
    # Step 12b.1: Get backfill batch
    if batch_df is None:
        if source_filter:
            batch_df = self.spark.sql(f"""
                SELECT * FROM {self.config.source_table}
                WHERE {source_filter}
            """)
```

⚙️ **What happens**: Load the data that arrived late

📊 **SQL executed**:
```sql
SELECT * FROM default.default.accounts_all
WHERE rpt_as_of_mo = '2024-02' AND cons_acct_key IN (1, 2, 3)
```

📊 **Data loaded**:
```
+---------------+-------------+----------+---------------+---------------------+
| cons_acct_key | rpt_as_of_mo| balance_ | days_past_due | base_ts             |
|               |             |    am    |               |                     |
+---------------+-------------+----------+---------------+---------------------+
| 1             | 2024-02     | 10500    | 60            | 2024-06-20 10:00:00 | ← NEWER ts!
| 2             | 2024-02     | 5500     | 0             | 2024-06-20 10:00:00 |
| 3             | 2024-02     | 15500    | 30            | 2024-06-20 10:00:00 |
+---------------+-------------+----------+---------------+---------------------+
```

**Note**: `base_ts` is June 2024, but data is for February 2024 → Late arrival!

---

### Step 13b: Prepare Backfill Data

📍 **Location**: `summary_pipeline.py:466`

```python
# Step 13b.1: Prepare batch with column mappings
batch_df = self._prepare_source_data(batch_df)
```

🔄 **Next**: Call `_prepare_source_data()`

---

### Step 14b: Apply Mappings and Transformations

📍 **Location**: `summary_pipeline.py:780-823`

```python
def _prepare_source_data(self, df: DataFrame) -> DataFrame:
    """Apply column mappings and transformations to source data."""
    
    # Step 14b.1: Select with column mapping
    result = df.select(*self.config.get_select_expressions())
    
    # Step 14b.2: Deduplicate
    window_spec = Window.partitionBy(
        self.config.primary_column, 
        self.config.partition_column
    ).orderBy(F.col(self.config.max_identifier_column).desc())
    
    result = result \
        .withColumn("_rn", F.row_number().over(window_spec)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")
    
    # Step 14b.3: Apply transformations
    for transform in self.config.column_transformations:
        result = result.withColumn(
            transform['name'],
            F.expr(transform['mapper_expr'])
        )
    
    for inferred in self.config.inferred_columns:
        result = result.withColumn(
            inferred['name'],
            F.expr(inferred['mapper_expr'])
        )
    
    # Step 14b.4: Apply rolling column mappers
    for rolling_col in self.config.rolling_columns:
        result = result.withColumn(
            rolling_col['mapper_column'],
            F.expr(rolling_col['mapper_expr'])
        )
    
    # Step 14b.5: Add month integer
    result = result.withColumn(
        "month_int",
        F.expr(month_to_int_expr(self.config.partition_column))
    )
    
    return result
```

⚙️ **What happens**: Same as forward mode, plus add `month_int` column

📊 **Data now includes**:
```
+-------+----------+----------+---------+
| cons_ | rpt_as_  | balance_ | month_  |
| acct_ | of_mo    |    am    |  int    |
|  key  |          |          |         |
+-------+----------+----------+---------+
| 1     | 2024-02  | 10500    | 24290   |
| 2     | 2024-02  | 5500     | 24290   |
| 3     | 2024-02  | 15500    | 24290   |
+-------+----------+----------+---------+

month_int = 2024 * 12 + 2 = 24290
```

🔄 **Return to**: `run_backfill()` at line 466

---

### Step 15b: Classify Records

📍 **Location**: `summary_pipeline.py:469`

```python
# Step 15b.1: Classify records
classified = self._classify_backfill_records(batch_df)
```

🔄 **Next**: Call `_classify_backfill_records()` - **Critical step!**

---

### Step 16b: Get Existing Summary Metadata

📍 **Location**: `summary_pipeline.py:825-838`

```python
def _classify_backfill_records(self, batch_df: DataFrame) -> DataFrame:
    """Classify records into Case I, II, III."""
    logger.info("Classifying backfill records...")
    
    # Step 16b.1: Get summary metadata
    summary_meta = self.spark.sql(f"""
        SELECT 
            {self.config.primary_column},
            MAX({self.config.partition_column}) as max_existing_month,
            MAX({self.config.max_identifier_column}) as max_existing_ts,
            {month_to_int_expr(f"MAX({self.config.partition_column})")} as max_month_int
        FROM {self.config.destination_table}
        GROUP BY {self.config.primary_column}
    """)
```

⚙️ **What happens**: For each account, get latest month and timestamp from summary

📊 **SQL executed**:
```sql
SELECT 
    cons_acct_key,
    MAX(rpt_as_of_mo) as max_existing_month,
    MAX(base_ts) as max_existing_ts,
    (CAST(SUBSTRING(MAX(rpt_as_of_mo), 1, 4) AS INT) * 12 + 
     CAST(SUBSTRING(MAX(rpt_as_of_mo), 6, 2) AS INT)) as max_month_int
FROM default.summary
GROUP BY cons_acct_key
```

📊 **Data loaded**:
```
+-------+------------------+--------------------+-------------+
| cons_ | max_existing_    | max_existing_ts    | max_month_  |
| acct_ |      month       |                    |     int     |
|  key  |                  |                    |             |
+-------+------------------+--------------------+-------------+
| 1     | 2024-06          | 2024-06-15 10:00   | 24294       |
| 2     | 2024-06          | 2024-06-15 10:00   | 24294       |
| 3     | 2024-06          | 2024-06-15 10:00   | 24294       |
| 4     | 2024-06          | 2024-06-15 10:00   | 24294       |
+-------+------------------+--------------------+-------------+
```

**Interpretation**: Accounts 1, 2, 3 have data through 2024-06

---

### Step 17b: Join and Classify

📍 **Location**: `summary_pipeline.py:841-865`

```python
# Step 17b.1: Join batch with metadata
classified = batch_df.alias("n").join(
    F.broadcast(summary_meta.alias("s")),
    F.col(f"n.{self.config.primary_column}") == F.col(f"s.{self.config.primary_column}"),
    "left"
).select(
    F.col("n.*"),
    F.col("s.max_existing_month"),
    F.col("s.max_existing_ts"),
    F.col("s.max_month_int")
)

# Step 17b.2: Classify into cases
classified = classified.withColumn(
    "case_type",
    F.when(F.col("max_existing_month").isNull(), F.lit("CASE_I"))
    .when(F.col("month_int") > F.col("max_month_int"), F.lit("CASE_II"))
    .otherwise(F.lit("CASE_III"))
).withColumn(
    "month_gap",
    F.when(
        F.col("case_type") == "CASE_II",
        F.col("month_int") - F.col("max_month_int") - 1
    ).otherwise(F.lit(0))
)

return classified.persist()
```

⚙️ **What happens**: Determine which case each record falls into

📊 **Classification logic**:
```python
For each backfill record:
  
  IF max_existing_month IS NULL:
    CASE_I  # Brand new account
  
  ELIF new_month > max_existing_month:
    CASE_II  # Forward entry (normal, just ahead of schedule)
  
  ELSE:
    CASE_III  # True backfill (historical correction)
```

📊 **Our data**:
```
+-------+----------+---------+------------------+-------------+-----------+
| cons_ | rpt_as_  | month_  | max_existing_    | max_month_  | case_type |
| acct_ | of_mo    |  int    |      month       |     int     |           |
|  key  |          |         |                  |             |           |
+-------+----------+---------+------------------+-------------+-----------+
| 1     | 2024-02  | 24290   | 2024-06          | 24294       | CASE_III  |
|       |          |         |                  |             | (24290 ≤  |
|       |          |         |                  |             |  24294)   |
| 2     | 2024-02  | 24290   | 2024-06          | 24294       | CASE_III  |
| 3     | 2024-02  | 24290   | 2024-06          | 24294       | CASE_III  |
+-------+----------+---------+------------------+-------------+-----------+

All three records are CASE_III (backfill) because:
  - They have existing data (not CASE_I)
  - New month (2024-02) is BEFORE max existing month (2024-06)
  - Therefore: Late-arriving historical data!
```

🔄 **Return to**: `run_backfill()` at line 469

---

### Step 18b: Filter to Case III Records

📍 **Location**: `summary_pipeline.py:471-484`

```python
# Process Case I (new accounts) - simple, no history to merge
case_i = classified.filter(F.col("case_type") == "CASE_I")
if case_i.limit(1).count() > 0:
    self._process_case_i(case_i)

# Process Case II (forward entries) - shift existing history
case_ii = classified.filter(F.col("case_type") == "CASE_II")
if case_ii.limit(1).count() > 0:
    self._process_case_ii(case_ii)

# Process Case III (backfill) - rebuild history
case_iii = classified.filter(F.col("case_type") == "CASE_III")
if case_iii.limit(1).count() > 0:
    self._process_case_iii(case_iii)
```

⚙️ **What happens**: Route each record to appropriate handler

📊 **Our case**:
```
case_i.count() = 0   → Skip
case_ii.count() = 0  → Skip
case_iii.count() = 3 → Process!
```

🔄 **Next**: Call `_process_case_iii()` - **Most complex logic!**

---

### Step 19b: Start Case III Processing

📍 **Location**: `summary_pipeline.py:960-976`

```python
def _process_case_iii(self, case_iii_df: DataFrame):
    """Process backfill entries (Case III)."""
    logger.info("Processing Case III (backfill with history rebuild)...")
    
    # Step 19b.1: Setup
    hl = self.config.history_length
    primary_col = self.config.primary_column
    partition_col = self.config.partition_column
    max_id_col = self.config.max_identifier_column
    
    # Step 19b.2: Create temp views
    affected_accounts = case_iii_df.select(primary_col).distinct()
    case_iii_df.createOrReplaceTempView("backfill_records")
    affected_accounts.createOrReplaceTempView("affected_accounts")
```

⚙️ **What happens**: Set up for complex SQL query

📊 **Temp views created**:
```sql
-- backfill_records
+---------------+-------------+----------+---------------------+
| cons_acct_key | rpt_as_of_mo| balance_ | base_ts             |
+---------------+-------------+----------+---------------------+
| 1             | 2024-02     | 10500    | 2024-06-20 10:00:00 |
| 2             | 2024-02     | 5500     | 2024-06-20 10:00:00 |
| 3             | 2024-02     | 15500    | 2024-06-20 10:00:00 |
+---------------+-------------+----------+---------------------+

-- affected_accounts
+---------------+
| cons_acct_key |
+---------------+
| 1             |
| 2             |
| 3             |
+---------------+
```

---

### Step 20b: Build Complex SQL - Validate Backfill

📍 **Location**: `summary_pipeline.py:1046-1058` (CTE 1)

```sql
WITH 
backfill_validated AS (
    SELECT b.*
    FROM backfill_records b
    LEFT JOIN (
        SELECT cons_acct_key, rpt_as_of_mo, base_ts
        FROM default.summary
        WHERE cons_acct_key IN (SELECT cons_acct_key FROM affected_accounts)
    ) e ON b.cons_acct_key = e.cons_acct_key 
        AND b.rpt_as_of_mo = e.rpt_as_of_mo
    WHERE e.base_ts IS NULL OR b.base_ts > e.base_ts
)
```

⚙️ **What happens**: Only keep backfill records that are NEWER than existing data

📊 **Scenario**:
```
Backfill record:
  Account 1, 2024-02, base_ts = 2024-06-20 10:00:00

Existing summary:
  Account 1, 2024-02, base_ts = 2024-02-15 10:00:00

Compare:
  2024-06-20 > 2024-02-15? YES
  → Keep this backfill record (it's newer)

Result (backfill_validated):
  All 3 records kept (they're all newer than existing 2024-02 data)
```

---

### Step 21b: Get Existing Data

📍 **Location**: `summary_pipeline.py:1060-1070` (CTE 2)

```sql
existing_data AS (
    SELECT 
        s.cons_acct_key,
        s.rpt_as_of_mo,
        (CAST(SUBSTRING(s.rpt_as_of_mo, 1, 4) AS INT) * 12 + 
         CAST(SUBSTRING(s.rpt_as_of_mo, 6, 2) AS INT)) as month_int,
        s.balance_am_history[0] as balance_am,  -- Extract from history
        s.past_due_am_history[0] as past_due_am,
        -- ... all other columns
        s.base_ts,
        2 as priority
    FROM default.summary s
    WHERE s.cons_acct_key IN (SELECT cons_acct_key FROM affected_accounts)
)
```

⚙️ **What happens**: Get ALL existing data for affected accounts (all months, not just 2024-02)

📊 **Data loaded**:
```
+-------+----------+---------+----------+----------+--------+----------+
| cons_ | rpt_as_  | month_  | balance_ | past_due | base_ts| priority |
| acct_ | of_mo    |  int    |    am    |    _am   |        |          |
|  key  |          |         |          |          |        |          |
+-------+----------+---------+----------+----------+--------+----------+
| 1     | 2024-01  | 24289   | 10000    | 0        | ...    | 2        |
| 1     | 2024-02  | 24290   | 10000    | 0        | ...    | 2        | ← Will be replaced
| 1     | 2024-03  | 24291   | 10200    | 0        | ...    | 2        |
| 1     | 2024-04  | 24292   | 10300    | 30       | ...    | 2        |
| 1     | 2024-05  | 24293   | 10400    | 30       | ...    | 2        |
| 1     | 2024-06  | 24294   | 10500    | 60       | ...    | 2        |
| 2     | 2024-01  | 24289   | 5000     | 0        | ...    | 2        |
| 2     | 2024-02  | 24290   | 5200     | 30       | ...    | 2        | ← Will be replaced
| ...   | ...      | ...     | ...      | ...      | ...    | ...      |
+-------+----------+---------+----------+----------+--------+----------+
```

**Note**: `priority = 2` (lower priority than backfill)

---

### Step 22b: Combine Backfill + Existing

📍 **Location**: `summary_pipeline.py:1072-1082` (CTE 3)

```sql
combined_data AS (
    SELECT 
        cons_acct_key, rpt_as_of_mo, month_int,
        balance_am, past_due_am, -- ... all columns
        base_ts,
        1 as priority
    FROM backfill_validated
    
    UNION ALL
    
    SELECT * FROM existing_data
)
```

⚙️ **What happens**: Combine backfill (priority 1) with existing (priority 2)

📊 **Combined data**:
```
+-------+----------+---------+----------+----------+--------+----------+
| cons_ | rpt_as_  | month_  | balance_ | past_due | base_ts| priority |
| acct_ | of_mo    |  int    |    am    |    _am   |        |          |
|  key  |          |         |          |          |        |          |
+-------+----------+---------+----------+----------+--------+----------+
| 1     | 2024-02  | 24290   | 10500    | 60       | Jun 20 | 1        | ← Backfill
| 1     | 2024-01  | 24289   | 10000    | 0        | Feb 15 | 2        | ← Existing
| 1     | 2024-02  | 24290   | 10000    | 0        | Feb 15 | 2        | ← Old (will lose)
| 1     | 2024-03  | 24291   | 10200    | 0        | Mar 15 | 2        |
| ...   | ...      | ...     | ...      | ...      | ...    | ...      |
+-------+----------+---------+----------+----------+--------+----------+

Note: Two rows for Account 1, 2024-02 (will be deduped next)
```

---

### Step 23b: Deduplicate

📍 **Location**: `summary_pipeline.py:1084-1096` (CTE 4 & 5)

```sql
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cons_acct_key, rpt_as_of_mo
            ORDER BY priority, base_ts DESC
        ) as rn
    FROM combined_data
),

final_data AS (
    SELECT * FROM deduped WHERE rn = 1
)
```

⚙️ **What happens**: For each account-month combination, keep only the best row

📊 **Deduplication**:
```
Account 1, 2024-02:
  Row 1: priority=1, base_ts=Jun 20, rn=1 ← WINNER (lowest priority, newest ts)
  Row 2: priority=2, base_ts=Feb 15, rn=2 ← Loses

Account 1, 2024-01:
  Row 1: priority=2, base_ts=Feb 15, rn=1 ← Only row, wins by default

Result (final_data):
+-------+----------+---------+----------+----------+
| cons_ | rpt_as_  | month_  | balance_ | past_due |
| acct_ | of_mo    |  int    |    am    |    _am   |
|  key  |          |         |          |          |
+-------+----------+---------+----------+----------+
| 1     | 2024-01  | 24289   | 10000    | 0        |
| 1     | 2024-02  | 24290   | 10500    | 60       | ← Updated!
| 1     | 2024-03  | 24291   | 10200    | 0        |
| 1     | 2024-04  | 24292   | 10300    | 30       |
| 1     | 2024-05  | 24293   | 10400    | 30       |
| 1     | 2024-06  | 24294   | 10500    | 60       |
+-------+----------+---------+----------+----------+
```

---

### Step 24b: Collect All Months Per Account

📍 **Location**: `summary_pipeline.py:1098-1104` (CTE 6)

```sql
account_months AS (
    SELECT 
        cons_acct_key,
        COLLECT_LIST(STRUCT(month_int, balance_am, past_due_am, ...)) as month_values,
        COLLECT_LIST(STRUCT(rpt_as_of_mo, month_int, base_ts)) as month_metadata
    FROM final_data
    GROUP BY cons_acct_key
)
```

⚙️ **What happens**: Collect all month data into arrays, one row per account

📊 **Result**:
```
+-------+--------------------------------+-----------------------------+
| cons_ | month_values (array of struct) | month_metadata (array)      |
| acct_ |                                |                             |
|  key  |                                |                             |
+-------+--------------------------------+-----------------------------+
| 1     | [{month_int:24289,             | [{rpt_as_of_mo:"2024-01",   |
|       |   balance_am:10000,            |   month_int:24289, ...},    |
|       |   past_due_am:0},              |  {rpt_as_of_mo:"2024-02",   |
|       |  {month_int:24290,             |   month_int:24290, ...},    |
|       |   balance_am:10500,   ← NEW!   |  {rpt_as_of_mo:"2024-03",   |
|       |   past_due_am:60},             |   month_int:24291, ...},    |
|       |  {month_int:24291,             |   ...]                      |
|       |   balance_am:10200,            |                             |
|       |   past_due_am:0},              |                             |
|       |  ...]                          |                             |
+-------+--------------------------------+-----------------------------+

Now we have ALL months for Account 1 in ONE row!
```

---

### Step 25b: Explode Back to Rows

📍 **Location**: `summary_pipeline.py:1106-1116` (CTE 7)

```sql
exploded AS (
    SELECT 
        cons_acct_key,
        month_values,
        meta.rpt_as_of_mo,
        meta.month_int,
        meta.base_ts
    FROM account_months
    LATERAL VIEW EXPLODE(month_metadata) t AS meta
)
```

⚙️ **What happens**: Expand back to one row per account-month, but keep full history available

📊 **Result**:
```
+-------+----------------------+----------+---------+
| cons_ | month_values         | rpt_as_  | month_  |
| acct_ | (all 6 months)       | of_mo    |  int    |
|  key  |                      |          |         |
+-------+----------------------+----------+---------+
| 1     | [{24289, 10000, 0},  | 2024-01  | 24289   |
|       |  {24290, 10500, 60}, |          |         |
|       |  {24291, 10200, 0},  |          |         |
|       |  ...]                |          |         |
| 1     | [{24289, 10000, 0},  | 2024-02  | 24290   | ← Same array, different row
|       |  {24290, 10500, 60}, |          |         |
|       |  {24291, 10200, 0},  |          |         |
|       |  ...]                |          |         |
| 1     | [{24289, 10000, 0},  | 2024-03  | 24291   |
|       |  {24290, 10500, 60}, |          |         |
|       |  {24291, 10200, 0},  |          |         |
|       |  ...]                |          |         |
| ...   | ...                  | ...      | ...     |
+-------+----------------------+----------+---------+

Each row represents ONE month, but has access to ALL months in month_values array!
```

---

### Step 26b: Rebuild History Arrays

📍 **Location**: `summary_pipeline.py:1118-1124` (Final SELECT)

```sql
SELECT
    e.cons_acct_key,
    e.rpt_as_of_mo,
    TRANSFORM(
        SEQUENCE(0, 35),
        offset -> (
            AGGREGATE(
                FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                CAST(NULL AS INT),
                (acc, x) -> x.balance_am
            )
        )
    ) as balance_am_history,
    -- ... same for other rolling columns
    element_at(FILTER(e.month_values, mv -> mv.month_int = e.month_int), 1).balance_am as balance_am,
    -- ... same for other scalar columns
    e.base_ts
FROM exploded e
```

⚙️ **What happens**: For each month, rebuild the 36-month history array using the full data

📊 **Example** - Account 1, Month 2024-03 (month_int = 24291):

```python
# Rebuild balance_am_history

TRANSFORM(SEQUENCE(0, 35), offset -> ...)

For offset = 0:
  Target month_int = 24291 - 0 = 24291  # Current month (2024-03)
  FILTER(month_values, mv -> mv.month_int = 24291)
  → [{month_int:24291, balance_am:10200, ...}]
  AGGREGATE: Extract balance_am
  → 10200

For offset = 1:
  Target month_int = 24291 - 1 = 24290  # One month ago (2024-02)
  FILTER(month_values, mv -> mv.month_int = 24290)
  → [{month_int:24290, balance_am:10500, ...}]  ← NEW corrected value!
  AGGREGATE: Extract balance_am
  → 10500

For offset = 2:
  Target month_int = 24291 - 2 = 24289  # Two months ago (2024-01)
  FILTER(month_values, mv -> mv.month_int = 24289)
  → [{month_int:24289, balance_am:10000, ...}]
  AGGREGATE: Extract balance_am
  → 10000

For offset = 3:
  Target month_int = 24291 - 3 = 24288  # Three months ago (2023-12)
  FILTER(month_values, mv -> mv.month_int = 24288)
  → []  # No data for this month
  AGGREGATE: Returns NULL
  → NULL

...

Result: balance_am_history = [10200, 10500, 10000, NULL, NULL, ...]
                               ↑      ↑ NEW!  ↑
                            2024-03 2024-02 2024-01
```

📊 **Final result for Account 1, 2024-03**:
```
+-------+----------+-----------------------+----------+
| cons_ | rpt_as_  | balance_am_history    | balance_ |
| acct_ | of_mo    |                       |    am    |
|  key  |          |                       |          |
+-------+----------+-----------------------+----------+
| 1     | 2024-03  | [10200, 10500, 10000, | 10200    |
|       |          |  NULL, ...]           |          |
+-------+----------+-----------------------+----------+
              Position 0: 2024-03 = 10200
              Position 1: 2024-02 = 10500 ← Corrected from 10000!
              Position 2: 2024-01 = 10000
```

**Key insight**: The backfill value (10500 for 2024-02) is now incorporated into ALL future months' history arrays!

---

### Step 27b: Write Backfill Results

📍 **Location**: `summary_pipeline.py:1135-1175`

```python
def _write_backfill_results(self, result: DataFrame):
    """Write backfill results via MERGE."""
    result.createOrReplaceTempView("backfill_result")
    
    # Step 27b.1: MERGE to summary
    self.spark.sql(f"""
        MERGE INTO {self.config.destination_table} t
        USING backfill_result s
        ON t.{self.config.primary_column} = s.{self.config.primary_column}
           AND t.{self.config.partition_column} = s.{self.config.partition_column}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

⚙️ **What happens**: Update summary table with corrected values

📊 **MERGE effect**:
```
Account 1, 2024-02:
  Before: balance_am = 10000, balance_am_history = [10000, 10000, ...]
  After:  balance_am = 10500, balance_am_history = [10500, 10000, ...] ← Updated

Account 1, 2024-03:
  Before: balance_am = 10200, balance_am_history = [10200, 10000, 10000, ...]
  After:  balance_am = 10200, balance_am_history = [10200, 10500, 10000, ...] ← History fixed!

Account 1, 2024-04:
  Before: balance_am_history = [10300, 10200, 10000, 10000, ...]
  After:  balance_am_history = [10300, 10200, 10500, 10000, ...] ← History fixed!

All future months get the corrected history!
```

---

### Step 28b: Update latest_summary

📍 **Location**: `summary_pipeline.py:1150-1175`

```python
# Step 28b.1: Get latest month per account
latest_per_account = self.spark.sql(f"""
    SELECT * FROM (
        SELECT *, 
            ROW_NUMBER() OVER (
                PARTITION BY {self.config.primary_column} 
                ORDER BY {self.config.partition_column} DESC
            ) as rn
        FROM backfill_result
    ) WHERE rn = 1
""").drop("rn")

# Step 28b.2: MERGE to latest_summary
self.spark.sql(f"""
    MERGE INTO {self.config.latest_history_table} t
    USING backfill_latest s
    ON t.{self.config.primary_column} = s.{self.config.primary_column}
    WHEN MATCHED AND s.{self.config.partition_column} >= t.{self.config.partition_column} 
        THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

⚙️ **What happens**: Update latest_summary only if the backfill included the latest month

📊 **Logic**:
```
Account 1:
  Backfill included: 2024-01, 2024-02, 2024-03, 2024-04, 2024-05, 2024-06
  Latest in backfill: 2024-06
  Current latest_summary: 2024-06
  
  Action: UPDATE (same month, but with corrected history arrays)

Account 2:
  Latest in backfill: 2024-06
  Action: UPDATE
```

---

### Step 29b: Backfill Complete

📍 **Location**: `summary_pipeline.py:486-487`

```python
total_time = (time.time() - self.start_time) / 60
logger.info(f"Backfill completed in {total_time:.2f} minutes")
```

⚙️ **What happens**: Log completion

📊 **Console output**:
```
Backfill completed in 0.85 minutes
```

🔄 **Return to**: `main()`, then cleanup and exit (same as forward mode)

---

## Summary of Backfill Mode Execution

### Complete Flow

```
1-10. Same as Forward Mode (setup)
11. Call run_backfill()
12. Load backfill data (late-arriving records)
13. Prepare data (apply transformations, add month_int)
14. Classify into Case I/II/III
    - Get max month and timestamp for each account
    - Compare backfill month with max month
    - Our example: All CASE_III (backfill)
15. Process CASE_III:
    16. Create temp views
    17. Build complex SQL with 7 CTEs:
        a. backfill_validated: Filter by newer timestamp
        b. existing_data: Get all existing months for affected accounts
        c. combined_data: UNION backfill + existing (with priority)
        d. deduped: ROW_NUMBER to pick winner per account-month
        e. final_data: SELECT winners
        f. account_months: COLLECT_LIST all months per account
        g. exploded: EXPLODE back to rows (one per month)
    18. Final SELECT:
        - For each month, rebuild history arrays using TRANSFORM
        - Look back 36 months using month_int arithmetic
        - FILTER + AGGREGATE to extract values
    19. Write results:
        - MERGE to summary (updates affected months)
        - MERGE to latest_summary (updates if needed)
20. Cleanup and exit
```

### Key Algorithm: History Array Rebuild

```
For each month M in final result:
  For each offset O from 0 to 35:
    Target month = M - O
    Find record in month_values where month_int = Target month
    Extract value
    Add to array at position O
  
Result: 36-element array with current month at [0], oldest at [35]
```

### Why This Works

1. **Collect all data**: Combines backfill + existing into one dataset
2. **Deduplicate**: Newer timestamp wins (backfill has priority)
3. **Group by account**: All months for each account in one array
4. **Rebuild arrays**: For each month, look back 36 months and build fresh array
5. **Result**: All affected months have corrected values in their history arrays

---

## Complete Documentation

### What We've Created

1. **EXECUTION_FLOW.md** (This document): Step-by-step execution trace
2. **README.md**: User guide and reference
3. **CODE_EXPLANATION.md**: Component-by-component breakdown
4. **COMPLETE_TEST_DOCUMENTATION.md**: Test results and validation

### How to Use This Document

**For understanding flow**:
- Follow the steps sequentially
- See exactly what happens at each point
- Understand data transformations with examples

**For debugging**:
- Find the step that's failing
- Check the expected data at that point
- Verify your data matches the examples

**For learning**:
- Read through once for overview
- Deep dive into complex steps (like Step 31 or Step 26b)
- Trace a single record through the entire pipeline

---

**Version**: 4.0.0  
**Last Updated**: January 2026  
**Document Type**: Execution Trace  
**Status**: Complete ✅

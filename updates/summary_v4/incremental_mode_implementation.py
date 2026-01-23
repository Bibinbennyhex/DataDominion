#!/usr/bin/env python3
"""
Summary Pipeline v4.0 - Incremental Mode Implementation

This file contains the run_incremental() method to be added to the SummaryPipeline class.
Insert this method into summary_pipeline.py in the SummaryPipeline class (after run_backfill method).
"""

def run_incremental(self):
    """
    Run incremental processing for production.
    
    This mode automatically handles all scenarios in a single run:
    - Backfill: Late-arriving corrections (month <= max existing, newer timestamp)
    - New accounts: First appearance of accounts
    - Forward: New month data (month > max existing)
    
    Processing Order (CRITICAL):
    1. Backfill FIRST (rebuilds history for affected months)
    2. New accounts SECOND (no dependencies)
    3. Forward LAST (uses corrected history from backfill)
    
    Usage:
        spark-submit summary_pipeline.py --config config.json --mode incremental
    
    Or simply (defaults to incremental):
        spark-submit summary_pipeline.py --config config.json
    """
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v4.0 - INCREMENTAL MODE (PRODUCTION)")
    logger.info("=" * 80)
    
    # ================================================================
    # Step 1: Get latest timestamp from summary table
    # ================================================================
    max_ts = self._get_max_summary_timestamp()
    
    if max_ts is None:
        logger.warning("No existing summary found. Use 'initial' mode for first load.")
        logger.warning("Example: --mode initial --start-month 2016-01 --end-month 2025-12")
        return
    
    logger.info(f"Latest timestamp in summary: {max_ts}")
    logger.info(f"Processing all new data since: {max_ts}")
    
    # ================================================================
    # Step 2: Read all new data (base_ts > max existing timestamp)
    # ================================================================
    logger.info("Reading new data from source...")
    
    batch_df = self.spark.sql(f"""
        SELECT * FROM {self.config.source_table}
        WHERE {self.config.max_identifier_column} > TIMESTAMP '{max_ts}'
    """)
    
    # Check if we have any new data
    record_count = batch_df.count()
    if record_count == 0:
        logger.info("No new records to process. Summary is up to date.")
        return
    
    logger.info(f"New records found: {record_count:,}")
    
    # ================================================================
    # Step 3: Prepare and classify ALL records
    # ================================================================
    logger.info("Preparing and classifying records...")
    
    # Apply column mappings, transformations, deduplication
    batch_df = self._prepare_source_data(batch_df)
    
    # Classify into Case I, II, III
    classified = self._classify_backfill_records(batch_df)
    classified.persist()  # Cache for multiple filters
    
    # Count each case type
    logger.info("Classifying records into cases...")
    case_counts = classified.groupBy("case_type").count().collect()
    case_dict = {row["case_type"]: row["count"] for row in case_counts}
    
    case_i_count = case_dict.get('CASE_I', 0)
    case_ii_count = case_dict.get('CASE_II', 0)
    case_iii_count = case_dict.get('CASE_III', 0)
    
    logger.info("-" * 80)
    logger.info("CLASSIFICATION RESULTS:")
    logger.info(f"  Case I   (New accounts):    {case_i_count:>10,} records")
    logger.info(f"  Case II  (Forward entries): {case_ii_count:>10,} records")
    logger.info(f"  Case III (Backfill):        {case_iii_count:>10,} records")
    logger.info(f"  TOTAL:                      {record_count:>10,} records")
    logger.info("-" * 80)
    
    # ================================================================
    # Step 4: Process in correct order
    # ================================================================
    # CRITICAL: Order matters!
    # - Backfill FIRST: Rebuilds history arrays with corrections
    # - New accounts SECOND: No dependencies, can go anytime
    # - Forward LAST: Must use corrected history from backfill
    
    # ----------------------------------------------------------------
    # 4a. Process Case III: Backfill (HIGHEST PRIORITY)
    # ----------------------------------------------------------------
    case_iii = classified.filter(F.col("case_type") == "CASE_III")
    if case_iii.limit(1).count() > 0:
        logger.info("=" * 80)
        logger.info(f"STEP 1/3: Processing {case_iii_count:,} backfill records (Case III)")
        logger.info("=" * 80)
        
        step_start = time.time()
        
        # Get affected months for logging
        affected_months = case_iii.select(self.config.partition_column).distinct().count()
        affected_accounts = case_iii.select(self.config.primary_column).distinct().count()
        
        logger.info(f"Affected: {affected_accounts:,} accounts, {affected_months} unique months")
        logger.info("Rebuilding history arrays with corrected values...")
        
        self._process_case_iii(case_iii)
        
        elapsed = (time.time() - step_start) / 60
        logger.info(f"Backfill processing completed in {elapsed:.2f} minutes")
        logger.info("-" * 80)
    else:
        logger.info("No backfill records to process (Case III)")
    
    # ----------------------------------------------------------------
    # 4b. Process Case I: New Accounts (MEDIUM PRIORITY)
    # ----------------------------------------------------------------
    case_i = classified.filter(F.col("case_type") == "CASE_I")
    if case_i.limit(1).count() > 0:
        logger.info("=" * 80)
        logger.info(f"STEP 2/3: Processing {case_i_count:,} new accounts (Case I)")
        logger.info("=" * 80)
        
        step_start = time.time()
        
        logger.info("Creating initial history arrays for new accounts...")
        
        self._process_case_i(case_i)
        
        elapsed = (time.time() - step_start) / 60
        logger.info(f"New account processing completed in {elapsed:.2f} minutes")
        logger.info("-" * 80)
    else:
        logger.info("No new accounts to process (Case I)")
    
    # ----------------------------------------------------------------
    # 4c. Process Case II: Forward Entries (LOWEST PRIORITY)
    # ----------------------------------------------------------------
    case_ii = classified.filter(F.col("case_type") == "CASE_II")
    if case_ii.limit(1).count() > 0:
        logger.info("=" * 80)
        logger.info(f"STEP 3/3: Processing {case_ii_count:,} forward entries (Case II)")
        logger.info("=" * 80)
        
        step_start = time.time()
        
        # Get unique months for logging
        forward_months = case_ii.select(self.config.partition_column).distinct() \
            .orderBy(self.config.partition_column).collect()
        month_list = [row[self.config.partition_column] for row in forward_months]
        
        logger.info(f"Processing months: {', '.join(month_list)}")
        logger.info("Shifting history arrays with new data (using corrected history from backfill)...")
        
        self._process_case_ii(case_ii)
        
        elapsed = (time.time() - step_start) / 60
        logger.info(f"Forward processing completed in {elapsed:.2f} minutes")
        logger.info("-" * 80)
    else:
        logger.info("No forward entries to process (Case II)")
    
    # Cleanup cached classified DataFrame
    classified.unpersist()
    
    # ================================================================
    # Step 5: Final optimization and summary
    # ================================================================
    logger.info("=" * 80)
    logger.info("FINALIZING")
    logger.info("=" * 80)
    
    logger.info("Optimizing tables (compacting files, expiring snapshots)...")
    self._optimize_tables()
    
    # Get final statistics
    logger.info("Gathering final statistics...")
    final_stats = self.spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT {self.config.primary_column}) as unique_accounts,
            MAX({self.config.partition_column}) as latest_month,
            MAX({self.config.max_identifier_column}) as latest_timestamp
        FROM {self.config.destination_table}
    """).first()
    
    logger.info("=" * 80)
    logger.info("INCREMENTAL PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total records in summary:    {final_stats['total_records']:,}")
    logger.info(f"Unique accounts:             {final_stats['unique_accounts']:,}")
    logger.info(f"Latest month:                {final_stats['latest_month']}")
    logger.info(f"Latest timestamp:            {final_stats['latest_timestamp']}")
    
    total_time = (time.time() - self.start_time) / 60
    logger.info(f"Total processing time:       {total_time:.2f} minutes")
    logger.info("=" * 80)


# ================================================================
# Updated parse_args() - Add 'incremental' mode
# ================================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v4.0 - Config-driven production scale'
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='Path to config JSON file')
    parser.add_argument('--mode', type=str, 
                       choices=['initial', 'forward', 'backfill', 'incremental'],
                       default='incremental',  # Changed default to incremental!
                       help='Processing mode (default: incremental for production)')
    parser.add_argument('--start-month', type=str,
                       help='Start month (YYYY-MM) for forward mode')
    parser.add_argument('--end-month', type=str,
                       help='End month (YYYY-MM) for forward mode')
    parser.add_argument('--backfill-filter', type=str,
                       help='SQL filter for backfill source data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without writing (not yet implemented)')
    
    return parser.parse_args()


# ================================================================
# Updated main() - Handle 'incremental' mode
# ================================================================

def main():
    """Main entry point."""
    args = parse_args()
    
    # Load config
    config = SummaryConfig(args.config)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        pipeline = SummaryPipeline(spark, config)
        
        if args.mode == 'incremental':
            # NEW: Production default mode
            # Automatically handles backfill, new accounts, and forward entries
            pipeline.run_incremental()
            
        elif args.mode == 'forward' or args.mode == 'initial':
            # Parse dates
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
            
        elif args.mode == 'backfill':
            pipeline.run_backfill(source_filter=args.backfill_filter)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# ================================================================
# INTEGRATION INSTRUCTIONS
# ================================================================

"""
To integrate this into summary_pipeline.py:

1. Add the run_incremental() method to the SummaryPipeline class
   (insert after run_backfill method, around line 488)

2. Replace the parse_args() function (around line 1206)
   with the updated version above

3. Replace the main() function (around line 1227)
   with the updated version above

4. Test with:
   python summary_pipeline.py --config summary_config.json --mode incremental
   
5. Or simply (defaults to incremental now):
   python summary_pipeline.py --config summary_config.json
"""

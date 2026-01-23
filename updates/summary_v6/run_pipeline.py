#!/usr/bin/env python3
"""
Summary Pipeline v6.0 - CLI Entry Point
========================================

Unified command-line interface for the summary pipeline.

Usage:
    # Production mode (auto-handles all scenarios)
    spark-submit run_pipeline.py --config config/pipeline_config.json
    
    # Resume from checkpoint
    spark-submit run_pipeline.py --config config/pipeline_config.json --resume abc123
    
    # Initial load
    spark-submit run_pipeline.py --config config/pipeline_config.json --mode initial
    
    # Show statistics
    spark-submit run_pipeline.py --config config/pipeline_config.json --stats
"""

import argparse
import sys
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SummaryPipeline")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v6.0 - Hybrid Production Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Production run (auto-handles all scenarios)
  spark-submit run_pipeline.py --config config/pipeline_config.json
  
  # Resume from failed run
  spark-submit run_pipeline.py --config config/pipeline_config.json --resume abc12345
  
  # Initial load from historical data
  spark-submit run_pipeline.py --config config/pipeline_config.json --mode initial
  
  # Show table statistics
  spark-submit run_pipeline.py --config config/pipeline_config.json --stats
        """
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='Path to config JSON file')
    parser.add_argument('--mode', type=str,
                       choices=['incremental', 'initial', 'forward', 'backfill'],
                       default='incremental',
                       help='Processing mode (default: incremental)')
    parser.add_argument('--resume', type=str,
                       help='Resume from checkpoint with given run ID')
    parser.add_argument('--stats', action='store_true',
                       help='Show table statistics and exit')
    parser.add_argument('--optimize', action='store_true',
                       help='Run table optimization and exit')
    parser.add_argument('--log-level', type=str,
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO',
                       help='Logging level')
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    logger.info("=" * 80)
    logger.info("Summary Pipeline v6.0 - Hybrid Production Pipeline")
    logger.info("=" * 80)
    logger.info(f"Config: {args.config}")
    logger.info(f"Mode: {args.mode}")
    if args.resume:
        logger.info(f"Resume from: {args.resume}")
    logger.info("=" * 80)
    
    # Import here to allow --help without Spark
    from core import PipelineConfig, create_spark_session
    from orchestration import SummaryPipeline
    from utils import TableOptimizer
    
    # Load config
    try:
        config = PipelineConfig(args.config)
        logger.info(f"Loaded configuration for {config.environment} environment")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(config)
    
    try:
        # Handle special modes
        if args.stats:
            optimizer = TableOptimizer(spark, config)
            
            logger.info("Table Statistics:")
            logger.info("-" * 40)
            
            for table in [config.summary_table, config.latest_summary_table]:
                stats = optimizer.get_table_stats(table)
                logger.info(f"{table}:")
                logger.info(f"  Records: {stats.get('record_count', 'N/A'):,}")
                logger.info(f"  Partitions: {stats.get('partition_count', 'N/A')}")
            
            return
        
        if args.optimize:
            optimizer = TableOptimizer(spark, config)
            optimizer.optimize_tables(force=True)
            logger.info("Optimization complete")
            return
        
        # Run pipeline
        pipeline = SummaryPipeline(spark, config)
        
        if args.mode == 'incremental':
            stats = pipeline.run_incremental(resume_run_id=args.resume)
        else:
            logger.error(f"Mode '{args.mode}' not yet implemented in this version")
            sys.exit(1)
        
        # Log final stats
        if stats:
            logger.info("=" * 80)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 80)
            logger.info(f"Run ID: {stats.run_id}")
            logger.info(f"Records processed: {stats.total_records:,}")
            logger.info(f"Duration: {stats.duration_minutes:.2f} minutes")
            logger.info(f"Success rate: {stats.success_rate:.1f}%")
            logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
